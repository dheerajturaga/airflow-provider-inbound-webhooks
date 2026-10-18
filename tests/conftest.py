# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""Standalone test setup: a throwaway Airflow home with a metadata database of its own."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
import uuid
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

from packaging.version import Version

# Airflow reads its configuration when first imported, so this must run before any Airflow import.
AIRFLOW_HOME = Path(tempfile.mkdtemp(prefix="airflow-inbound-webhooks-tests-"))
# A known password for the default "admin" user, so tests can call the API server's REST API.
(AIRFLOW_HOME / "simple_auth_manager_passwords.json").write_text('{"admin": "admin"}')
# Tests delete Dags, assets and connections, so they never use the database Airflow is configured with:
# a new SQLite file, or the PostgreSQL or MySQL database INBOUND_WEBHOOKS_TEST_DATABASE names.
TEST_DATABASE = os.environ.get("INBOUND_WEBHOOKS_TEST_DATABASE") or f"sqlite:///{AIRFLOW_HOME / 'airflow.db'}"
os.environ.update(
    {
        "AIRFLOW_HOME": str(AIRFLOW_HOME),
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": str(
            AIRFLOW_HOME / "simple_auth_manager_passwords.json"
        ),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": TEST_DATABASE,
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__AUTH_MANAGER": (
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
        ),
        # The webhook names the tests use; tests of the allowlist override it.
        "AIRFLOW__INBOUND_WEBHOOKS__WEBHOOK_NAMES": (
            "github,github_enterprise,gitlab,jira,deploybot,huggingface,bitbucket"
        ),
    }
)
# Airflow 3.2 and later find the DB manager in the provider metadata, which the tests rely on there;
# 3.1 only runs it when configured, as the README tells its users to.
if Version(version("apache-airflow")) < Version("3.2"):
    os.environ["AIRFLOW__DATABASE__EXTERNAL_DB_MANAGERS"] = "airflow_inbound_webhooks.db.WebhooksDBManager"

import pytest  # noqa: E402
from airflow.models.asset import (  # noqa: E402
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
)
from airflow.models.connection import Connection  # noqa: E402
from airflow.models.dag import DagModel  # noqa: E402
from airflow.models.dagbundle import DagBundleModel  # noqa: E402
from airflow.models.log import Log  # noqa: E402
from airflow.utils.session import create_session  # noqa: E402
from sqlalchemy import delete, select  # noqa: E402

from airflow_inbound_webhooks.models import delivery_table  # noqa: E402

try:
    from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
except ImportError:  # Airflow 3.1
    from airflow.models.dagbag import DagBag, sync_bag_to_db

# Children first, so deletes satisfy foreign keys.
CLEARED_MODELS = (
    delivery_table,
    Log,
    AssetDagRunQueue,
    AssetEvent,
    DagScheduleAssetReference,
    DagScheduleAssetNameReference,
    DagScheduleAssetUriReference,
    AssetActive,
    AssetModel,
    DagModel,
    DagBundleModel,
    Connection,
)


@pytest.fixture(scope="session", autouse=True)
def airflow_db():
    result = subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"], capture_output=True, text=True, check=False
    )
    if result.returncode:
        pytest.fail(f"airflow db migrate failed:\n{(result.stdout + result.stderr)[-3000:]}")


def clear_db() -> None:
    with create_session() as session:
        for model in CLEARED_MODELS:
            session.execute(delete(model))


@pytest.fixture
def clean_db():
    clear_db()
    yield
    clear_db()


@pytest.fixture
def session():
    with create_session() as session:
        yield session


@pytest.fixture
def subscribe(session):
    """Make a Dag schedule on an asset name, as parsing a Dag file would."""

    def subscribe_dag(
        dag_id: str, asset_name: str, *, paused: bool = False, stale: bool = False, ref: str | None = None
    ) -> None:
        """With ``ref`` "name" or "uri", schedule as ``Asset.ref(name=...)`` or ``Asset.ref(uri=...)``."""
        if session.get(DagBundleModel, "testing") is None:
            session.add(DagBundleModel(name="testing"))
            session.flush()
        session.add(DagModel(dag_id=dag_id, is_paused=paused, is_stale=stale, bundle_name="testing"))
        # Several Dags can schedule on one name, which is still one asset.
        if (asset_model := session.scalar(select(AssetModel).where(AssetModel.name == asset_name))) is None:
            asset_model = AssetModel(name=asset_name, uri=asset_name, group="asset")
            session.add_all([asset_model, AssetActive.for_asset(asset_model)])
        if ref == "name":
            session.add(DagScheduleAssetNameReference(name=asset_name, dag_id=dag_id))
        elif ref == "uri":
            session.add(DagScheduleAssetUriReference(uri=asset_name, dag_id=dag_id))
        else:
            asset_model.scheduled_dags.append(DagScheduleAssetReference(dag_id=dag_id))
        session.commit()

    return subscribe_dag


@pytest.fixture
def parse_dag_file(tmp_path):
    """Parse a Dag file as the Dag processor does, into a DagBag with its Dags and import errors."""

    def parse(source: str) -> DagBag:
        path = tmp_path / "dags.py"
        path.write_text(textwrap.dedent(source))
        return DagBag(dag_folder=str(path))

    return parse


@pytest.fixture
def registered_policy():
    """Have Airflow call the package's cluster policy, as its entry point makes it once installed."""
    from airflow import settings

    from airflow_inbound_webhooks import policy

    # Airflow 3.1 keeps the policy plugin manager in a module global.
    if hasattr(settings, "get_policy_plugin_manager"):
        manager = settings.get_policy_plugin_manager()
    else:
        manager = settings.POLICY_PLUGIN_MANAGER
    # A package whose installed metadata predates the entry point has not been registered.
    if manager.is_registered(policy):
        yield
        return
    manager.register(policy)
    yield
    manager.unregister(policy)


@pytest.fixture
def store_dags(session):
    """Store a DagBag's Dags, their assets and what they schedule on, as the Dag processor does."""

    def store(bag: DagBag) -> None:
        if session.get(DagBundleModel, "testing") is None:
            session.add(DagBundleModel(name="testing"))
        session.commit()
        sync_bag_to_db(bag, "testing", None)

    return store


@pytest.fixture
def add_delivery(session):
    """Store a delivery record, as a recorded GitHub delivery would leave one."""

    def add(**values) -> str:
        row = {
            "id": str(uuid.uuid4()),
            "webhook": "github",
            # Unique per webhook, like a sender's.
            "delivery_id": str(uuid.uuid4()),
            "body_digest": "0" * 64,
            "received_at": datetime(2026, 10, 17, 12, tzinfo=timezone.utc),
            "source_type": "github",
            "path": "my-org/web",
            "candidates": ["github/my-org/web/push"],
            "asset_events": {"github/my-org/web/push": 1},
            "correlation_key": None,
            "outcome": "accepted",
            "team_name": None,
            "payload": {"ref": "refs/heads/main"},
            **values,
        }
        session.execute(delivery_table.insert().values(**row))
        session.commit()
        return row["id"]

    return add
