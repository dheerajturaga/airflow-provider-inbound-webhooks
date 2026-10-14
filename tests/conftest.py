# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""Standalone test setup: a throwaway Airflow home with a SQLite metadata database."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Airflow reads its configuration when first imported, so this must run before any Airflow import.
AIRFLOW_HOME = Path(tempfile.mkdtemp(prefix="airflow-inbound-webhooks-tests-"))
# A known password for the default "admin" user, so tests can call the API server's REST API.
(AIRFLOW_HOME / "simple_auth_manager_passwords.json").write_text('{"admin": "admin"}')
os.environ.update(
    {
        "AIRFLOW_HOME": str(AIRFLOW_HOME),
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": str(
            AIRFLOW_HOME / "simple_auth_manager_passwords.json"
        ),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{AIRFLOW_HOME / 'airflow.db'}",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__AUTH_MANAGER": (
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
        ),
        # Serve any webhook name, so each test can use the ones it needs; allowlist tests override it.
        "AIRFLOW__INBOUND_WEBHOOKS__WEBHOOK_NAMES": "*",
    }
)

import pytest  # noqa: E402
from airflow.models.asset import (  # noqa: E402
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
)
from airflow.models.connection import Connection  # noqa: E402
from airflow.models.dag import DagModel  # noqa: E402
from airflow.models.dagbundle import DagBundleModel  # noqa: E402
from airflow.utils.session import create_session  # noqa: E402
from sqlalchemy import delete  # noqa: E402

# Children first, so deletes satisfy foreign keys.
CLEARED_MODELS = (
    AssetDagRunQueue,
    AssetEvent,
    DagScheduleAssetReference,
    AssetActive,
    AssetModel,
    DagModel,
    DagBundleModel,
    Connection,
)


@pytest.fixture(scope="session", autouse=True)
def airflow_db():
    subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], check=True, capture_output=True)


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

    def subscribe_dag(dag_id: str, asset_name: str) -> None:
        if session.get(DagBundleModel, "testing") is None:
            session.add(DagBundleModel(name="testing"))
            session.flush()
        asset_model = AssetModel(name=asset_name, uri=asset_name, group="asset")
        asset_model.scheduled_dags = [DagScheduleAssetReference(dag_id=dag_id)]
        session.add_all(
            [
                DagModel(dag_id=dag_id, is_paused=False, is_stale=False, bundle_name="testing"),
                asset_model,
                AssetActive.for_asset(asset_model),
            ]
        )
        session.commit()

    return subscribe_dag
