# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest import mock

import pytest
from airflow.exceptions import AirflowClusterPolicyViolation
from airflow.models.dag import DagModel
from airflow.sdk import DAG, Asset, task
from sqlalchemy import select

from airflow_inbound_webhooks import WebhookTopic, policy

if sys.version_info >= (3, 11):
    import tomllib
else:  # pytest depends on tomli before Python 3.11.
    import tomli as tomllib

try:
    from airflow.sdk import PartitionedAssetTimetable
except ImportError:  # Airflow 3.1 has no partitions.
    PartitionedAssetTimetable = None

ROOT = Path(__file__).parents[1]
DAG_CHECKS = "AIRFLOW__INBOUND_WEBHOOKS__DAG_CHECKS"
PUSH = WebhookTopic(webhook="github", path="my-org/web", event="push")
ANY_PUSH = WebhookTopic(webhook="github", path="*", event="push")
MAIN_PUSH = WebhookTopic(webhook="github", path="my-org/web", event="push", detail="main")
OPENED = WebhookTopic(webhook="github", path="my-org/web", event="pull_request", action="opened")
REOPENED = WebhookTopic(webhook="github", path="my-org/web", event="pull_request", action="reopened")
RELEASE = WebhookTopic(webhook="github", path="my-org/web", event="release")
PARTITIONED = pytest.mark.skipif(
    PartitionedAssetTimetable is None, reason="Airflow has partitioned timetables from 3.2"
)


def build_dag(schedule=None, *, outlets=(), inlets=()) -> DAG:
    with DAG("deploy", schedule=schedule) as dag:

        @task(outlets=list(outlets), inlets=list(inlets))
        def deploy(): ...

        deploy()
    return dag


def build_batching_dag(assets) -> DAG:
    # As a timetable with batch_asset_events, from Airflow 3.4.
    timetable_class = type(build_dag(assets).timetable)
    batching = type("BatchingTimetable", (timetable_class,), {"batch_asset_events": True})
    return build_dag(batching(assets))


def build_mapped_dag(*, outlets) -> DAG:
    with DAG("deploy", schedule=None) as dag:

        @task(outlets=list(outlets))
        def deploy(environment): ...

        deploy.expand(environment=["staging", "production"])
    return dag


def set_dag_checks(value: str):
    # Airflow reads an option's environment variable on every lookup.
    return mock.patch.dict(os.environ, {DAG_CHECKS: value})


@pytest.fixture
def checks_on():
    with set_dag_checks("error"):
        yield


@pytest.fixture(autouse=True)
def forget_logged_warnings():
    policy.warn_unknown_dag_checks.cache_clear()
    yield
    policy.warn_unknown_dag_checks.cache_clear()


@pytest.mark.parametrize("value", [None, "off"], ids=["unset", "off"])
def test_checks_are_off_unless_turned_on(value):
    with mock.patch.dict(os.environ, {} if value is None else {DAG_CHECKS: value}):
        policy.dag_policy(build_dag(PUSH | ANY_PUSH, outlets=[PUSH]))


def test_checks_that_are_off_leave_the_dag_unexamined():
    with mock.patch.object(policy, "find_problems") as find_problems:
        policy.dag_policy(build_dag(PUSH, outlets=[PUSH]))

    find_problems.assert_not_called()


@pytest.mark.usefixtures("checks_on")
def test_processes_that_imported_no_topic_leave_dags_unexamined(monkeypatch):
    # A Dag can only hold a topic once something has imported the class.
    monkeypatch.delitem(sys.modules, policy.TOPICS_MODULE)

    with (
        mock.patch.object(policy, "are_checks_on") as are_checks_on,
        mock.patch.object(policy, "find_problems") as find_problems,
    ):
        policy.dag_policy(build_dag(Asset("github/my-org/web/push")))

    are_checks_on.assert_not_called()
    find_problems.assert_not_called()


@pytest.mark.parametrize("value", ["warn", "on"])
def test_unknown_value_checks_nothing_and_is_logged_once(caplog, value):
    with set_dag_checks(value):
        policy.dag_policy(build_dag(outlets=[PUSH]))
        policy.dag_policy(build_dag(outlets=[PUSH]))

    warnings = [record for record in caplog.records if record.name == policy.__name__]
    assert [record.levelno for record in warnings] == [logging.WARNING]
    assert f"is {value!r} rather than off or error" in warnings[0].getMessage()


def test_value_is_read_without_case_or_spaces():
    with set_dag_checks(" Error "), pytest.raises(AirflowClusterPolicyViolation):
        policy.dag_policy(build_dag(outlets=[PUSH]))


@pytest.mark.usefixtures("checks_on")
@pytest.mark.parametrize(
    ("dag", "message"),
    [
        pytest.param(
            build_dag(outlets=[PUSH]),
            "task 'deploy' has the webhook topic 'github/my-org/web/push' in its outlets",
            id="outlet",
        ),
        pytest.param(
            build_dag(inlets=[PUSH]),
            "task 'deploy' has the webhook topic 'github/my-org/web/push' in its inlets",
            id="inlet",
        ),
        pytest.param(
            build_mapped_dag(outlets=[PUSH]),
            "task 'deploy' has the webhook topic 'github/my-org/web/push' in its outlets",
            id="outlet-of-a-mapped-task",
        ),
        pytest.param(
            build_dag(PUSH | MAIN_PUSH),
            "'github/my-org/web/push' | 'github/my-org/web/push/main', which one delivery can both match",
            id="name-and-its-prefix",
        ),
        pytest.param(
            build_dag(RELEASE | (PUSH | ANY_PUSH)),
            "'github/my-org/web/push' | 'github/*/push'",
            id="name-and-its-wildcard-in-a-nested-or",
        ),
    ],
)
def test_topic_used_in_a_way_no_delivery_serves_is_refused(dag, message):
    with pytest.raises(AirflowClusterPolicyViolation, match="Dag 'deploy' uses webhook topics") as raised:
        policy.dag_policy(dag)

    assert message in str(raised.value)


@PARTITIONED
@pytest.mark.usefixtures("checks_on")
@pytest.mark.parametrize(
    "assets", [PUSH, PUSH & Asset("snowflake://acme/raw/orders")], ids=["alone", "in-and"]
)
def test_partitioned_timetable_over_a_topic_is_refused(assets):
    dag = build_dag(PartitionedAssetTimetable(assets=assets))

    with pytest.raises(
        AirflowClusterPolicyViolation, match="'github/my-org/web/push' with a PartitionedAsset"
    ):
        policy.dag_policy(dag)


@pytest.mark.usefixtures("checks_on")
@pytest.mark.parametrize(
    "dag",
    [
        pytest.param(build_dag(PUSH), id="one-topic"),
        pytest.param(build_dag(OPENED | REOPENED), id="topics-no-delivery-offers-together"),
        # An & of several assets, a list included, batches a delivery's events into one run.
        pytest.param(build_dag(PUSH & ANY_PUSH), id="overlapping-topics-in-and"),
        pytest.param(build_dag([PUSH, ANY_PUSH]), id="overlapping-topics-in-a-list"),
        pytest.param(build_dag((PUSH & RELEASE) | ANY_PUSH), id="overlapping-topics-across-and"),
        pytest.param(build_dag((PUSH | ANY_PUSH) & RELEASE), id="overlapping-topics-in-or-inside-and"),
        pytest.param(build_dag(PUSH | ANY_PUSH | (RELEASE & OPENED)), id="overlapping-topics-beside-and"),
        pytest.param(build_batching_dag(PUSH | ANY_PUSH), id="overlapping-topics-in-batched-runs"),
        pytest.param(build_dag(PUSH | PUSH), id="one-topic-twice"),
        pytest.param(
            build_dag(PUSH | Asset("github/*/push"), outlets=[Asset("github/my-org/web/push")]),
            id="assets-on-topic-names",
        ),
        pytest.param(build_dag("@daily", outlets=[Asset("s3://bucket/key")]), id="no-topics"),
    ],
)
def test_dag_that_uses_topics_as_deliveries_serve_them_is_accepted(dag):
    policy.dag_policy(dag)


@PARTITIONED
@pytest.mark.usefixtures("checks_on")
def test_partitioned_timetable_over_data_assets_is_accepted():
    policy.dag_policy(build_dag(PartitionedAssetTimetable(assets=Asset("snowflake://acme/raw/orders"))))


@pytest.mark.usefixtures("checks_on")
def test_every_problem_of_a_dag_is_reported_at_once():
    with pytest.raises(AirflowClusterPolicyViolation) as raised:
        policy.dag_policy(build_dag(PUSH | ANY_PUSH, outlets=[RELEASE], inlets=[PUSH]))

    assert str(raised.value).splitlines()[1:] == [
        (
            "- task 'deploy' has the webhook topic 'github/my-org/web/release' in its outlets, but only "
            "deliveries may record a topic's events"
        ),
        (
            "- task 'deploy' has the webhook topic 'github/my-org/web/push' in its inlets, but a topic is "
            "not data to read: the delivery is in triggering_asset_events"
        ),
        (
            "- it schedules on 'github/my-org/web/push' | 'github/*/push', which one delivery can both "
            "match, and from Airflow 3.4 each of its events starts a run: schedule each topic in a Dag of "
            "its own"
        ),
    ]


TWO_DAGS = """
from airflow.sdk import Asset, dag, task

from airflow_inbound_webhooks import WebhookTopic

push = WebhookTopic(webhook="github", path="my-org/web", event="push")


@dag(schedule=push)
def deploy():
    @task(outlets=[push])
    def announce(): ...

    announce()


@dag(schedule=Asset("github/my-org/web/release"))
def publish():
    @task
    def notes(): ...

    notes()


deploy()
publish()
"""


def get_stored_dags(session) -> dict[str, tuple[bool, bool]]:
    session.commit()
    rows = session.execute(select(DagModel.dag_id, DagModel.has_import_errors, DagModel.is_stale))
    return {dag_id: (has_import_errors, is_stale) for dag_id, has_import_errors, is_stale in rows}


@pytest.mark.usefixtures("registered_policy", "clean_db")
def test_refused_dag_stops_every_dag_of_its_file(parse_dag_file, store_dags, session):
    with set_dag_checks("off"):
        store_dags(parse_dag_file(TWO_DAGS))
    assert get_stored_dags(session) == {"deploy": (False, False), "publish": (False, False)}

    with set_dag_checks("error"):
        bag = parse_dag_file(TWO_DAGS)
    store_dags(bag)

    (error,) = bag.import_errors.values()
    assert error.startswith("AirflowClusterPolicyViolation: Dag 'deploy' uses webhook topics")
    assert "'github/my-org/web/push' in its outlets" in error
    assert set(bag.dags) == {"publish"}
    # Airflow marks every Dag of a file with an import error as stale, the one the checks accept too.
    assert get_stored_dags(session) == {"deploy": (True, True), "publish": (True, True)}


# Airflow loads cluster policies from entry points as it initializes, when first imported.
LOAD_DAG_FILE = """
import json
import sys

import airflow

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # Airflow 3.1
    from airflow.models.dagbag import DagBag

print(json.dumps(DagBag(dag_folder=sys.argv[1]).import_errors))
"""


def test_airflow_loads_the_policy_from_its_entry_point(tmp_path):
    declared = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["entry-points"]
    # The installed package's metadata can predate the entry point, so a distribution of its own
    # declares it as pyproject.toml does.
    dist_info = tmp_path / "inbound_webhooks_policy_check-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: inbound-webhooks-policy-check\nVersion: 1.0\n"
    )
    (dist_info / "entry_points.txt").write_text(
        "[airflow.policy]\n"
        + "".join(f"{name} = {value}\n" for name, value in declared["airflow.policy"].items())
    )
    dag_file = tmp_path / "dags.py"
    dag_file.write_text(textwrap.dedent(TWO_DAGS))
    env = {
        **os.environ,
        DAG_CHECKS: "error",
        "PYTHONPATH": os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")])),
    }

    result = subprocess.run(
        [sys.executable, "-c", LOAD_DAG_FILE, str(dag_file)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert declared["airflow.policy"] == {"inbound_webhooks": "airflow_inbound_webhooks.policy"}
    # The errors are the last line: Airflow can log warnings to stdout first.
    (error,) = json.loads(result.stdout.splitlines()[-1]).values()
    assert error.startswith("AirflowClusterPolicyViolation: Dag 'deploy' uses webhook topics")


# Importing the topics module would let every Dag past the first check.
IMPORT_AIRFLOW = """
import sys

import airflow
import airflow_inbound_webhooks.policy

print("airflow_inbound_webhooks.topics" in sys.modules)
"""


# As a library, Airflow does not initialize, so it has not loaded the policy before a name is removed.
LOAD_POLICY_WITHOUT = """
import importlib
import sys

import pluggy

module_name, _, name = sys.argv[1].rpartition(".")
delattr(importlib.import_module(module_name), name)
import airflow.policies
import airflow_inbound_webhooks.policy

manager = pluggy.PluginManager(airflow.policies.local_settings_hookspec.project_name)
manager.add_hookspecs(airflow.policies)
manager.register(airflow_inbound_webhooks.policy)
print([hookimpl.function.__name__ for hookimpl in manager.hook.dag_policy.get_hookimpls()])
"""


@pytest.mark.parametrize(
    "missing",
    [
        "airflow.policies.hookimpl",
        "airflow.exceptions.AirflowClusterPolicyViolation",
        "airflow.configuration.conf",
    ],
)
def test_policy_loads_where_airflow_lacks_a_name_it_uses(missing):
    # Airflow loads the policy as it initializes, so failing to import it would stop every process.
    result = subprocess.run(
        [sys.executable, "-c", LOAD_POLICY_WITHOUT, missing],
        env={**os.environ, "_AIRFLOW__AS_LIBRARY": "1"},
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.splitlines()[-1] == "['dag_policy']"


def test_loading_the_policy_leaves_webhook_topics_unimported():
    # A fresh interpreter, because this test session has imported the topics module already.
    result = subprocess.run(
        [sys.executable, "-c", IMPORT_AIRFLOW], capture_output=True, text=True, check=True
    )

    assert result.stdout.splitlines()[-1] == "False"
