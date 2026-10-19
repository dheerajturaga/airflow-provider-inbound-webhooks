# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import re
import runpy
import textwrap
from pathlib import Path
from unittest import mock

import pytest
from airflow.sdk import Asset
from airflow.sdk.execution_time.context import TriggeringAssetEventsAccessor

ROOT = Path(__file__).parents[1]
EXAMPLE_DAGS = sorted((ROOT / "examples" / "dags").glob("*.py"))


@pytest.mark.parametrize("path", EXAMPLE_DAGS, ids=[path.name for path in EXAMPLE_DAGS])
def test_example_dag_file_builds_its_dags(path):
    module_globals = runpy.run_path(str(path))

    assert any(isinstance(value, Asset) for value in module_globals.values())


def build_event(timestamp: int, payload: dict | None) -> mock.Mock:
    """Return an asset event as a task receives it, with its payload, or left out if it is None."""
    return mock.Mock(timestamp=timestamp, extra={"payload": payload, "payload_truncated": payload is None})


@pytest.mark.parametrize(
    ("file_name", "read_payloads", "payload", "expected"),
    [
        pytest.param("ci_cd.py", "get_payloads", {"after": "abc123"}, {"after": "abc123"}, id="ci_cd"),
        pytest.param(
            "jira_issues.py", "get_issues", {"issue": {"key": "OPS-1"}}, {"key": "OPS-1"}, id="jira"
        ),
        pytest.param(
            "webhook_topics.py", "get_payloads", {"after": "abc123"}, {"after": "abc123"}, id="webhook_topics"
        ),
    ],
)
def test_example_dags_skip_payloads_left_out_of_their_events(file_name, read_payloads, payload, expected):
    module_globals = runpy.run_path(str(ROOT / "examples" / "dags" / file_name))
    triggering_asset_events = {"asset": [build_event(2, payload), build_event(1, None)]}

    assert module_globals[read_payloads](triggering_asset_events, "asset") == [expected]


@pytest.mark.usefixtures("registered_policy")
@pytest.mark.parametrize("path", EXAMPLE_DAGS, ids=[path.name for path in EXAMPLE_DAGS])
def test_example_dag_file_passes_the_topic_checks(parse_dag_file, path):
    with mock.patch.dict(os.environ, {"AIRFLOW__INBOUND_WEBHOOKS__DAG_CHECKS": "error"}):
        bag = parse_dag_file(path.read_text())

    assert bag.import_errors == {}
    assert bag.dags


@pytest.mark.parametrize("path", EXAMPLE_DAGS, ids=[path.name for path in EXAMPLE_DAGS])
def test_example_dag_tasks_handle_a_run_started_by_hand(parse_dag_file, path):
    tasks = [task for dag in parse_dag_file(path.read_text()).dags.values() for task in dag.tasks]

    # A run started by hand carries no asset events: each task copes rather than fails.
    assert tasks
    for task in tasks:
        task.python_callable(triggering_asset_events=TriggeringAssetEventsAccessor.build([]))


def test_quick_start_writes_the_example_dag():
    # The quick start writes its Dag from docs/quick-start.md, so it must stay the file the test above
    # builds.
    heredoc = re.search(r"<<'EOF'\n(.*?\n) *EOF\n", (ROOT / "docs" / "quick-start.md").read_text(), re.DOTALL)
    # The example without its license header, which the quick start leaves out.
    example = (ROOT / "examples" / "dags" / "quickstart.py").read_text().split("\n", 2)[2]

    assert heredoc is not None
    assert textwrap.dedent(heredoc.group(1)) == example
