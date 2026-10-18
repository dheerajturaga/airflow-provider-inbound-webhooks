# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
import runpy
import textwrap
from pathlib import Path
from unittest import mock

import pytest
from airflow.sdk import Asset

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


def test_readme_quick_start_writes_the_example_dag():
    # The quick start writes its Dag from the README, so it must stay the file the test above builds.
    heredoc = re.search(r"<<'EOF'\n(.*?\n) *EOF\n", (ROOT / "README.md").read_text(), re.DOTALL)
    # The example without its license header, which the README leaves out.
    example = (ROOT / "examples" / "dags" / "quickstart.py").read_text().split("\n", 2)[2]

    assert heredoc is not None
    assert textwrap.dedent(heredoc.group(1)) == example
