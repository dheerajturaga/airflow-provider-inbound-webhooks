# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
import runpy
import textwrap
from pathlib import Path

import pytest
from airflow.sdk import Asset

ROOT = Path(__file__).parents[1]
EXAMPLE_DAGS = sorted((ROOT / "examples" / "dags").glob("*.py"))


@pytest.mark.parametrize("path", EXAMPLE_DAGS, ids=[path.name for path in EXAMPLE_DAGS])
def test_example_dag_file_builds_its_dags(path):
    module_globals = runpy.run_path(str(path))

    assert any(isinstance(value, Asset) for value in module_globals.values())


def test_readme_quick_start_writes_the_example_dag():
    # The quick start writes its Dag from the README, so it must stay the file the test above builds.
    heredoc = re.search(r"<<'EOF'\n(.*?\n) *EOF\n", (ROOT / "README.md").read_text(), re.DOTALL)
    # The example without its license header, which the README leaves out.
    example = (ROOT / "examples" / "dags" / "quickstart.py").read_text().split("\n", 2)[2]

    assert heredoc is not None
    assert textwrap.dedent(heredoc.group(1)) == example
