# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from pathlib import Path

import pytest
from airflow.sdk import Asset

EXAMPLE_DAGS = sorted((Path(__file__).parents[1] / "examples" / "dags").glob("*.py"))


@pytest.mark.parametrize("path", EXAMPLE_DAGS, ids=[path.name for path in EXAMPLE_DAGS])
def test_example_dag_file_builds_its_dags(path):
    module_globals = runpy.run_path(str(path))

    assert any(isinstance(value, Asset) for value in module_globals.values())
