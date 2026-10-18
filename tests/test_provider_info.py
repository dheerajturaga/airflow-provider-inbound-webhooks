# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import subprocess
import sys
from importlib.metadata import metadata
from importlib.resources import files

import jsonschema
from packaging.utils import canonicalize_name

from airflow_inbound_webhooks.get_provider_info import get_provider_info

# Airflow imports the package for its provider metadata in every process, so Dag files alone should
# import what only they use.
DISCOVER_PROVIDER = """
import sys
import airflow_inbound_webhooks
from airflow_inbound_webhooks.get_provider_info import get_provider_info
get_provider_info()
print(*sys.modules)
"""


def test_provider_discovery_leaves_the_task_sdk_unimported():
    # A fresh interpreter, because this test session has imported the Task SDK already.
    result = subprocess.run(
        [sys.executable, "-c", DISCOVER_PROVIDER], capture_output=True, text=True, check=True
    )

    assert {"airflow.sdk", "airflow_inbound_webhooks.topics"} & set(result.stdout.split()) == set()


def test_provider_info_conforms_to_the_installed_airflow_schema():
    schema = json.loads(files("airflow").joinpath("provider_info.schema.json").read_text())

    jsonschema.validate(get_provider_info(), schema)


def test_package_name_matches_the_distribution():
    # Airflow refuses to load a provider whose package-name differs from its distribution's name.
    distribution_name = canonicalize_name(metadata("airflow-provider-inbound-webhooks")["Name"])

    assert get_provider_info()["package-name"] == distribution_name


def test_configuration_options_are_registered():
    # The default can only come from the provider metadata: the test environment sets no value.
    result = subprocess.run(
        [sys.executable, "-m", "airflow", "config", "get-value", "inbound_webhooks", "connection_cache_ttl"],
        capture_output=True,
        text=True,
        check=True,
    )

    # The value is the last line: Airflow's CLI logs warnings to stdout first, such as Python 3.14's
    # deprecation of argparse.FileType.
    assert result.stdout.splitlines()[-1] == "60"
