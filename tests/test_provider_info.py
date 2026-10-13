# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from importlib.metadata import metadata
from importlib.resources import files

import jsonschema
from packaging.utils import canonicalize_name

from airflow_inbound_webhooks.get_provider_info import get_provider_info


def test_provider_info_conforms_to_the_installed_airflow_schema():
    schema = json.loads(files("airflow").joinpath("provider_info.schema.json").read_text())

    jsonschema.validate(get_provider_info(), schema)


def test_package_name_matches_the_distribution():
    # Airflow refuses to load a provider whose package-name differs from its distribution's name.
    distribution_name = canonicalize_name(metadata("airflow-provider-inbound-webhooks")["Name"])

    assert get_provider_info()["package-name"] == distribution_name
