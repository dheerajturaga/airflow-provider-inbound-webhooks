# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import subprocess
import sys
from unittest import mock

import pytest

from airflow_inbound_webhooks.plugin import get_deliveries_page_href

# Only the endpoint uses these, so only the API server should import them, on the first delivery.
ENDPOINT_ONLY_MODULES = {
    "airflow.assets.manager",
    "airflow.models.asset",
    "airflow_inbound_webhooks.models",
    # What the deliveries page needs to check a login.
    "airflow.api_fastapi.core_api.security",
}
# Airflow imports plugins in every process, including each task run, once the Task SDK is loaded.
# Airflow 3.1 imports the endpoint's modules itself while loading it, so only what the plugin adds counts.
IMPORT_PLUGIN = """
import sys
import airflow.sdk
loaded_by_airflow = set(sys.modules)
import airflow_inbound_webhooks.plugin
print(*set(sys.modules) - loaded_by_airflow)
"""


def test_importing_the_plugin_leaves_endpoint_internals_unimported():
    # A fresh interpreter, because this test session has imported the models already.
    result = subprocess.run([sys.executable, "-c", IMPORT_PLUGIN], capture_output=True, text=True, check=True)

    assert ENDPOINT_ONLY_MODULES & set(result.stdout.split()) == set()


@pytest.mark.parametrize(
    ("base_url", "expected_href"),
    [
        pytest.param("", "/webhooks/_ui/deliveries", id="unset"),
        pytest.param("http://localhost:8080", "/webhooks/_ui/deliveries", id="root"),
        pytest.param("https://example.com/airflow/", "/airflow/webhooks/_ui/deliveries", id="under-a-path"),
    ],
)
def test_deliveries_page_is_framed_from_where_airflow_is_served(base_url, expected_href):
    with mock.patch.dict(os.environ, {"AIRFLOW__API__BASE_URL": base_url}):
        assert get_deliveries_page_href() == expected_href
