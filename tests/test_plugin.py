# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import subprocess
import sys

# Only the endpoint uses these, so only the API server should import them, on the first delivery.
ENDPOINT_ONLY_MODULES = {"airflow.assets.manager", "airflow.models.asset"}
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
