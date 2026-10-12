# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from airflow_inbound_webhooks.app import build_app


class WebhooksPlugin(AirflowPlugin):
    """Serves ``POST /webhooks/<name>``; registered through the ``airflow.plugins`` entry point."""

    name = "webhooks"
    fastapi_apps = [{"app": build_app(), "url_prefix": "/webhooks", "name": "Webhooks"}]
