# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin

from airflow_inbound_webhooks.app import build_app


def get_deliveries_page_href() -> str:
    # The UI loads the page into a frame from this URL, so it carries the path Airflow is served under,
    # as Airflow derives it from [api] base_url.
    root_path = urlsplit(conf.get("api", "base_url", fallback="")).path.rstrip("/")
    return f"{root_path}/webhooks/_ui/deliveries"


class WebhooksPlugin(AirflowPlugin):
    """Serves ``POST /webhooks/<name>``; registered through the ``airflow.plugins`` entry point."""

    name = "webhooks"
    fastapi_apps = [{"app": build_app(), "url_prefix": "/webhooks", "name": "Webhooks"}]
    external_views = [
        {
            "name": "Webhook deliveries",
            "href": get_deliveries_page_href(),
            "destination": "nav",
            "category": "browse",
            "url_route": "webhook-deliveries",
        }
    ]
