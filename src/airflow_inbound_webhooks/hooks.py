# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from airflow.sdk import BaseHook

from airflow_inbound_webhooks import connection_form


class InboundWebhookHook(BaseHook):
    """
    Registers the "Inbound Webhook" connection type and its form.

    Webhook connections are read by the plugin's endpoint, so the hook has no client to create.
    """

    conn_name_attr = "webhook_conn_id"
    default_conn_name = "webhook_default"
    conn_type = connection_form.CONN_TYPE
    hook_name = connection_form.HOOK_NAME

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return the form's extra fields; only Airflow 3.1 calls this, later versions use conn-fields."""
        return connection_form.build_form_widgets()

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return the standard-field behaviour; only Airflow 3.1 calls this."""
        return connection_form.build_ui_field_behaviour()
