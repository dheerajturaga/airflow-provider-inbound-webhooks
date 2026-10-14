# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from airflow_inbound_webhooks import configuration, connection_form


def get_provider_info() -> dict[str, Any]:
    """Return the provider metadata Airflow discovers through the ``apache_airflow_provider`` entry point."""
    return {
        # Airflow refuses to load the provider unless this matches the distribution name.
        "package-name": "airflow-provider-inbound-webhooks",
        "name": "Inbound Webhooks",
        "description": "Inbound webhooks: verified deliveries from any service become asset events.",
        "connection-types": [
            {
                "connection-type": connection_form.CONN_TYPE,
                "hook-class-name": "airflow_inbound_webhooks.hooks.InboundWebhookHook",
                "hook-name": connection_form.HOOK_NAME,
                "conn-fields": connection_form.build_conn_fields(),
                "ui-field-behaviour": {
                    "hidden-fields": connection_form.HIDDEN_FIELDS,
                    "relabeling": connection_form.RELABELING,
                },
            }
        ],
        "config": configuration.build_config_section(),
    }
