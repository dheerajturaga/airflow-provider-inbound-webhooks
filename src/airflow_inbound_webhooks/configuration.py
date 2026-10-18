# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
The ``[inbound_webhooks]`` configuration options, defined once.

The provider metadata declares them, so ``airflow config list`` shows them with their defaults. The
endpoint reads them with the same defaults as fallbacks, so it does not depend on the process having
loaded provider configuration.
"""

from __future__ import annotations

from typing import Any

SECTION = "inbound_webhooks"
WEBHOOK_NAMES = "webhook_names"
CONNECTION_CACHE_TTL = "connection_cache_ttl"
RATE_LIMIT_PER_MINUTE = "rate_limit_per_minute"
DELIVERY_RETENTION = "delivery_retention"
MAX_EVENT_PAYLOAD_BYTES = "max_event_payload_bytes"
DAG_CHECKS = "dag_checks"
ANY_WEBHOOK_NAME = "*"
DAG_CHECKS_OFF = "off"
DAG_CHECKS_ERROR = "error"
DEFAULT_CONNECTION_CACHE_TTL = 60
DEFAULT_DELIVERY_RETENTION_DAYS = 7
# No limit, so Dags keep receiving the payloads they read; AIP-125 proposes 65536 for Airflow.
DEFAULT_MAX_EVENT_PAYLOAD_BYTES = 0


def build_config_section() -> dict[str, Any]:
    """Return the options in the provider metadata's ``config`` format."""
    return {
        SECTION: {
            "description": "The inbound webhooks endpoint, POST /webhooks/<name> on the API server.",
            "options": {
                WEBHOOK_NAMES: {
                    "description": (
                        "Comma-separated names of the webhooks to serve, each with a webhook_<name> "
                        "connection. Requests for other names are answered with 404 without looking up "
                        "a connection, so they cannot add calls to a secrets backend. Empty serves no "
                        f"webhook. {ANY_WEBHOOK_NAME}, which serves any name and looks its connection up on "
                        "every delivery, is deprecated and will be removed in a later release."
                    ),
                    "version_added": "0.1.0",
                    "type": "string",
                    "example": "github,jira",
                    "default": "",
                },
                CONNECTION_CACHE_TTL: {
                    "description": (
                        "Seconds a listed webhook's connection, or the failure to read it, is reused "
                        "before it is read again. A changed option takes up to this long to apply. A "
                        "delivery that fails verification has the connection read again, at most once per "
                        "this period, so a secret changed in the connection applies to the first delivery "
                        "signed with it. 0 reads the connection on every delivery."
                    ),
                    "version_added": "0.1.0",
                    "type": "integer",
                    "example": "300",
                    "default": str(DEFAULT_CONNECTION_CACHE_TTL),
                },
                RATE_LIMIT_PER_MINUTE: {
                    "description": (
                        "Verified deliveries each API server worker records per webhook and minute. More "
                        "are answered with 429 and Retry-After before anything is recorded; requests that "
                        "fail verification do not count. Each worker counts on its own, so N workers "
                        "record up to N times as many. 0 turns the limit off."
                    ),
                    "version_added": "0.2.0",
                    "type": "integer",
                    "example": "600",
                    "default": "0",
                },
                DELIVERY_RETENTION: {
                    "description": (
                        "Days of delivery records that airflow webhooks clean keeps unless told otherwise. "
                        "Keep it longer than any of your services retries a delivery: a retry that comes "
                        "after its first delivery's record is deleted is recorded again."
                    ),
                    "version_added": "0.2.0",
                    "type": "integer",
                    "example": "30",
                    "default": str(DEFAULT_DELIVERY_RETENTION_DAYS),
                },
                MAX_EVENT_PAYLOAD_BYTES: {
                    "description": (
                        "Bytes of the largest payload copied into a delivery's asset events, measured as "
                        "compact JSON in UTF-8 after store_payload, so for jira the summary is measured. A "
                        "larger payload is left out: the events have payload null and payload_truncated "
                        "true. The delivery record keeps the payload, which the deliveries page and API "
                        "show until airflow webhooks clean deletes the record after delivery_retention. 0 "
                        "copies every payload. AIP-125 proposes 65536 as Airflow's default."
                    ),
                    "version_added": "0.2.0",
                    "type": "integer",
                    "example": "65536",
                    "default": str(DEFAULT_MAX_EVENT_PAYLOAD_BYTES),
                },
                DAG_CHECKS: {
                    "description": (
                        f"{DAG_CHECKS_ERROR} refuses to load a Dag that uses a WebhookTopic in a way no "
                        "delivery can serve: in a task's outlets or inlets, in a PartitionedAssetTimetable, "
                        "or in a | condition with another topic that one delivery can also match. Its Dag "
                        "file then shows an import error that says why, and every Dag in that file stops "
                        "running, not only the refused one, so keep Dags that schedule on topics in files "
                        f"of their own before turning the checks on. {DAG_CHECKS_OFF} checks nothing, and "
                        "so does any other value, which is logged as a warning. It is read wherever Dag "
                        "files are parsed, so set the same value on the Dag processor and the workers. "
                        "There is no warning mode, because a warning raised while a Dag is checked reaches "
                        "only the Dag processor's log, never the UI."
                    ),
                    "version_added": "0.2.0",
                    "type": "string",
                    "example": DAG_CHECKS_ERROR,
                    "default": DAG_CHECKS_OFF,
                },
            },
        }
    }
