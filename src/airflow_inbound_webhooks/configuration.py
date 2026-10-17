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
ANY_WEBHOOK_NAME = "*"
DEFAULT_CONNECTION_CACHE_TTL = 60


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
                        f"webhook; {ANY_WEBHOOK_NAME} serves any name and looks its connection up on "
                        "every delivery."
                    ),
                    "version_added": "0.1.0",
                    "type": "string",
                    "example": "github,jira",
                    "default": "",
                },
                CONNECTION_CACHE_TTL: {
                    "description": (
                        "Seconds a listed webhook's connection, or the failure to read it, is reused "
                        "before it is read again. A rotated secret or changed option takes up to this "
                        "long to apply. 0 reads the connection on every delivery."
                    ),
                    "version_added": "0.1.0",
                    "type": "integer",
                    "example": "300",
                    "default": str(DEFAULT_CONNECTION_CACHE_TTL),
                },
                RATE_LIMIT_PER_MINUTE: {
                    "description": (
                        "Deliveries each API server worker accepts per webhook and minute. More are "
                        "answered with 429 and Retry-After before their body is read or verified. Each "
                        "worker counts on its own, so N workers accept up to N times as many. 0 turns the "
                        "limit off."
                    ),
                    "version_added": "0.2.0",
                    "type": "integer",
                    "example": "600",
                    "default": "0",
                },
            },
        }
    }
