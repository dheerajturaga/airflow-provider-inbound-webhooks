# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Inbound webhooks for Apache Airflow.

``WebhookTopic``, which Dag files import from here, is a public interface. The Python modules are not
one yet and can change in any release; see "Versioning" in the README for what is.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow_inbound_webhooks.sources import (
    SOURCE_TYPES,
    GitHubSource,
    GitLabSource,
    HmacJsonSource,
    JiraSource,
    TokenHeaderSource,
    WebhookEvent,
    WebhookSource,
    WebhookVerificationError,
)

if TYPE_CHECKING:
    from airflow_inbound_webhooks.topics import WebhookTopic

__all__ = [
    "SOURCE_TYPES",
    "GitHubSource",
    "GitLabSource",
    "HmacJsonSource",
    "JiraSource",
    "TokenHeaderSource",
    "WebhookEvent",
    "WebhookSource",
    "WebhookTopic",
    "WebhookVerificationError",
]


def __getattr__(name: str) -> Any:
    # Airflow imports this package in every process to read the provider's metadata, and WebhookTopic
    # needs the Task SDK, so only the Dag files that use it import it.
    if name == "WebhookTopic":
        from airflow_inbound_webhooks.topics import WebhookTopic

        return WebhookTopic
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
