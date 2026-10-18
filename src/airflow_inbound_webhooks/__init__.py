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
    # WebhookTopic imports the Task SDK, and so Airflow, which initializes as it is imported. Only Dag
    # files need it, so importing the package alone, as running the test sender with python -m does,
    # imports nothing from Airflow. Airflow's own processes save nothing by it, since importing
    # Airflow imports the Task SDK too, but the cluster policy, which they all import, tells from
    # sys.modules whether a Dag file has used WebhookTopic.
    if name == "WebhookTopic":
        from airflow_inbound_webhooks.topics import WebhookTopic

        return WebhookTopic
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted([*globals(), "WebhookTopic"])
