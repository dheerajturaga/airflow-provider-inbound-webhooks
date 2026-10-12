# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
The naming rule: ``<webhook name>/<path>/<kind segment>/...``.

A Dag subscribes to an event by scheduling on the asset with that name. The rule is shared by
every source, so subscribing works the same way for GitHub, GitLab and any other sender.
"""

from __future__ import annotations

import unicodedata
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_inbound_webhooks.sources import WebhookEvent


def normalize_segment(value: str) -> str:
    # Airflow rejects non-ASCII asset names on MySQL; fold on every backend so names do not
    # depend on the database ("🚀 deploy" -> "deploy", "café" -> "cafe").
    return unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().strip()


def build_wildcard_path(path: str) -> str:
    head, separator, _ = path.rpartition("/")
    return f"{head}{separator}*"


def build_candidate_names(webhook_name: str, event: WebhookEvent) -> list[str]:
    """
    Return every asset name a Dag could subscribe to for this delivery, broad to specific.

    Each kind prefix is offered for the event's own path and for the path with its last
    segment replaced by ``*`` (``my-org/web`` -> ``my-org/*``: any repository in the org).
    """
    if event.path:
        path = "/".join(normalize_segment(segment) for segment in event.path.split("/"))
        scopes: list[str | None] = list(dict.fromkeys([path, build_wildcard_path(path)]))
    else:
        scopes = [None]
    names: dict[str, None] = {}
    for kind in event.kinds:
        prefix: list[str] = []
        for segment in kind:
            if not (normalized := normalize_segment(segment)):
                break
            prefix.append(normalized)
            for scope in scopes:
                names["/".join(part for part in (webhook_name, scope, *prefix) if part is not None)] = None
    return list(names)
