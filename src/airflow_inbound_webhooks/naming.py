# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
The naming rule: ``<webhook name>/<path>/<kind segment>/...``.

A Dag subscribes to an event by scheduling on the asset with that name. The rule is shared by
every source, so subscribing works the same way for GitHub, GitLab and any other sender.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_inbound_webhooks.sources import WebhookEvent

# The names the endpoint serves, as in POST /webhooks/<name>.
WEBHOOK_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


def normalize_segment(value: str) -> str:
    # MySQL stores asset names as latin1 and fails any lookup that includes a non-ASCII name; fold on
    # every database so names do not depend on it ("🚀 deploy" -> "deploy", "café" -> "cafe").
    folded = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    # Empty parts are dropped so no name contains "//": "/foo" -> "foo", while a branch such as
    # "feature/x" keeps its slash.
    return "/".join(part for part in folded.split("/") if part.strip()).strip()


def normalize_path(path: str | None) -> str | None:
    """Return ``path`` as names hold it: each segment normalized, and those left empty dropped."""
    segments = [
        normalized for segment in (path or "").split("/") if (normalized := normalize_segment(segment))
    ]
    return "/".join(segments) or None


def build_wildcard_paths(path: str) -> list[str]:
    """Return ``path`` with ``*`` in place of each trailing part, narrow to broad (``a/*``, ``*``)."""
    segments = path.split("/")
    return ["/".join([*segments[:end], "*"]) for end in range(len(segments) - 1, -1, -1)]


def build_name(webhook_name: str, scope: str | None, kind: Iterable[str]) -> str:
    """Join a webhook name, a normalized path or one of its wildcard paths, and kind segments."""
    return "/".join(part for part in (webhook_name, scope, *kind) if part is not None)


def build_candidate_names(webhook_name: str, event: WebhookEvent) -> list[str]:
    """
    Return every asset name a Dag could subscribe to for this delivery, broad to specific.

    Each kind prefix is offered for the event's own path and for the path with ``*`` in place of
    each trailing part, so ``*`` means anything below that point: ``platform/backend/api`` is also
    offered as ``platform/backend/*``, ``platform/*`` and ``*``.

    A path segment that normalizes to nothing is left out of the path, and a kind segment that does
    ends the kind, so no name contains ``//``.
    """
    if (path := normalize_path(event.path)) is None:
        scopes: list[str | None] = [None]
    else:
        scopes = list(dict.fromkeys([path, *build_wildcard_paths(path)]))
    names: dict[str, None] = {}
    for kind in event.kinds:
        prefix: list[str] = []
        for segment in kind:
            if not (normalized := normalize_segment(segment)):
                break
            prefix.append(normalized)
            for scope in scopes:
                names[build_name(webhook_name, scope, prefix)] = None
    return list(names)


def can_be_offered(name: str) -> bool:
    """
    Return whether some delivery can offer ``name``, whatever the source type of its webhook.

    A name starts with a webhook name the endpoint can serve, and has at least one kind segment
    after it. What follows the webhook name is joined from normalized segments, so it is ASCII, with
    no empty parts and no whitespace around it: normalizing it changes nothing. Every such name is
    offered by some delivery: an ``hmac_json`` webhook offers it for an event named all that follows
    the webhook name.
    """
    webhook_name, _, rest = name.partition("/")
    return bool(WEBHOOK_NAME_PATTERN.fullmatch(webhook_name) and rest and normalize_segment(rest) == rest)
