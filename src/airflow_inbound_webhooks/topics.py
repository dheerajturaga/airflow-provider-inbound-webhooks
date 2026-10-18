# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Webhook topics: the assets Dags schedule on to run on webhook events.

Dag files import this module, and the Dag processor and workers import Dag files, so it imports only
``airflow.sdk`` and the naming rule.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from airflow.sdk import Asset

from airflow_inbound_webhooks import naming

if TYPE_CHECKING:
    from airflow.sdk import AssetAccessControl, AssetWatcher


class WebhookTopic(Asset):
    """
    An event name a Dag schedules on, as an asset in the group ``webhook``.

    Give the name's segments as the sender names them, and the topic gets the name the endpoint
    builds from them::

        WebhookTopic(
            webhook="github",
            path="my-org/web",
            event="pull_request",
            action="labeled",
            detail="deploy-preview",
        )

    ``path``, ``action`` and ``detail`` are optional; an event without an action, such as GitHub's
    ``push``, takes its detail right after it (``event="push", detail="main"``). A path can hold
    several segments, with ``*`` in place of trailing ones, as in ``my-org/*`` or ``*``. Or give the
    whole name, as a delivery's ``candidates`` list it:
    ``WebhookTopic("github/my-org/web/pull_request/labeled/deploy-preview")``. A name no delivery can
    offer is refused.

    The endpoint and the scheduler match by name, so ``Asset(name)`` matches the same deliveries. A
    topic refuses ``extra`` and ``watchers``: only deliveries record its events.
    """

    asset_type: ClassVar[str] = "webhook"

    def __init__(
        self,
        name: str | None = None,
        uri: str | None = None,
        *,
        webhook: str | None = None,
        path: str | None = None,
        event: str | None = None,
        action: str | None = None,
        detail: str | None = None,
        group: str | None = None,
        extra: dict[str, Any] | None = None,
        watchers: list[AssetWatcher] | None = None,
        access_control: AssetAccessControl | None = None,
    ) -> None:
        if all(segment is None for segment in (webhook, path, event, action, detail)):
            if name is None:
                raise TypeError("WebhookTopic() requires a name, or webhook= and event=")
            check_name(name)
        elif name is not None:
            raise TypeError("WebhookTopic() takes either a name or its segments, not both")
        else:
            name = build_topic_name(webhook, path, event, action, detail)
        # attrs.evolve passes every field back, the URI included, which is the name.
        if uri is not None and uri != name:
            raise ValueError(f"WebhookTopic {name!r} cannot have the URI {uri!r}: a topic's URI is its name")
        if extra:
            raise ValueError(f"WebhookTopic {name!r} takes no extra: each event's extra holds its delivery")
        if watchers:
            raise ValueError(f"WebhookTopic {name!r} takes no watchers: only deliveries record its events")
        kwargs: dict[str, Any] = {}
        if group is not None:
            kwargs["group"] = group
        # Only Airflow 3.3 and later have access_control, so it is passed only when given.
        if access_control is not None:
            kwargs["access_control"] = access_control
        super().__init__(name, **kwargs)


def check_name(name: str) -> None:
    """Refuse a whole name that no delivery can offer, suggesting the name it may stand for."""
    if not isinstance(name, str):
        raise ValueError("WebhookTopic name must be a string")
    if naming.can_be_offered(name):
        return
    webhook_name, _, rest = name.partition("/")
    if not naming.WEBHOOK_NAME_PATTERN.fullmatch(webhook_name):
        raise ValueError(
            f"WebhookTopic name {name!r} does not start with a webhook name, which is lowercase letters, "
            "digits, - and _"
        )
    if not naming.normalize_segment(rest):
        raise ValueError(f"WebhookTopic name {name!r} has no event after the webhook name")
    message = (
        f"WebhookTopic name {name!r} is never offered: names are folded to ASCII, with empty parts and "
        "the spaces around each segment left out"
    )
    # Each part normalized on its own, as the endpoint normalizes a segment without a slash.
    suggestion = naming.normalize_path(name)
    if suggestion is not None and naming.can_be_offered(suggestion):
        message = f"{message}. Did you mean {suggestion!r}?"
    raise ValueError(message)


def build_topic_name(
    webhook: str | None, path: str | None, event: str | None, action: str | None, detail: str | None
) -> str:
    """Build a topic's name from its segments, normalized as the endpoint normalizes a delivery's."""
    if webhook is None or event is None:
        raise TypeError("WebhookTopic() needs both webhook= and event= to build a name from segments")
    segments = {"webhook": webhook, "path": path, "event": event, "action": action, "detail": detail}
    for keyword, value in segments.items():
        if value is not None and not isinstance(value, str):
            raise ValueError(f"WebhookTopic {keyword} must be a string")
    if not naming.WEBHOOK_NAME_PATTERN.fullmatch(webhook):
        raise ValueError(
            f"WebhookTopic webhook {webhook!r} is not a webhook name: use lowercase letters, digits, - and _"
        )
    kind: list[str] = []
    # An event without an action, such as GitHub's push, has its detail right after it: push/main.
    for keyword in ("event", "action", "detail"):
        if (value := segments[keyword]) is None:
            continue
        # The endpoint ends a name at a segment that normalizes to nothing, so none goes past it.
        if not (normalized := naming.normalize_segment(value)):
            raise ValueError(
                f"WebhookTopic {keyword} {value!r} is empty once folded to ASCII, so no delivery offers "
                "a name with it"
            )
        kind.append(normalized)
    return naming.build_name(webhook, naming.normalize_path(path), kind)
