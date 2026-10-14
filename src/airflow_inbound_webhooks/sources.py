# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Per-service adapters: verify a delivery, then describe it as a path and kinds.

These two methods are the only service-specific code; naming, lookup and recording are shared.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from starlette.datastructures import Headers


class WebhookVerificationError(Exception):
    """The request does not carry a valid signature or token for this webhook."""


@dataclass(frozen=True)
class WebhookEvent:
    """
    A verified and parsed delivery.

    :param path: Where it happened, ``/``-separated from broad to narrow (``my-org/web``),
        or None if the sender has no such notion.
    :param kinds: What happened, each from broad to specific
        (``("pull_request", "labeled", "deploy-preview")``). Every prefix of every kind is a
        candidate asset name, so one delivery can be subscribed to at several levels.
    :param payload: What each asset event stores as ``extra["payload"]``: the body, or a summary of
        it for senders whose bodies are large.
    """

    path: str | None
    kinds: list[tuple[str, ...]]
    delivery_id: str | None
    payload: dict[str, Any]


class WebhookSource(ABC):
    """Verifies and describes deliveries from one kind of sender."""

    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> WebhookSource:
        """Build from the ``webhook_<name>`` connection's extra; raise ``ValueError`` if it is invalid."""
        return cls()

    @abstractmethod
    def verify(self, headers: Headers, body: bytes, secret: str) -> None:
        """Raise ``WebhookVerificationError`` unless the request was sent by the holder of ``secret``."""

    @abstractmethod
    def parse(self, headers: Headers, body: bytes) -> WebhookEvent:
        """Describe a verified request; raise ``ValueError`` if the body is malformed."""


def load_json_object(headers: Headers, body: bytes) -> dict[str, Any]:
    content_type = headers.get("content-type", "").split(";")[0].strip()
    if content_type != "application/json":
        raise ValueError(
            f"Expected Content-Type application/json, got {content_type or 'none'}; "
            "configure the sender to deliver JSON"
        )
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    return payload


SIGNATURE_ENCODINGS = ("hex", "base64")


def verify_hmac_sha256(
    received: str | None, secret: str, body: bytes, *, prefix: str = "", encoding: str = "hex"
) -> None:
    if received is None or not received.startswith(prefix):
        raise WebhookVerificationError("Missing or malformed signature header")
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    expected = digest.hex() if encoding == "hex" else base64.b64encode(digest).decode()
    # compare_digest raises TypeError on non-ASCII str, and header values are attacker-controlled.
    if not hmac.compare_digest(received[len(prefix) :].encode(), expected.encode()):
        raise WebhookVerificationError("Signature does not match")


def verify_token(received: str | None, secret: str, *, prefix: str = "") -> None:
    # A bare token proves who sent the request, not that the body is unmodified; integrity
    # depends on TLS between the sender and the API server.
    if received is None or not received.startswith(prefix):
        raise WebhookVerificationError("Missing or malformed token header")
    if not hmac.compare_digest(received[len(prefix) :].encode(), secret.encode()):
        raise WebhookVerificationError("Token does not match")


def get_field(payload: dict[str, Any], dotted_path: str) -> Any:
    value: Any = payload
    for key in dotted_path.split("."):
        value = value.get(key) if isinstance(value, dict) else None
    return value


class GitHubSource(WebhookSource):
    """
    GitHub.com and GitHub Enterprise Server; an organization webhook covers every repository.

    Kinds use GitHub's event and action names. The detail is the label for ``labeled`` and
    ``unlabeled``, the branch for ``push`` (tag pushes have none), and the workflow name for
    ``workflow_run``. Organization-level events have the path ``<org>/*``.
    """

    def verify(self, headers: Headers, body: bytes, secret: str) -> None:
        verify_hmac_sha256(headers.get("X-Hub-Signature-256"), secret, body, prefix="sha256=")

    def parse(self, headers: Headers, body: bytes) -> WebhookEvent:
        payload = load_json_object(headers, body)
        if not (event := headers.get("X-GitHub-Event")):
            raise ValueError("Missing X-GitHub-Event header")
        action = payload.get("action")
        detail = self.find_detail(event, action, payload)
        kind = (event, *(segment for segment in (action, detail) if segment))
        return WebhookEvent(
            path=self.find_path(payload),
            kinds=[kind],
            delivery_id=headers.get("X-GitHub-Delivery"),
            payload=payload,
        )

    @staticmethod
    def find_path(payload: dict[str, Any]) -> str | None:
        if repo := get_field(payload, "repository.full_name"):
            return repo
        if org := get_field(payload, "organization.login"):
            return f"{org}/*"
        return None

    @staticmethod
    def find_detail(event: str, action: str | None, payload: dict[str, Any]) -> str | None:
        if action in ("labeled", "unlabeled"):
            return get_field(payload, "label.name")
        if event == "push" and (ref := payload.get("ref", "")).startswith("refs/heads/"):
            return ref.removeprefix("refs/heads/")
        if event == "workflow_run":
            return get_field(payload, "workflow.name")
        return None


class GitLabSource(WebhookSource):
    """
    GitLab.com and self-managed GitLab; a group webhook covers every project in the group.

    Kinds use the payload's ``object_kind`` with ``object_attributes.action`` (or ``status``, for
    pipelines). The detail is the branch for ``push`` and the tag for ``tag_push``. GitLab has no
    label events, so a label added to a merge request or issue is reported as the derived kind
    ``(<object_kind>, "labeled", <label>)``.
    """

    def verify(self, headers: Headers, body: bytes, secret: str) -> None:
        verify_token(headers.get("X-Gitlab-Token"), secret)

    def parse(self, headers: Headers, body: bytes) -> WebhookEvent:
        payload = load_json_object(headers, body)
        if not (event := payload.get("object_kind")):
            raise ValueError("Missing object_kind")
        attributes = payload.get("object_attributes") or {}
        kind: tuple[str, ...] = (event,)
        if action := attributes.get("action") or attributes.get("status"):
            kind = (*kind, action)
        ref = payload.get("ref", "")
        if event == "push" and ref.startswith("refs/heads/"):
            kind = (*kind, ref.removeprefix("refs/heads/"))
        elif event == "tag_push" and ref.startswith("refs/tags/"):
            kind = (*kind, ref.removeprefix("refs/tags/"))
        previous = {label["title"] for label in get_field(payload, "changes.labels.previous") or []}
        added = [
            label["title"]
            for label in get_field(payload, "changes.labels.current") or []
            if label["title"] not in previous
        ]
        return WebhookEvent(
            path=get_field(payload, "project.path_with_namespace"),
            kinds=[kind, *((event, "labeled", label) for label in added)],
            delivery_id=headers.get("X-Gitlab-Event-UUID"),
            payload=payload,
        )


STORED_PAYLOADS = ("summary", "full")
# Kept in a summary's issue.fields; the full issue repeats every field, custom fields included.
JIRA_SUMMARY_FIELDS = frozenset(
    {
        "project",
        "issuetype",
        "status",
        "summary",
        "labels",
        "priority",
        "assignee",
        "resolution",
        "fixVersions",
        "parent",
    }
)


@dataclass(frozen=True, kw_only=True)
class JiraSource(WebhookSource):
    """
    Jira Cloud and Jira Data Center webhooks with a secret, which sign the body like GitHub does.

    Kinds use ``webhookEvent`` without its ``jira:`` prefix. The detail is the issue type for
    ``issue_created``. ``issue_updated`` offers a kind per field in the changelog,
    ``(issue_updated, <field>)``, with the new status for ``status`` and, as for GitLab, a derived
    ``(issue_updated, "labeled", <label>)`` for each label added. The path is the project key.

    Every delivery carries the whole issue, so by default only a summary is stored:
    ``store_payload`` set to ``full`` stores the body as received.
    """

    store_payload: str = "summary"

    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> JiraSource:
        store_payload = extra.get("store_payload", "summary")
        if store_payload not in STORED_PAYLOADS:
            raise ValueError(f"jira store_payload must be one of {STORED_PAYLOADS}")
        return cls(store_payload=store_payload)

    def verify(self, headers: Headers, body: bytes, secret: str) -> None:
        verify_hmac_sha256(headers.get("X-Hub-Signature"), secret, body, prefix="sha256=")

    def parse(self, headers: Headers, body: bytes) -> WebhookEvent:
        payload = load_json_object(headers, body)
        webhook_event = payload.get("webhookEvent")
        if not isinstance(webhook_event, str) or not webhook_event:
            raise ValueError("Missing webhookEvent")
        event = webhook_event.removeprefix("jira:")
        kind: tuple[str, ...] = (event,)
        if event == "issue_created" and (issue_type := get_field(payload, "issue.fields.issuetype.name")):
            kind = (*kind, issue_type)
        return WebhookEvent(
            path=get_field(payload, "issue.fields.project.key") or get_field(payload, "project.key"),
            kinds=list(dict.fromkeys([kind, *self.find_changes(event, payload)])),
            delivery_id=headers.get("X-Atlassian-Webhook-Identifier"),
            payload=payload if self.store_payload == "full" else self.summarize(payload),
        )

    @staticmethod
    def find_changes(event: str, payload: dict[str, Any]) -> list[tuple[str, ...]]:
        changes: list[tuple[str, ...]] = []
        for item in get_field(payload, "changelog.items") or []:
            if not (field := item.get("field")):
                continue
            if field == "status" and (status := item.get("toString")):
                changes.append((event, field, status))
            else:
                changes.append((event, field))
            if field == "labels":
                # Jira labels cannot contain spaces, so the changelog lists them space-separated.
                previous = set((item.get("fromString") or "").split())
                current = (item.get("toString") or "").split()
                changes.extend((event, "labeled", label) for label in current if label not in previous)
        return changes

    @staticmethod
    def summarize(payload: dict[str, Any]) -> dict[str, Any]:
        issue = payload.get("issue")
        if not isinstance(issue, dict) or not isinstance(fields := issue.get("fields"), dict):
            return payload
        summary_fields = {key: value for key, value in fields.items() if key in JIRA_SUMMARY_FIELDS}
        return {**payload, "issue": {**issue, "fields": summary_fields}}


@dataclass(frozen=True, kw_only=True)
class ConfiguredJsonSource(WebhookSource):
    """
    Base for services configured entirely from the connection extra.

    The event name comes from ``event_header`` or ``event_field``, the optional path from
    ``path_field``. Fields are dotted paths into the payload (``data.type``).
    """

    event_field: str | None = None
    event_header: str | None = None
    path_field: str | None = None

    @staticmethod
    def read_event_options(source_type: str, extra: dict[str, Any]) -> dict[str, str | None]:
        if ("event_field" in extra) == ("event_header" in extra):
            raise ValueError(f"{source_type} needs exactly one of event_field or event_header")
        return {key: extra.get(key) for key in ("event_field", "event_header", "path_field")}

    def parse(self, headers: Headers, body: bytes) -> WebhookEvent:
        payload = load_json_object(headers, body)
        if self.event_header:
            event = headers.get(self.event_header)
        else:
            event = get_field(payload, self.event_field or "")
        if not isinstance(event, str) or not event:
            raise ValueError(f"No event name in {self.event_header or self.event_field!r}")
        path = get_field(payload, self.path_field) if self.path_field else None
        if path is not None and not isinstance(path, str):
            raise ValueError(f"{self.path_field!r} is not a string")
        return WebhookEvent(path=path or None, kinds=[(event,)], delivery_id=None, payload=payload)


@dataclass(frozen=True, kw_only=True)
class HmacJsonSource(ConfiguredJsonSource):
    """
    Any service that signs the raw body with HMAC-SHA256.

    For example::

        {"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event"}

    ``signature_prefix`` strips a scheme such as ``v1=``; ``signature_encoding`` is ``hex``
    (default) or ``base64``.
    """

    signature_header: str
    signature_prefix: str = ""
    signature_encoding: str = "hex"

    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> HmacJsonSource:
        if "signature_header" not in extra:
            raise ValueError("hmac_json needs signature_header in the connection extra")
        encoding = extra.get("signature_encoding", "hex")
        if encoding not in SIGNATURE_ENCODINGS:
            raise ValueError(f"hmac_json signature_encoding must be one of {SIGNATURE_ENCODINGS}")
        return cls(
            signature_header=extra["signature_header"],
            signature_prefix=extra.get("signature_prefix", ""),
            signature_encoding=encoding,
            **cls.read_event_options("hmac_json", extra),
        )

    def verify(self, headers: Headers, body: bytes, secret: str) -> None:
        verify_hmac_sha256(
            headers.get(self.signature_header),
            secret,
            body,
            prefix=self.signature_prefix,
            encoding=self.signature_encoding,
        )


@dataclass(frozen=True, kw_only=True)
class TokenHeaderSource(ConfiguredJsonSource):
    """
    Any service that can only send the shared secret in a header it lets you configure.

    For example::

        {
            "source_type": "token_header",
            "token_header": "Authorization",
            "token_prefix": "Bearer ",
            "event_field": "event_type",
        }
    """

    token_header: str
    token_prefix: str = ""

    @classmethod
    def from_extra(cls, extra: dict[str, Any]) -> TokenHeaderSource:
        if "token_header" not in extra:
            raise ValueError("token_header needs token_header in the connection extra")
        return cls(
            token_header=extra["token_header"],
            token_prefix=extra.get("token_prefix", ""),
            **cls.read_event_options("token_header", extra),
        )

    def verify(self, headers: Headers, body: bytes, secret: str) -> None:
        verify_token(headers.get(self.token_header), secret, prefix=self.token_prefix)


SOURCE_TYPES: dict[str, type[WebhookSource]] = {
    "github": GitHubSource,
    "gitlab": GitLabSource,
    "jira": JiraSource,
    "hmac_json": HmacJsonSource,
    "token_header": TokenHeaderSource,
}
