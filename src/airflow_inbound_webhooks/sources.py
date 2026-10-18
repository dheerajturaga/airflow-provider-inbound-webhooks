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
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

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
    :param payload: The body as received. The connection's ``store_payload`` decides what asset
        events and the delivery record keep of it.
    :param correlation_key: What the delivery is about, such as a pull request or a job run, so that
        deliveries about the same thing can be found together. No source type sets one yet.
    """

    path: str | None
    kinds: list[tuple[str, ...]]
    delivery_id: str | None
    payload: dict[str, Any]
    correlation_key: str | None = None


class WebhookSource(ABC):
    """Verifies and describes deliveries from one kind of sender."""

    # What is stored of a delivery's payload unless the connection's store_payload says otherwise.
    default_store_payload: ClassVar[str] = "full"
    # Whether summarize() keeps less than the payload, which store_payload "summary" needs.
    has_summary: ClassVar[bool] = False

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

    def summarize(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Return the parts of a payload worth storing, for source types with ``has_summary``."""
        return payload


# Levels of objects and arrays a JSON body may nest, its own object being the first. Webhook payloads
# nest far less. What stores or reads an accepted payload gives up long before Python's json does:
# MySQL refuses a JSON document nested more than 100 levels deep, counting the value innermost, and
# asset events hold the payload a level down in their extra, so the limit stays well under that.
# Pydantic, with which Airflow's APIs serve asset events, refuses data nested about 255 levels deep,
# and FastAPI's encoder in the deliveries API runs out of recursion at about the same depth.
MAX_JSON_DEPTH = 64
NESTED_TOO_DEEPLY = f"JSON body is nested more than {MAX_JSON_DEPTH} levels deep"


def refuse_constant(name: str) -> Any:
    # Python's json accepts NaN and Infinity, which JSON does not, and MySQL refuses to store them.
    raise ValueError(f"JSON body contains {name}, which JSON does not allow")


def refuse_infinite_number(text: str) -> float:
    # Python's json reads a number too large for a float, such as 1e400, as infinity, as if the body
    # held Infinity.
    if math.isinf(value := float(text)):
        raise ValueError("JSON body contains a number too large to store")
    return value


def is_nested_too_deeply(payload: dict[str, Any]) -> bool:
    # A stack rather than recursion, which would fail on the depths it is meant to refuse.
    stack: list[tuple[Any, int]] = [(payload, 1)]
    while stack:
        value, depth = stack.pop()
        if depth > MAX_JSON_DEPTH:
            return True
        children = value.values() if isinstance(value, dict) else value
        stack.extend((child, depth + 1) for child in children if isinstance(child, (dict, list)))
    return False


def load_json_object(headers: Headers, body: bytes) -> dict[str, Any]:
    content_type = headers.get("content-type", "").split(";")[0].strip()
    if content_type != "application/json":
        raise ValueError(
            f"Expected Content-Type application/json, got {content_type or 'none'}; "
            "configure the sender to deliver JSON"
        )
    try:
        payload = json.loads(body, parse_constant=refuse_constant, parse_float=refuse_infinite_number)
    except RecursionError:
        # json raises RecursionError rather than ValueError for nesting deeper than the interpreter
        # allows, which would otherwise answer a signed delivery with a 500.
        raise ValueError(NESTED_TOO_DEEPLY) from None
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    if is_nested_too_deeply(payload):
        raise ValueError(NESTED_TOO_DEEPLY)
    return payload


SIGNATURE_ENCODINGS = ("hex", "base64")


def verify_hmac_sha256(
    received: str | None, secret: str, body: bytes, *, prefix: str = "", encoding: str = "hex"
) -> None:
    if received is None or not received.startswith(prefix):
        raise WebhookVerificationError("Missing or malformed signature header")
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    received_digest = received[len(prefix) :]
    if encoding == "hex":
        # Hex digits are case-insensitive, and some senders write them in uppercase.
        expected, received_digest = digest.hex(), received_digest.lower()
    else:
        expected = base64.b64encode(digest).decode()
    # compare_digest raises TypeError on non-ASCII str, and header values are attacker-controlled.
    if not hmac.compare_digest(received_digest.encode(), expected.encode()):
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


def get_text(payload: dict[str, Any], dotted_path: str) -> str | None:
    # The built-in sources read a field only as a non-empty string and treat any other value as
    # absent, so a delivery of an unexpected shape gets fewer names, or a 400 if it lacks a required
    # field, rather than a 500.
    value = get_field(payload, dotted_path)
    return value if isinstance(value, str) and value else None


def get_label_titles(labels: Any) -> list[str]:
    # A malformed entry is skipped rather than failing the delivery, whose event is still valid.
    if not isinstance(labels, list):
        return []
    return [
        label["title"] for label in labels if isinstance(label, dict) and isinstance(label.get("title"), str)
    ]


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
        action = get_text(payload, "action")
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
        if repo := get_text(payload, "repository.full_name"):
            return repo
        if org := get_text(payload, "organization.login"):
            return f"{org}/*"
        return None

    @staticmethod
    def find_detail(event: str, action: str | None, payload: dict[str, Any]) -> str | None:
        if action in ("labeled", "unlabeled"):
            return get_text(payload, "label.name")
        if event == "push" and (ref := get_text(payload, "ref") or "").startswith("refs/heads/"):
            return ref.removeprefix("refs/heads/")
        if event == "workflow_run":
            return get_text(payload, "workflow.name")
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
        if not (event := get_text(payload, "object_kind")):
            raise ValueError("Missing object_kind")
        kind: tuple[str, ...] = (event,)
        if action := get_text(payload, "object_attributes.action") or get_text(
            payload, "object_attributes.status"
        ):
            kind = (*kind, action)
        ref = get_text(payload, "ref") or ""
        if event == "push" and ref.startswith("refs/heads/"):
            kind = (*kind, ref.removeprefix("refs/heads/"))
        elif event == "tag_push" and ref.startswith("refs/tags/"):
            kind = (*kind, ref.removeprefix("refs/tags/"))
        previous = set(get_label_titles(get_field(payload, "changes.labels.previous")))
        added = [
            title
            for title in get_label_titles(get_field(payload, "changes.labels.current"))
            if title not in previous
        ]
        return WebhookEvent(
            path=get_text(payload, "project.path_with_namespace"),
            kinds=[kind, *((event, "labeled", label) for label in added)],
            # GitLab 17.4 and later keep Idempotency-Key across retries and resends. Not
            # X-Gitlab-Event-UUID: webhooks triggered by earlier webhooks share it.
            delivery_id=headers.get("Idempotency-Key"),
            payload=payload,
        )


STORED_PAYLOADS = ("full", "summary", "none")
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

    default_store_payload = "summary"
    has_summary = True

    def verify(self, headers: Headers, body: bytes, secret: str) -> None:
        verify_hmac_sha256(headers.get("X-Hub-Signature"), secret, body, prefix="sha256=")

    def parse(self, headers: Headers, body: bytes) -> WebhookEvent:
        payload = load_json_object(headers, body)
        webhook_event = payload.get("webhookEvent")
        if not isinstance(webhook_event, str) or not webhook_event:
            raise ValueError("Missing webhookEvent")
        event = webhook_event.removeprefix("jira:")
        kind: tuple[str, ...] = (event,)
        if event == "issue_created" and (issue_type := get_text(payload, "issue.fields.issuetype.name")):
            kind = (*kind, issue_type)
        return WebhookEvent(
            path=get_text(payload, "issue.fields.project.key") or get_text(payload, "project.key"),
            kinds=list(dict.fromkeys([kind, *self.find_changes(event, payload)])),
            delivery_id=headers.get("X-Atlassian-Webhook-Identifier"),
            payload=payload,
        )

    @staticmethod
    def find_changes(event: str, payload: dict[str, Any]) -> list[tuple[str, ...]]:
        changes: list[tuple[str, ...]] = []
        items = get_field(payload, "changelog.items")
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict) or not (field := get_text(item, "field")):
                continue
            if field == "status" and (status := get_text(item, "toString")):
                changes.append((event, field, status))
            else:
                changes.append((event, field))
            if field == "labels":
                # Jira labels cannot contain spaces, so the changelog lists them space-separated.
                previous = set((get_text(item, "fromString") or "").split())
                current = (get_text(item, "toString") or "").split()
                changes.extend((event, "labeled", label) for label in current if label not in previous)
        return changes

    def summarize(self, payload: dict[str, Any]) -> dict[str, Any]:
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
    ``path_field``, and the optional delivery ID from ``delivery_id_header`` or ``delivery_id_field``.
    Fields are dotted paths into the payload (``data.type``).
    """

    event_field: str | None = None
    event_header: str | None = None
    path_field: str | None = None
    delivery_id_field: str | None = None
    delivery_id_header: str | None = None

    @staticmethod
    def read_delivery_options(source_type: str, extra: dict[str, Any]) -> dict[str, str | None]:
        """Read the options that say where a delivery names its event, path and ID."""
        if ("event_field" in extra) == ("event_header" in extra):
            raise ValueError(f"{source_type} needs exactly one of event_field or event_header")
        if "delivery_id_field" in extra and "delivery_id_header" in extra:
            raise ValueError(f"{source_type} takes at most one of delivery_id_field or delivery_id_header")
        keys = ("event_field", "event_header", "path_field", "delivery_id_field", "delivery_id_header")
        return {key: extra.get(key) for key in keys}

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
        return WebhookEvent(
            path=path or None,
            kinds=[(event,)],
            delivery_id=self.read_delivery_id(headers, payload),
            payload=payload,
        )

    def read_delivery_id(self, headers: Headers, payload: dict[str, Any]) -> str | None:
        if self.delivery_id_header:
            return headers.get(self.delivery_id_header)
        if not self.delivery_id_field:
            return None
        delivery_id = get_field(payload, self.delivery_id_field)
        # Some services number their deliveries.
        if isinstance(delivery_id, int) and not isinstance(delivery_id, bool):
            return str(delivery_id)
        if delivery_id is not None and not isinstance(delivery_id, str):
            raise ValueError(f"{self.delivery_id_field!r} is not a string or an integer")
        return delivery_id


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
            **cls.read_delivery_options("hmac_json", extra),
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
            **cls.read_delivery_options("token_header", extra),
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
