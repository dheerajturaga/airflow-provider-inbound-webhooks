# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest
from starlette.datastructures import Headers

from airflow_inbound_webhooks.naming import build_candidate_names
from airflow_inbound_webhooks.sources import (
    GitHubSource,
    GitLabSource,
    HmacJsonSource,
    JiraSource,
    TokenHeaderSource,
    WebhookVerificationError,
    load_json_object,
)

SECRET = "s3cret"
BODY = b'{"ref": "refs/heads/main"}'
JSON = {"content-type": "application/json"}


def compute_hex_signature(body: bytes = BODY, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def parse_json(source, payload: dict, **headers):
    return source.parse(Headers({**JSON, **headers}), json.dumps(payload).encode())


class TestGitHubSource:
    def test_verify_accepts_valid_signature(self):
        GitHubSource().verify(
            Headers({"X-Hub-Signature-256": f"sha256={compute_hex_signature()}"}), BODY, SECRET
        )

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="missing"),
            pytest.param({"X-Hub-Signature-256": compute_hex_signature()}, id="missing-prefix"),
            pytest.param(
                {"X-Hub-Signature-256": f"sha256={compute_hex_signature(secret='wrong')}"}, id="wrong-secret"
            ),
            pytest.param({"X-Hub-Signature-256": "sha256=é"}, id="non-ascii"),
        ],
    )
    def test_verify_rejects(self, headers):
        with pytest.raises(WebhookVerificationError):
            GitHubSource().verify(Headers(headers), BODY, SECRET)

    @pytest.mark.parametrize(
        ("event", "payload", "expected_path", "expected_kind"),
        [
            pytest.param(
                "pull_request",
                {
                    "action": "labeled",
                    "label": {"name": "deploy-preview"},
                    "repository": {"full_name": "o/web"},
                },
                "o/web",
                ("pull_request", "labeled", "deploy-preview"),
                id="label-added",
            ),
            pytest.param(
                "pull_request",
                {"action": "opened", "repository": {"full_name": "o/web"}},
                "o/web",
                ("pull_request", "opened"),
                id="no-detail",
            ),
            pytest.param(
                "push",
                {"ref": "refs/heads/feature/x", "repository": {"full_name": "o/web"}},
                "o/web",
                ("push", "feature/x"),
                id="branch-push",
            ),
            pytest.param(
                "push",
                {"ref": "refs/tags/v1.0", "repository": {"full_name": "o/web"}},
                "o/web",
                ("push",),
                id="tag-push",
            ),
            pytest.param(
                "workflow_run",
                {"action": "completed", "workflow": {"name": "build"}, "repository": {"full_name": "o/web"}},
                "o/web",
                ("workflow_run", "completed", "build"),
                id="workflow-run",
            ),
            pytest.param(
                "organization",
                {"action": "member_added", "organization": {"login": "o"}},
                "o/*",
                ("organization", "member_added"),
                id="org-level",
            ),
        ],
    )
    def test_parse(self, event, payload, expected_path, expected_kind):
        parsed = parse_json(GitHubSource(), payload, **{"X-GitHub-Event": event, "X-GitHub-Delivery": "d-1"})

        assert (parsed.path, parsed.kinds, parsed.delivery_id) == (expected_path, [expected_kind], "d-1")

    @pytest.mark.parametrize(
        ("headers", "body"),
        [
            pytest.param({**JSON}, b"{}", id="no-event-header"),
            pytest.param(
                {"content-type": "application/x-www-form-urlencoded", "X-GitHub-Event": "push"},
                b"payload=%7B%7D",
                id="form",
            ),
            pytest.param({**JSON, "X-GitHub-Event": "push"}, b"[]", id="not-an-object"),
            # Deeper than the recursion limit, but well within the default max_body_bytes.
            pytest.param(
                {**JSON, "X-GitHub-Event": "push"},
                b"[" * 200_000 + b"]" * 200_000,
                id="nested-too-deeply",
            ),
        ],
    )
    def test_parse_rejects(self, headers, body):
        with pytest.raises(ValueError, match=r"X-GitHub-Event|JSON"):
            GitHubSource().parse(Headers(headers), body)


class TestGitLabSource:
    def test_verify_accepts_matching_token(self):
        GitLabSource().verify(Headers({"X-Gitlab-Token": SECRET}), BODY, SECRET)

    @pytest.mark.parametrize("headers", [{}, {"X-Gitlab-Token": "wrong"}], ids=["missing", "wrong"])
    def test_verify_rejects(self, headers):
        with pytest.raises(WebhookVerificationError):
            GitLabSource().verify(Headers(headers), BODY, SECRET)

    @pytest.mark.parametrize(
        ("payload", "expected_kinds"),
        [
            pytest.param(
                {
                    "object_kind": "merge_request",
                    "object_attributes": {"action": "update"},
                    "changes": {
                        "labels": {
                            "previous": [{"title": "bug"}],
                            "current": [{"title": "bug"}, {"title": "deploy-preview"}],
                        }
                    },
                },
                [("merge_request", "update"), ("merge_request", "labeled", "deploy-preview")],
                id="label-added",
            ),
            pytest.param(
                {
                    "object_kind": "merge_request",
                    "object_attributes": {"action": "update"},
                    "changes": {
                        "labels": {
                            "previous": "bug",
                            "current": ["bug", {"name": "x"}, {"title": 1}, {"title": "deploy-preview"}],
                        }
                    },
                },
                [("merge_request", "update"), ("merge_request", "labeled", "deploy-preview")],
                id="malformed-labels-skipped",
            ),
            pytest.param(
                {"object_kind": "push", "ref": "refs/heads/main"}, [("push", "main")], id="branch-push"
            ),
            pytest.param(
                {"object_kind": "tag_push", "ref": "refs/tags/v1.0"}, [("tag_push", "v1.0")], id="tag-push"
            ),
            pytest.param(
                {"object_kind": "pipeline", "object_attributes": {"status": "success"}},
                [("pipeline", "success")],
                id="pipeline-status",
            ),
        ],
    )
    def test_parse(self, payload, expected_kinds):
        payload = {**payload, "project": {"path_with_namespace": "group/sub/web"}}

        parsed = parse_json(GitLabSource(), payload, **{"Idempotency-Key": "k-1"})

        assert (parsed.path, parsed.kinds, parsed.delivery_id) == ("group/sub/web", expected_kinds, "k-1")

    def test_event_uuid_is_not_a_delivery_id(self):
        # Recursive webhooks share it, so it would take distinct deliveries for retries.
        payload = {"object_kind": "push", "project": {"path_with_namespace": "group/web"}}

        parsed = parse_json(GitLabSource(), payload, **{"X-Gitlab-Event-UUID": "u-1"})

        assert parsed.delivery_id is None

    def test_parse_rejects_missing_object_kind(self):
        with pytest.raises(ValueError, match="object_kind"):
            parse_json(GitLabSource(), {})


class TestJiraSource:
    ISSUE = {
        "id": "10042",
        "key": "OPS-42",
        "fields": {
            "project": {"key": "OPS"},
            "issuetype": {"name": "Bug"},
            "status": {"name": "Ready for Deploy"},
            "summary": "Checkout fails",
            "description": "A long description",
            "customfield_10016": 5,
        },
    }

    def test_verify_accepts_valid_signature(self):
        JiraSource().verify(Headers({"X-Hub-Signature": f"sha256={compute_hex_signature()}"}), BODY, SECRET)

    def test_verify_accepts_atlassian_example_signature(self):
        signature = "sha256=a4771c39fbe90f317c7824e83ddef3caae9cb3d976c214ace1f2937e133263c9"

        JiraSource().verify(
            Headers({"X-Hub-Signature": signature}), b"Hello World!", "It's a Secret to Everybody"
        )

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="missing"),
            pytest.param({"X-Hub-Signature": compute_hex_signature()}, id="missing-prefix"),
            pytest.param(
                {"X-Hub-Signature": f"sha256={compute_hex_signature(secret='wrong')}"}, id="wrong-secret"
            ),
            pytest.param({"X-Hub-Signature-256": f"sha256={compute_hex_signature()}"}, id="github-header"),
        ],
    )
    def test_verify_rejects(self, headers):
        with pytest.raises(WebhookVerificationError):
            JiraSource().verify(Headers(headers), BODY, SECRET)

    @pytest.mark.parametrize(
        ("payload", "expected_path", "expected_kinds"),
        [
            pytest.param(
                {"webhookEvent": "jira:issue_created", "issue": ISSUE},
                "OPS",
                [("issue_created", "Bug")],
                id="issue-created",
            ),
            pytest.param(
                {
                    "webhookEvent": "jira:issue_updated",
                    "issue": ISSUE,
                    "changelog": {
                        "items": [
                            {"field": "status", "fromString": "In Progress", "toString": "Ready for Deploy"},
                            {"field": "labels", "fromString": "backend", "toString": "backend deploy"},
                            {"field": "Story Points", "fromString": None, "toString": "5"},
                        ]
                    },
                },
                "OPS",
                [
                    ("issue_updated",),
                    ("issue_updated", "status", "Ready for Deploy"),
                    ("issue_updated", "labels"),
                    ("issue_updated", "labeled", "deploy"),
                    ("issue_updated", "Story Points"),
                ],
                id="issue-updated",
            ),
            pytest.param(
                {
                    "webhookEvent": "jira:issue_updated",
                    "issue": ISSUE,
                    "changelog": {"items": [{"field": "labels", "fromString": "backend", "toString": ""}]},
                },
                "OPS",
                [("issue_updated",), ("issue_updated", "labels")],
                id="label-removed",
            ),
            pytest.param(
                {"webhookEvent": "comment_created", "issue": ISSUE, "comment": {"body": "Looks good"}},
                "OPS",
                [("comment_created",)],
                id="comment",
            ),
            pytest.param(
                {"webhookEvent": "project_created", "project": {"key": "NEW"}},
                "NEW",
                [("project_created",)],
                id="project",
            ),
            pytest.param(
                {"webhookEvent": "sprint_started", "sprint": {"id": 7, "originBoardId": 3}},
                None,
                [("sprint_started",)],
                id="no-project-key",
            ),
        ],
    )
    def test_parse(self, payload, expected_path, expected_kinds):
        parsed = parse_json(JiraSource(), payload, **{"X-Atlassian-Webhook-Identifier": "w-1"})

        assert (parsed.path, parsed.kinds, parsed.delivery_id) == (expected_path, expected_kinds, "w-1")

    def test_summary_keeps_key_issue_fields(self):
        changelog = {"items": [{"field": "status", "toString": "Ready for Deploy"}]}
        payload = {"webhookEvent": "jira:issue_updated", "issue": self.ISSUE, "changelog": changelog}

        assert JiraSource().summarize(payload) == {
            "webhookEvent": "jira:issue_updated",
            "issue": {
                "id": "10042",
                "key": "OPS-42",
                "fields": {
                    "project": {"key": "OPS"},
                    "issuetype": {"name": "Bug"},
                    "status": {"name": "Ready for Deploy"},
                    "summary": "Checkout fails",
                },
            },
            "changelog": changelog,
        }

    def test_parse_keeps_the_payload_as_received(self):
        # The connection's store_payload decides what is stored of it.
        payload = {"webhookEvent": "jira:issue_created", "issue": self.ISSUE}

        assert parse_json(JiraSource(), payload).payload == payload

    def test_parse_rejects_missing_webhook_event(self):
        with pytest.raises(ValueError, match="webhookEvent"):
            parse_json(JiraSource(), {"issue": self.ISSUE})


class TestHmacJsonSource:
    EXTRA = {"signature_header": "X-Signature", "event_field": "data.type", "path_field": "service"}

    def test_from_extra_verify_and_parse(self):
        source = HmacJsonSource.from_extra(self.EXTRA)
        body = json.dumps({"data": {"type": "deployed"}, "service": "billing"}).encode()
        headers = Headers({**JSON, "X-Signature": compute_hex_signature(body)})

        source.verify(headers, body, SECRET)
        parsed = source.parse(headers, body)

        assert (parsed.path, parsed.kinds) == ("billing", [("deployed",)])

    @pytest.mark.parametrize(
        ("options", "signature"),
        [
            pytest.param({"signature_prefix": "v1="}, f"v1={compute_hex_signature()}", id="prefixed-hex"),
            pytest.param({}, compute_hex_signature().upper(), id="uppercase-hex"),
            pytest.param(
                {"signature_encoding": "base64"},
                base64.b64encode(hmac.new(SECRET.encode(), BODY, hashlib.sha256).digest()).decode(),
                id="base64",
            ),
        ],
    )
    def test_verify_signature_formats(self, options, signature):
        source = HmacJsonSource.from_extra({**self.EXTRA, **options})

        source.verify(Headers({"X-Signature": signature}), BODY, SECRET)

    def test_base64_signature_is_case_sensitive(self):
        source = HmacJsonSource.from_extra({**self.EXTRA, "signature_encoding": "base64"})
        signature = base64.b64encode(hmac.new(SECRET.encode(), BODY, hashlib.sha256).digest()).decode()

        with pytest.raises(WebhookVerificationError):
            source.verify(Headers({"X-Signature": signature.swapcase()}), BODY, SECRET)

    def test_base64_source_rejects_hex_signature(self):
        source = HmacJsonSource.from_extra({**self.EXTRA, "signature_encoding": "base64"})

        with pytest.raises(WebhookVerificationError):
            source.verify(Headers({"X-Signature": compute_hex_signature()}), BODY, SECRET)

    def test_event_from_header(self):
        source = HmacJsonSource.from_extra({"signature_header": "X-Signature", "event_header": "X-Event"})

        parsed = parse_json(source, {}, **{"X-Event": "deployed"})

        assert (parsed.path, parsed.kinds) == (None, [("deployed",)])

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"event_field": "type"}, id="no-signature-header"),
            pytest.param({"signature_header": "X-Signature"}, id="no-event-source"),
            pytest.param(
                {"signature_header": "X-Signature", "event_field": "type", "event_header": "X-Event"},
                id="both-event-sources",
            ),
            pytest.param(
                {
                    "signature_header": "X-Signature",
                    "event_field": "type",
                    "signature_encoding": "hexadecimal",
                },
                id="unknown-encoding",
            ),
            pytest.param(
                {
                    "signature_header": "X-Signature",
                    "event_field": "type",
                    "delivery_id_field": "id",
                    "delivery_id_header": "X-Delivery",
                },
                id="both-delivery-id-sources",
            ),
        ],
    )
    def test_from_extra_rejects(self, extra):
        with pytest.raises(ValueError, match="hmac_json"):
            HmacJsonSource.from_extra(extra)

    @pytest.mark.parametrize(
        "payload",
        [
            {"data": {}},
            {"data": {"type": "deployed"}, "service": 1},
            {"data": {"type": "deployed"}, "id": {"value": "d-1"}},
            {"data": {"type": "deployed"}, "id": True},
        ],
        ids=["no-event", "non-string-path", "object-delivery-id", "boolean-delivery-id"],
    )
    def test_parse_rejects(self, payload):
        source = HmacJsonSource.from_extra({**self.EXTRA, "delivery_id_field": "id"})

        with pytest.raises(ValueError, match="event name|not a string"):
            parse_json(source, payload)

    @pytest.mark.parametrize(
        ("options", "payload", "headers", "expected"),
        [
            pytest.param({}, {"id": "d-1"}, {"X-Delivery": "h-1"}, None, id="not-configured"),
            pytest.param({"delivery_id_header": "X-Delivery"}, {}, {"X-Delivery": "h-1"}, "h-1", id="header"),
            pytest.param({"delivery_id_field": "meta.id"}, {"meta": {"id": "d-1"}}, {}, "d-1", id="field"),
            pytest.param({"delivery_id_field": "meta.id"}, {"meta": {"id": 42}}, {}, "42", id="numbered"),
            pytest.param({"delivery_id_field": "meta.id"}, {}, {}, None, id="missing"),
        ],
    )
    def test_delivery_id(self, options, payload, headers, expected):
        source = HmacJsonSource.from_extra({**self.EXTRA, **options})

        parsed = parse_json(source, {"data": {"type": "deployed"}, **payload}, **headers)

        assert parsed.delivery_id == expected


class TestTokenHeaderSource:
    EXTRA = {"token_header": "Authorization", "token_prefix": "Bearer ", "event_field": "event_type"}

    def test_from_extra_verify_and_parse(self):
        source = TokenHeaderSource.from_extra(self.EXTRA)
        headers = Headers({**JSON, "Authorization": f"Bearer {SECRET}"})

        source.verify(headers, BODY, SECRET)
        parsed = parse_json(source, {"event_type": "monitor_alert"})

        assert (parsed.path, parsed.kinds, parsed.delivery_id) == (None, [("monitor_alert",)], None)

    def test_delivery_id_from_header(self):
        source = TokenHeaderSource.from_extra({**self.EXTRA, "delivery_id_header": "X-Request-Id"})

        parsed = parse_json(source, {"event_type": "monitor_alert"}, **{"X-Request-Id": "r-1"})

        assert parsed.delivery_id == "r-1"

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="missing"),
            pytest.param({"Authorization": SECRET}, id="missing-prefix"),
            pytest.param({"Authorization": "Bearer wrong"}, id="wrong"),
            pytest.param({"Authorization": "Bearer \u00e9"}, id="non-ascii"),
        ],
    )
    def test_verify_rejects(self, headers):
        with pytest.raises(WebhookVerificationError):
            TokenHeaderSource.from_extra(self.EXTRA).verify(Headers(headers), BODY, SECRET)

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"event_field": "event_type"}, id="no-token-header"),
            pytest.param({"token_header": "X-Webhook-Secret"}, id="no-event-source"),
        ],
    )
    def test_from_extra_rejects(self, extra):
        with pytest.raises(ValueError, match="token_header"):
            TokenHeaderSource.from_extra(extra)


# Values of the wrong type, or empty, for any field a parser reads.
ODD_VALUES = [None, 7, 1.5, True, "", "text", [], ["text"], [None], {}, {"text": 1}]
# A delivery of each built-in source type carrying every field its parser reads, and those fields.
FULL_DELIVERIES = [
    *(
        pytest.param(
            GitHubSource(),
            {"X-GitHub-Event": event},
            {
                "action": "labeled",
                "label": {"name": "deploy-preview"},
                "ref": "refs/heads/main",
                "workflow": {"name": "CI"},
                "repository": {"full_name": "o/web"},
                "organization": {"login": "o"},
            },
            ["action", "label", "label.name", "ref", "workflow", "workflow.name", "repository"]
            + ["repository.full_name", "organization", "organization.login"],
            id=f"github-{event}",
        )
        for event in ("push", "pull_request", "workflow_run")
    ),
    *(
        pytest.param(
            GitLabSource(),
            {},
            {
                "object_kind": object_kind,
                "object_attributes": {"action": "update", "status": "success"},
                "ref": f"refs/{'tags' if object_kind == 'tag_push' else 'heads'}/main",
                "project": {"path_with_namespace": "g/sub/web"},
                "changes": {
                    "labels": {"previous": [{"title": "a"}], "current": [{"title": "a"}, {"title": "b"}]}
                },
            },
            [
                "object_kind",
                "object_attributes",
                "object_attributes.action",
                "object_attributes.status",
                "ref",
            ]
            + ["project", "project.path_with_namespace", "changes", "changes.labels"]
            + ["changes.labels.previous", "changes.labels.current", "changes.labels.current.1"]
            + ["changes.labels.current.1.title"],
            id=f"gitlab-{object_kind}",
        )
        for object_kind in ("push", "tag_push", "merge_request")
    ),
    *(
        pytest.param(
            JiraSource(),
            {},
            {
                "webhookEvent": f"jira:{event}",
                "issue": {"fields": {"project": {"key": "OPS"}, "issuetype": {"name": "Bug"}}},
                "project": {"key": "OPS"},
                "changelog": {
                    "items": [
                        {"field": "status", "toString": "Done"},
                        {"field": "labels", "fromString": "a", "toString": "a b"},
                    ]
                },
            },
            ["webhookEvent", "issue", "issue.fields", "issue.fields.project", "issue.fields.project.key"]
            + ["issue.fields.issuetype", "issue.fields.issuetype.name", "project", "project.key", "changelog"]
            + [
                "changelog.items",
                "changelog.items.0",
                "changelog.items.0.field",
                "changelog.items.0.toString",
            ]
            + ["changelog.items.1.fromString", "changelog.items.1.toString"],
            id=f"jira-{event}",
        )
        for event in ("issue_created", "issue_updated")
    ),
]


def replace_field(payload: dict, dotted_path: str, value) -> dict:
    payload = json.loads(json.dumps(payload))
    *parents, last = dotted_path.split(".")
    target = payload
    for key in parents:
        target = target[int(key)] if isinstance(target, list) else target[key]
    if isinstance(target, list):
        target[int(last)] = value
    else:
        target[last] = value
    return payload


@pytest.mark.parametrize(("source", "headers", "payload", "fields"), FULL_DELIVERIES)
def test_signed_delivery_of_any_shape_is_named_or_refused(source, headers, payload, fields):
    # A signature proves who sent a delivery, not its shape. Anything but ValueError (a 400) would be a
    # 500, which services retry and GitLab answers by disabling the webhook for a while.
    for field in fields:
        for value in ODD_VALUES:
            try:
                parsed = parse_json(source, replace_field(payload, field, value), **headers)
            except ValueError:
                continue
            assert parsed.path is None or isinstance(parsed.path, str), (field, value)
            assert all(isinstance(segment, str) for kind in parsed.kinds for segment in kind), (field, value)
            build_candidate_names("hooks", parsed)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_json_constants_json_does_not_allow_are_refused(constant):
    # Python's json accepts them, but they are not JSON, and MySQL refuses to store them.
    body = f'{{"event": "deployed", "value": {constant}}}'.encode()

    with pytest.raises(ValueError, match="which JSON does not allow"):
        load_json_object(Headers(JSON), body)
