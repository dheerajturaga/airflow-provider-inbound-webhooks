# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest
from starlette.datastructures import Headers

from airflow_inbound_webhooks.sources import (
    GitHubSource,
    GitLabSource,
    HmacJsonSource,
    TokenHeaderSource,
    WebhookVerificationError,
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

        parsed = parse_json(GitLabSource(), payload, **{"X-Gitlab-Event-UUID": "u-1"})

        assert (parsed.path, parsed.kinds, parsed.delivery_id) == ("group/sub/web", expected_kinds, "u-1")

    def test_parse_rejects_missing_object_kind(self):
        with pytest.raises(ValueError, match="object_kind"):
            parse_json(GitLabSource(), {})


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
        ],
    )
    def test_from_extra_rejects(self, extra):
        with pytest.raises(ValueError, match="hmac_json"):
            HmacJsonSource.from_extra(extra)

    @pytest.mark.parametrize(
        "payload",
        [{"data": {}}, {"data": {"type": "deployed"}, "service": 1}],
        ids=["no-event", "non-string-path"],
    )
    def test_parse_rejects(self, payload):
        with pytest.raises(ValueError, match="event name|not a string"):
            parse_json(HmacJsonSource.from_extra(self.EXTRA), payload)


class TestTokenHeaderSource:
    EXTRA = {"token_header": "Authorization", "token_prefix": "Bearer ", "event_field": "event_type"}

    def test_from_extra_verify_and_parse(self):
        source = TokenHeaderSource.from_extra(self.EXTRA)
        headers = Headers({**JSON, "Authorization": f"Bearer {SECRET}"})

        source.verify(headers, BODY, SECRET)
        parsed = parse_json(source, {"event_type": "monitor_alert"})

        assert (parsed.path, parsed.kinds) == (None, [("monitor_alert",)])

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
