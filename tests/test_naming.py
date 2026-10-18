# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from airflow_inbound_webhooks.naming import build_candidate_names, can_be_offered
from airflow_inbound_webhooks.sources import WebhookEvent


def build_event(path, *kinds) -> WebhookEvent:
    return WebhookEvent(path=path, kinds=list(kinds), delivery_id=None, payload={})


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        pytest.param(
            build_event("my-org/web", ("pull_request", "labeled", "deploy-preview")),
            [
                "github/my-org/web/pull_request",
                "github/my-org/*/pull_request",
                "github/*/pull_request",
                "github/my-org/web/pull_request/labeled",
                "github/my-org/*/pull_request/labeled",
                "github/*/pull_request/labeled",
                "github/my-org/web/pull_request/labeled/deploy-preview",
                "github/my-org/*/pull_request/labeled/deploy-preview",
                "github/*/pull_request/labeled/deploy-preview",
            ],
            id="repo-org-and-any-scope",
        ),
        pytest.param(
            build_event("platform/backend/api", ("pipeline", "success")),
            [
                "github/platform/backend/api/pipeline",
                "github/platform/backend/*/pipeline",
                "github/platform/*/pipeline",
                "github/*/pipeline",
                "github/platform/backend/api/pipeline/success",
                "github/platform/backend/*/pipeline/success",
                "github/platform/*/pipeline/success",
                "github/*/pipeline/success",
            ],
            id="wildcard-at-every-level",
        ),
        pytest.param(
            build_event("my-org/*", ("organization", "member_added")),
            [
                "github/my-org/*/organization",
                "github/*/organization",
                "github/my-org/*/organization/member_added",
                "github/*/organization/member_added",
            ],
            id="org-level-event",
        ),
        pytest.param(
            build_event(None, ("deployed",)),
            ["github/deployed"],
            id="no-path",
        ),
        pytest.param(
            build_event("billing", ("deployed",)),
            ["github/billing/deployed", "github/*/deployed"],
            id="single-segment-path",
        ),
        pytest.param(
            build_event("my-org/web", ("merge_request", "update"), ("merge_request", "labeled", "a")),
            [
                "github/my-org/web/merge_request",
                "github/my-org/*/merge_request",
                "github/*/merge_request",
                "github/my-org/web/merge_request/update",
                "github/my-org/*/merge_request/update",
                "github/*/merge_request/update",
                "github/my-org/web/merge_request/labeled",
                "github/my-org/*/merge_request/labeled",
                "github/*/merge_request/labeled",
                "github/my-org/web/merge_request/labeled/a",
                "github/my-org/*/merge_request/labeled/a",
                "github/*/merge_request/labeled/a",
            ],
            id="several-kinds-share-prefixes",
        ),
        pytest.param(
            build_event("o/web", ("pull_request", "labeled", "\U0001f680 Café")),
            [
                "github/o/web/pull_request",
                "github/o/*/pull_request",
                "github/*/pull_request",
                "github/o/web/pull_request/labeled",
                "github/o/*/pull_request/labeled",
                "github/*/pull_request/labeled",
                "github/o/web/pull_request/labeled/Cafe",
                "github/o/*/pull_request/labeled/Cafe",
                "github/*/pull_request/labeled/Cafe",
            ],
            id="non-ascii-folded",
        ),
        pytest.param(
            build_event("o/web", ("pull_request", "labeled", "\U0001f680")),
            [
                "github/o/web/pull_request",
                "github/o/*/pull_request",
                "github/*/pull_request",
                "github/o/web/pull_request/labeled",
                "github/o/*/pull_request/labeled",
                "github/*/pull_request/labeled",
            ],
            id="segment-empty-after-folding",
        ),
        pytest.param(
            build_event("\U0001f680", ("deployed",)),
            ["github/deployed"],
            id="path-empty-after-folding",
        ),
        pytest.param(
            build_event("ops/\U0001f680/api/", ("deployed",)),
            ["github/ops/api/deployed", "github/ops/*/deployed", "github/*/deployed"],
            id="empty-path-segments-dropped",
        ),
        pytest.param(
            build_event("o/web", ("pull_request", "labeled", "/kind//bug/")),
            [
                "github/o/web/pull_request",
                "github/o/*/pull_request",
                "github/*/pull_request",
                "github/o/web/pull_request/labeled",
                "github/o/*/pull_request/labeled",
                "github/*/pull_request/labeled",
                "github/o/web/pull_request/labeled/kind/bug",
                "github/o/*/pull_request/labeled/kind/bug",
                "github/*/pull_request/labeled/kind/bug",
            ],
            id="empty-parts-of-a-segment-dropped",
        ),
        pytest.param(
            build_event("o/web", ("push", "/")),
            ["github/o/web/push", "github/o/*/push", "github/*/push"],
            id="segment-of-slashes-ends-kind",
        ),
    ],
)
def test_build_candidate_names(event, expected):
    names = build_candidate_names("github", event)

    assert names == expected
    assert all(can_be_offered(name) for name in names)


@pytest.mark.parametrize(
    "name",
    [
        "github/my-org/web/pull_request/labeled/deploy-preview",
        "github/*/push",
        "huggingface/update",
        # Spaces next to a slash inside a segment, as in a Jira status, are kept.
        "jira/OPS/issue_updated/status/In Review / QA",
        # An hmac_json service may name an event "*", which is only a literal.
        "deploybot/*",
    ],
)
def test_name_a_delivery_can_offer(name):
    assert can_be_offered(name)


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("github", id="no-kind"),
        pytest.param("GitHub/push", id="webhook-name-not-served"),
        pytest.param("-github/push", id="webhook-name-starting-with-a-dash"),
        pytest.param("github/push/", id="trailing-slash"),
        pytest.param("github//push", id="empty-part"),
        pytest.param("github/ push", id="space-starting-a-segment"),
        pytest.param("github/push ", id="space-ending-a-segment"),
        pytest.param("github/labeled/Café", id="not-folded-to-ascii"),
    ],
)
def test_name_no_delivery_can_offer(name):
    assert not can_be_offered(name)
