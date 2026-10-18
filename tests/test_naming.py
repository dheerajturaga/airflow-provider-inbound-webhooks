# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from airflow_inbound_webhooks.naming import build_candidate_names, can_be_offered, can_be_offered_together
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


@pytest.mark.parametrize(
    ("name", "other", "delivery"),
    [
        pytest.param(
            "github/my-org/web/pull_request",
            "github/my-org/web/pull_request/labeled",
            build_event("my-org/web", ("pull_request", "labeled", "deploy-preview")),
            id="name-and-its-prefix",
        ),
        pytest.param(
            "github/my-org/web/push",
            "github/my-org/*/push",
            build_event("my-org/web", ("push", "main")),
            id="path-and-its-wildcard",
        ),
        pytest.param(
            "github/my-org/web/push",
            "github/*/push",
            build_event("my-org/web", ("push", "main")),
            id="path-and-any-path",
        ),
        pytest.param(
            "github/my-org/*/push",
            "github/*/push",
            build_event("my-org/web", ("push", "main")),
            id="two-wildcards-of-one-path",
        ),
        pytest.param(
            "github/my-org/web/push",
            "github/*/push/main",
            build_event("my-org/web", ("push", "main")),
            id="wildcard-and-prefix-at-once",
        ),
        pytest.param(
            "gitlab/platform/backend/*/pipeline",
            "gitlab/platform/*/pipeline/success",
            build_event("platform/backend/api", ("pipeline", "success")),
            id="deeper-wildcards",
        ),
        pytest.param(
            "github/my-org/*/organization",
            "github/*/organization/member_added",
            build_event("my-org/*", ("organization", "member_added")),
            id="organization-event",
        ),
        pytest.param(
            "github/installation",
            "github/installation/created",
            build_event(None, ("installation", "created")),
            id="without-a-path",
        ),
    ],
)
def test_names_one_delivery_offers_together(name, other, delivery):
    assert {name, other} <= set(build_candidate_names(name.partition("/")[0], delivery))
    assert can_be_offered_together(name, other)
    assert can_be_offered_together(other, name)


@pytest.mark.parametrize(
    ("name", "other"),
    [
        pytest.param(
            "github/my-org/web/pull_request/opened", "github/my-org/web/pull_request/reopened", id="actions"
        ),
        pytest.param("github/my-org/web/push/main", "github/*/push/develop", id="branches"),
        pytest.param("github/my-org/web/push", "github/my-org/api/push", id="repositories"),
        pytest.param("github/my-org/*/push", "github/other-org/*/push", id="organizations"),
        pytest.param("github/my-org/web/push", "github/my-org/web/*/push", id="path-and-paths-below-it"),
        pytest.param("github/*/push", "github/push", id="with-and-without-a-path"),
        pytest.param("github/my-org/web/push", "gitlab/my-org/web/push", id="webhooks"),
        # An event named like another, read after a wildcard as if it were a path segment.
        pytest.param("github/*/push/release", "github/*/release", id="branch-named-like-an-event"),
        pytest.param(
            "github/my-org/*/push/release", "github/my-org/*/release/published", id="in-an-organization"
        ),
        pytest.param("github/*/pull_request/labeled/release", "github/*/release", id="label-named-so"),
        pytest.param(
            "github/*/workflow_run/completed/release", "github/*/release/published", id="workflow-named-so"
        ),
        pytest.param("gitlab/*/push/release", "gitlab/*/release", id="gitlab-branch-named-so"),
        pytest.param(
            "gitlab/platform/*/push/release", "gitlab/platform/*/release/create", id="in-a-gitlab-group"
        ),
        # A Jira update that changes the status and adds the label offers both, through two kinds.
        pytest.param(
            "jira/OPS/issue_updated/status/Done",
            "jira/OPS/issue_updated/labeled/hotfix",
            id="different-kinds-not-detected",
        ),
    ],
)
def test_names_no_delivery_offers_together_through_one_kind(name, other):
    assert not can_be_offered_together(name, other)
    assert not can_be_offered_together(other, name)


def test_segment_holding_a_slash_is_compared_as_two():
    # GitHub offers these for two labels, never for one delivery, but names alone cannot tell.
    assert can_be_offered_together(
        "github/my-org/web/pull_request/labeled/kind", "github/my-org/web/pull_request/labeled/kind/bug"
    )


@pytest.mark.parametrize(
    ("name", "other", "delivery"),
    [
        pytest.param(
            "github/my-org/*/release",
            "github/my-org/web/push/release",
            build_event("my-org/web/push", ("release",)),
            id="path-of-three-segments",
        ),
        pytest.param(
            "github/*/release",
            "github/my-org/release/push",
            build_event("my-org", ("release", "push")),
            id="path-of-one-segment",
        ),
    ],
)
def test_how_many_segments_a_path_has_is_not_known(name, other, delivery):
    # GitHub's paths have two segments, so no GitHub delivery offers both, but a delivery to a webhook
    # of another source type can: names alone cannot tell.
    assert {name, other} <= set(build_candidate_names("github", delivery))
    assert can_be_offered_together(name, other)
