# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from airflow_inbound_webhooks.naming import build_candidate_names
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
                "github/my-org/web/pull_request/labeled",
                "github/my-org/*/pull_request/labeled",
                "github/my-org/web/pull_request/labeled/deploy-preview",
                "github/my-org/*/pull_request/labeled/deploy-preview",
            ],
            id="repo-and-org-scope",
        ),
        pytest.param(
            build_event("my-org/*", ("organization", "member_added")),
            ["github/my-org/*/organization", "github/my-org/*/organization/member_added"],
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
                "github/my-org/web/merge_request/update",
                "github/my-org/*/merge_request/update",
                "github/my-org/web/merge_request/labeled",
                "github/my-org/*/merge_request/labeled",
                "github/my-org/web/merge_request/labeled/a",
                "github/my-org/*/merge_request/labeled/a",
            ],
            id="several-kinds-share-prefixes",
        ),
        pytest.param(
            build_event("o/web", ("pull_request", "labeled", "\U0001f680 Café")),
            [
                "github/o/web/pull_request",
                "github/o/*/pull_request",
                "github/o/web/pull_request/labeled",
                "github/o/*/pull_request/labeled",
                "github/o/web/pull_request/labeled/Cafe",
                "github/o/*/pull_request/labeled/Cafe",
            ],
            id="non-ascii-folded",
        ),
        pytest.param(
            build_event("o/web", ("pull_request", "labeled", "\U0001f680")),
            [
                "github/o/web/pull_request",
                "github/o/*/pull_request",
                "github/o/web/pull_request/labeled",
                "github/o/*/pull_request/labeled",
            ],
            id="segment-empty-after-folding",
        ),
    ],
)
def test_build_candidate_names(event, expected):
    assert build_candidate_names("github", event) == expected
