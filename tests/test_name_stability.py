# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Hold the names deliveries offer in place, since Dags subscribe to them.

A Dag whose name stops matching does not fail; it just stops running. So ``candidate_names.json``
records, in order, the candidate names the endpoint offers for each delivery below and for each of
``FULL_DELIVERIES`` in ``test_sources.py``, and each name the README and the example Dags use must be
one that a delivery offers.

Changing a name that existing deliveries produce is a breaking change, which CHANGELOG.md lists under
"Changed event names", with the old and new names; adding a name is not. Once a change to the names is
deliberate, record them again, and review the changes to ``candidate_names.json`` before committing
them::

    INBOUND_WEBHOOKS_RECORD_NAMES=1 .venv/bin/pytest tests/test_name_stability.py
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, NamedTuple

import pytest
from test_sources import FULL_DELIVERIES, parse_json

from airflow_inbound_webhooks.naming import build_candidate_names
from airflow_inbound_webhooks.sources import (
    SOURCE_TYPES,
    GitHubSource,
    GitLabSource,
    JiraSource,
    WebhookSource,
)

ROOT = Path(__file__).parents[1]
CANDIDATE_NAMES = Path(__file__).with_name("candidate_names.json")
# When set, the names deliveries offer now replace those in CANDIDATE_NAMES.
RECORD_NAMES = "INBOUND_WEBHOOKS_RECORD_NAMES"
BREAKING_CHANGE = (
    "Dags subscribe to these names, so changing a name that existing deliveries produce is a breaking "
    'change: the changelog lists it under "Changed event names", with the old and new names (see the '
    'introduction of CHANGELOG.md, and "Event names" in the README). Adding a name is not. Once the '
    "change is deliberate, record the names again, as the docstring of tests/test_name_stability.py says."
)


class Delivery(NamedTuple):
    """A delivery to ``POST /webhooks/<webhook>``, whose connection configures ``source``."""

    webhook: str
    source: WebhookSource
    headers: dict[str, str]
    payload: dict[str, Any]


def offer_names(delivery: Delivery) -> list[str]:
    """Return the candidate names the endpoint offers for a delivery, as ``record_delivery`` builds them."""
    return build_candidate_names(
        delivery.webhook, parse_json(delivery.source, delivery.payload, **delivery.headers)
    )


def configure(extra: dict[str, Any]) -> WebhookSource:
    """Return the source a webhook connection with this extra configures."""
    return SOURCE_TYPES[extra["source_type"]].from_extra(extra)


def github(event: str, payload: dict[str, Any], *, webhook: str = "github") -> Delivery:
    return Delivery(webhook, GitHubSource(), {"X-GitHub-Event": event}, payload)


SOURCE_TYPE_NAMES = {source_class: name for name, source_class in SOURCE_TYPES.items()}
# The repository my-org/web, as GitHub names it in deliveries about it.
MY_ORG_WEB = {"repository": {"full_name": "my-org/web"}, "organization": {"login": "my-org"}}
LABELED = {"action": "labeled", "number": 7, "label": {"name": "deploy-preview"}, **MY_ORG_WEB}
DELIVERIES: dict[str, Delivery] = {
    # Each to the webhook named after its source type, whose connection needs nothing but a secret.
    **{
        param.id: Delivery(SOURCE_TYPE_NAMES[type(param.values[0])], *param.values[:3])
        for param in FULL_DELIVERIES
    },
    # GitHub's, trimmed to the fields their names come from, and a few that their names leave out.
    "github-ping": github("ping", {"zen": "Design for failure.", "hook_id": 1, **MY_ORG_WEB}),
    "github-ping-organization-webhook": github(
        "ping", {"zen": "Design for failure.", "hook_id": 2, "organization": {"login": "my-org"}}
    ),
    "github-pull_request-opened": github("pull_request", {"action": "opened", "number": 7, **MY_ORG_WEB}),
    "github-pull_request-closed-merged": github(
        "pull_request", {"action": "closed", "number": 7, "pull_request": {"merged": True}, **MY_ORG_WEB}
    ),
    "github-pull_request-labeled": github("pull_request", LABELED),
    "github-pull_request-unlabeled": github("pull_request", {**LABELED, "action": "unlabeled"}),
    "github-pull_request-labeled-with-slash": github(
        "pull_request", {**LABELED, "label": {"name": "kind/bug"}}
    ),
    "github-pull_request-labeled-non-ascii": github(
        "pull_request", {**LABELED, "label": {"name": "\U0001f680 deploy"}}
    ),
    "github-pull_request-labeled-emoji-only": github(
        "pull_request", {**LABELED, "label": {"name": "\U0001f680"}}
    ),
    "github-pull_request-labeled-personal-repository": github(
        "pull_request",
        {
            "action": "labeled",
            "number": 1,
            "label": {"name": "deploy-preview"},
            "repository": {"full_name": "dheerajturaga/inbound-webhooks-demo"},
        },
    ),
    "github-pull_request_review-submitted": github(
        "pull_request_review", {"action": "submitted", "review": {"state": "approved"}, **MY_ORG_WEB}
    ),
    "github-issues-labeled-with-spaces": github(
        "issues",
        {"action": "labeled", "issue": {"number": 8}, "label": {"name": "good first issue"}, **MY_ORG_WEB},
    ),
    "github-push-branch": github("push", {"ref": "refs/heads/main", **MY_ORG_WEB}),
    "github-push-branch-with-slash": github("push", {"ref": "refs/heads/feature/login", **MY_ORG_WEB}),
    # GitHub reports deleting a branch as a push to it.
    "github-push-branch-deleted": github(
        "push", {"ref": "refs/heads/feature/login", "deleted": True, **MY_ORG_WEB}
    ),
    "github-push-tag": github("push", {"ref": "refs/tags/v1.2.0", **MY_ORG_WEB}),
    "github-push-mixed-case": github(
        "push",
        {
            "ref": "refs/heads/Release-1.2",
            "repository": {"full_name": "My-Org/Web-UI"},
            "organization": {"login": "My-Org"},
        },
    ),
    # Only a push has its branch as its detail.
    "github-create-branch": github("create", {"ref": "feature/login", "ref_type": "branch", **MY_ORG_WEB}),
    "github-delete-tag": github("delete", {"ref": "v1.2.0", "ref_type": "tag", **MY_ORG_WEB}),
    "github-release-published": github(
        "release", {"action": "published", "release": {"tag_name": "v1.2.0"}, **MY_ORG_WEB}
    ),
    "github-workflow_run-completed": github(
        "workflow_run",
        {
            "action": "completed",
            "workflow_run": {"id": 30433642, "conclusion": "failure"},
            "workflow": {"name": "CI"},
            **MY_ORG_WEB,
        },
    ),
    "github-workflow_job-completed": github(
        "workflow_job", {"action": "completed", "workflow_job": {"name": "build"}, **MY_ORG_WEB}
    ),
    "github-deployment_status-created": github(
        "deployment_status",
        {
            "action": "created",
            "deployment_status": {"state": "success", "environment": "production"},
            **MY_ORG_WEB,
        },
    ),
    "github-organization-member_added": github(
        "organization", {"action": "member_added", "organization": {"login": "my-org"}}
    ),
    "github-repository-created": github(
        "repository",
        {"action": "created", "repository": {"full_name": "my-org/api"}, "organization": {"login": "my-org"}},
    ),
    # Neither a repository nor an organization: a GitHub App installed on a personal account.
    "github-installation-created": github(
        "installation", {"action": "created", "installation": {"id": 1, "account": {"login": "octocat"}}}
    ),
    # A second GitHub instance, as the README names it.
    "ghes-pull_request-labeled": github("pull_request", LABELED, webhook="ghes"),
    # The other deliveries the README and the example Dags name.
    "gitlab-pipeline-success": Delivery(
        "gitlab",
        GitLabSource(),
        {},
        {
            "object_kind": "pipeline",
            "object_attributes": {"status": "success"},
            "project": {"path_with_namespace": "platform/api"},
        },
    ),
    "gitlab-push-subgroup": Delivery(
        "gitlab",
        GitLabSource(),
        {},
        {
            "object_kind": "push",
            "ref": "refs/heads/main",
            "project": {"path_with_namespace": "platform/backend/api"},
        },
    ),
    "jira-issue_updated-status-and-label": Delivery(
        "jira",
        JiraSource(),
        {},
        {
            "webhookEvent": "jira:issue_updated",
            "issue": {"key": "OPS-42", "fields": {"project": {"key": "OPS"}}},
            "changelog": {
                "items": [
                    {"field": "status", "fromString": "In Progress", "toString": "Ready for Deploy"},
                    {"field": "labels", "fromString": "backend", "toString": "backend hotfix"},
                ]
            },
        },
    ),
    "jira-issue_created-bug": Delivery(
        "jira",
        JiraSource(),
        {},
        {
            "webhookEvent": "jira:issue_created",
            "issue": {"key": "OPS-43", "fields": {"project": {"key": "OPS"}, "issuetype": {"name": "Bug"}}},
        },
    ),
    "jira-sprint_started": Delivery(
        "jira", JiraSource(), {}, {"webhookEvent": "sprint_started", "sprint": {"id": 7, "originBoardId": 3}}
    ),
    "demo-deployed": Delivery(
        "demo",
        configure(
            {
                "source_type": "hmac_json",
                "signature_header": "X-Signature",
                "event_field": "event",
                "path_field": "service",
            }
        ),
        {},
        {"event": "deployed", "service": "billing"},
    ),
    "deploybot-deployed": Delivery(
        "deploybot",
        configure(
            {
                "source_type": "hmac_json",
                "signature_header": "X-Signature",
                "signature_prefix": "sha256=",
                "event_field": "type",
                "path_field": "service",
            }
        ),
        {},
        {"type": "deployed", "service": "billing"},
    ),
    # A token_header source, configured as its docstring is.
    "alerts-monitor_alert": Delivery(
        "alerts",
        configure(
            {
                "source_type": "token_header",
                "token_header": "Authorization",
                "token_prefix": "Bearer ",
                "event_field": "event_type",
            }
        ),
        {},
        {"event_type": "monitor_alert"},
    ),
}
WEBHOOKS = sorted({delivery.webhook for delivery in DELIVERIES.values()})
README = ROOT / "README.md"
EXAMPLE_DAGS = sorted((ROOT / "examples" / "dags").glob("*.py"))
ASSET_NAME = re.compile(r'Asset\("([^"]+)"\)')
# A name in code in the README's text. Placeholders, such as <group>, are not names.
INLINE_NAME = re.compile(rf"`((?:{'|'.join(map(re.escape, WEBHOOKS))})/[^`<]*)`")


@pytest.fixture(scope="module")
def offered_names() -> dict[str, list[str]]:
    return {delivery_id: offer_names(delivery) for delivery_id, delivery in DELIVERIES.items()}


@pytest.fixture(scope="module")
def recorded_names(offered_names) -> dict[str, list[str]]:
    if os.environ.get(RECORD_NAMES):
        CANDIDATE_NAMES.write_text(json.dumps(offered_names, indent=2) + "\n")
    return json.loads(CANDIDATE_NAMES.read_text())


@pytest.mark.parametrize("delivery_id", list(DELIVERIES))
def test_delivery_offers_the_names_recorded_for_it(recorded_names, delivery_id):
    assert delivery_id in recorded_names, (
        f"{delivery_id} has no names recorded: record them, as this module's docstring says"
    )
    recorded, names = recorded_names[delivery_id], offer_names(DELIVERIES[delivery_id])

    assert names == recorded, (
        f"{delivery_id} no longer offers the names recorded for it in tests/candidate_names.json.\n"
        f"No longer offered: {[name for name in recorded if name not in names]}\n"
        f"Newly offered: {[name for name in names if name not in recorded]}\n{BREAKING_CHANGE}"
    )


def test_names_are_only_recorded_for_deliveries_that_are_tested(recorded_names):
    # Dropping a delivery drops the check of its names, so it must show in the recorded names too.
    assert sorted(recorded_names.keys() - DELIVERIES.keys()) == [], (
        "Record the names again, as this module's docstring says"
    )


def find_name_lists(markdown: str) -> list[list[str]]:
    """
    Return each list of the names that the README shows one delivery offering.

    These are its ``text`` blocks of names, a name at the start of each line, and the quick start's answer.
    """
    lists = [
        [re.split(r" {2,}", line.strip())[0] for line in block.strip().splitlines()]
        for block in re.findall(r"```text\n(.*?)```", markdown, re.DOTALL)
    ]
    answer = re.search(r'^ *(\{"status":.*\})$', markdown, re.MULTILINE)
    assert answer is not None
    names_only = [names for names in lists if names[0].split("/")[0] in WEBHOOKS]
    return [*names_only, json.loads(answer.group(1))["candidates"]]


def test_names_the_readme_shows_a_delivery_offering_are_all_it_offers(offered_names):
    name_lists = find_name_lists(README.read_text())

    # The quick start's names and its answer, a label added to a pull request, and a Jira transition.
    assert len(name_lists) >= 4
    unmatched = [names for names in name_lists if names not in offered_names.values()]
    assert unmatched == [], (
        f"The README shows a delivery offering these names, in this order, and none does: {unmatched}. "
        f"Either the README is wrong, or deliveries no longer offer the names it shows. {BREAKING_CHANGE}"
    )


def find_documented_names() -> list[tuple[str, str]]:
    """Return each name the README and the example Dags schedule on or mention, with its file's name."""
    found = [
        (path.name, name) for path in [README, *EXAMPLE_DAGS] for name in ASSET_NAME.findall(path.read_text())
    ]
    return found + [(README.name, name) for name in INLINE_NAME.findall(README.read_text())]


def test_names_the_readme_and_the_example_dags_use_are_offered(offered_names):
    offered = {name for names in offered_names.values() for name in names}
    documented = find_documented_names()

    # Every file uses names where this test looks for them.
    assert {path for path, _ in documented} == {README.name, *(path.name for path in EXAMPLE_DAGS)}
    # "gitlab/platform/*/..." stands for the names that start with "gitlab/platform/*/".
    missing = [
        (path, name)
        for path, name in documented
        if name not in offered
        and not (
            name.endswith("/...")
            and any(offered_name.startswith(name.removesuffix("...")) for offered_name in offered)
        )
    ]
    assert missing == [], (
        f"No delivery in tests/test_name_stability.py offers these names: {missing}. Add the delivery "
        f"that the README or the example Dag means; or, if deliveries offered them before: {BREAKING_CHANGE}"
    )
