# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Hold the names deliveries offer in place, since Dags subscribe to them.

A Dag whose name stops matching does not fail; it just stops running. So each delivery below is posted
to the endpoint, and ``candidate_names.json`` records, in order, the candidate names its delivery
record keeps. Each name the README and the example Dags use must be one that a delivery offers.

Changing a name that existing deliveries produce is a breaking change, which CHANGELOG.md lists under
"Changed event names", with the old and new names; adding a name is not. Once a change to the names is
deliberate, record them again, and review the changes to ``candidate_names.json`` before committing
them::

    INBOUND_WEBHOOKS_RECORD_NAMES=1 .venv/bin/pytest tests/test_name_stability.py
"""

from __future__ import annotations

import ast
import hashlib
import hmac
import json
import os
import re
import runpy
import textwrap
from http import HTTPStatus
from pathlib import Path
from typing import Any, NamedTuple
from unittest import mock

import pytest
from airflow.sdk import Asset
from airflow.utils.session import create_session
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from airflow_inbound_webhooks import WebhookTopic
from airflow_inbound_webhooks.app import WEBHOOK_NAME_PATTERN, build_app
from airflow_inbound_webhooks.models import delivery_table

ROOT = Path(__file__).parents[1]
CANDIDATE_NAMES = Path(__file__).with_name("candidate_names.json")
# Set to 1, the names deliveries offer now replace those in CANDIDATE_NAMES.
RECORD_NAMES = "INBOUND_WEBHOOKS_RECORD_NAMES"
RECORD_AGAIN = "Once the change is deliberate, record the names again, as tests/test_name_stability.py says."
BREAKING_CHANGE = (
    "Dags subscribe to these names, so changing a name that existing deliveries produce is a breaking "
    'change: the changelog lists it under "Changed event names", with the old and new names (see the '
    f'introduction of CHANGELOG.md, and "Event names" in the README). {RECORD_AGAIN}'
)
SECRET = "s3cret"
# Each webhook's connection extra; github, gitlab and jira need nothing but a secret.
CONNECTIONS: dict[str, dict[str, str]] = {
    "github": {},
    # A second GitHub instance, as the README names one.
    "ghes": {"source_type": "github"},
    "gitlab": {},
    "jira": {},
    # As the README's quick start creates it.
    "demo": {
        "source_type": "hmac_json",
        "signature_header": "X-Signature",
        "event_field": "event",
        "path_field": "service",
    },
    # As the README's "Connect any other service" creates it.
    "deploybot": {
        "source_type": "hmac_json",
        "signature_header": "X-Signature",
        "signature_prefix": "sha256=",
        "event_field": "type",
        "path_field": "service",
    },
    # As the token_header source's docstring configures it.
    "alerts": {
        "source_type": "token_header",
        "token_header": "Authorization",
        "token_prefix": "Bearer ",
        "event_field": "event_type",
    },
}


class Delivery(NamedTuple):
    """A delivery to ``POST /webhooks/<webhook>`` as its sender sends it, without the headers that sign it."""

    webhook: str
    headers: dict[str, str]
    payload: dict[str, Any]


def sign(webhook: str, body: bytes) -> dict[str, str]:
    """Return the headers with which the sender authenticates a delivery to ``webhook``."""
    extra = CONNECTIONS[webhook]
    source_type = extra.get("source_type", webhook)
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    if source_type == "github":
        return {"X-Hub-Signature-256": f"sha256={signature}"}
    if source_type == "jira":
        return {"X-Hub-Signature": f"sha256={signature}"}
    if source_type == "gitlab":
        return {"X-Gitlab-Token": SECRET}
    if source_type == "hmac_json":
        return {extra["signature_header"]: extra.get("signature_prefix", "") + signature}
    if source_type == "token_header":
        return {extra["token_header"]: extra.get("token_prefix", "") + SECRET}
    raise ValueError(f"sign() does not know how a sender authenticates {source_type} deliveries")


def offer_names(client: TestClient, delivery: Delivery) -> list[str]:
    """Post a delivery to the endpoint, and return the candidate names its delivery record keeps."""
    with create_session() as session:
        session.execute(delete(delivery_table))
    body = json.dumps(delivery.payload).encode()
    headers = {"content-type": "application/json", **delivery.headers, **sign(delivery.webhook, body)}

    response = client.post(f"/{delivery.webhook}", content=body, headers=headers)

    assert response.status_code == HTTPStatus.ACCEPTED, f"{delivery} was answered: {response.text}"
    # The record keeps them, whatever the answer to the sender holds.
    with create_session() as session:
        return session.scalars(select(delivery_table.c.candidates)).one()


def github(event: str, payload: dict[str, Any], *, webhook: str = "github") -> Delivery:
    return Delivery(webhook, {"X-GitHub-Event": event}, payload)


def gitlab(event: str, payload: dict[str, Any]) -> Delivery:
    return Delivery("gitlab", {"X-Gitlab-Event": event}, payload)


def jira(payload: dict[str, Any]) -> Delivery:
    return Delivery("jira", {}, payload)


# The repository my-org/web, as GitHub names it in deliveries about it.
MY_ORG_WEB = {"repository": {"full_name": "my-org/web"}, "organization": {"login": "my-org"}}
LABELED = {"action": "labeled", "number": 7, "label": {"name": "deploy-preview"}, **MY_ORG_WEB}
OPS_42 = {"key": "OPS-42", "fields": {"project": {"key": "OPS"}, "issuetype": {"name": "Bug"}}}
# Deliveries as their senders send them, trimmed to the fields their names come from and a few that
# their names leave out.
DELIVERIES: dict[str, Delivery] = {
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
    # A branch named like an event, which the README uses to show the limits of the topic checks.
    "github-push-branch-named-release": github("push", {"ref": "refs/heads/release", **MY_ORG_WEB}),
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
    "ghes-pull_request-labeled": github("pull_request", LABELED, webhook="ghes"),
    "gitlab-push-subgroup": gitlab(
        "Push Hook",
        {
            "object_kind": "push",
            "event_name": "push",
            "ref": "refs/heads/main",
            "project": {"path_with_namespace": "platform/backend/api"},
        },
    ),
    "gitlab-tag_push-new-tag": gitlab(
        "Tag Push Hook",
        {
            "object_kind": "tag_push",
            "event_name": "tag_push",
            "before": "0000000000000000000000000000000000000000",
            "after": "82b3d5ae55f7080f1e6022629cdb57bfae7cccc7",
            "ref": "refs/tags/v1.2.0",
            "ref_protected": True,
            "project": {"path_with_namespace": "platform/api"},
        },
    ),
    # A label added: GitLab sends the labels before and after the change.
    "gitlab-merge_request-update-labeled": gitlab(
        "Merge Request Hook",
        {
            "object_kind": "merge_request",
            "event_type": "merge_request",
            "project": {"path_with_namespace": "platform/api"},
            "object_attributes": {"iid": 1, "action": "update", "state": "opened", "target_branch": "main"},
            "labels": [{"id": 206, "title": "API"}, {"id": 205, "title": "deploy-preview"}],
            "changes": {
                "labels": {
                    "previous": [{"id": 206, "title": "API"}],
                    "current": [{"id": 206, "title": "API"}, {"id": 205, "title": "deploy-preview"}],
                }
            },
        },
    ),
    "gitlab-pipeline-success": gitlab(
        "Pipeline Hook",
        {
            "object_kind": "pipeline",
            "object_attributes": {"id": 31, "ref": "main", "status": "success"},
            "project": {"path_with_namespace": "platform/api"},
        },
    ),
    "jira-issue_created-bug": jira({"webhookEvent": "jira:issue_created", "issue": OPS_42}),
    "jira-issue_updated-status-and-label": jira(
        {
            "webhookEvent": "jira:issue_updated",
            "issue": OPS_42,
            "changelog": {
                "items": [
                    {"field": "status", "fromString": "In Progress", "toString": "Ready for Deploy"},
                    {"field": "labels", "fromString": "backend", "toString": "backend hotfix"},
                ]
            },
        }
    ),
    "jira-issue_updated-custom-field": jira(
        {
            "webhookEvent": "jira:issue_updated",
            "issue": OPS_42,
            "changelog": {
                "items": [
                    {"field": "Story Points", "fieldtype": "custom", "fromString": None, "toString": "5"}
                ]
            },
        }
    ),
    "jira-comment_created": jira(
        {"webhookEvent": "comment_created", "issue": OPS_42, "comment": {"id": "10100", "body": "Looks good"}}
    ),
    "jira-sprint_started": jira({"webhookEvent": "sprint_started", "sprint": {"id": 7, "originBoardId": 3}}),
    "demo-deployed": Delivery("demo", {}, {"event": "deployed", "service": "billing"}),
    "deploybot-deployed": Delivery("deploybot", {}, {"type": "deployed", "service": "billing"}),
    "alerts-monitor_alert": Delivery("alerts", {}, {"event_type": "monitor_alert"}),
}
README = ROOT / "README.md"
EXAMPLE_DAGS = sorted((ROOT / "examples" / "dags").glob("*.py"))
# What the README's Python may build assets with, by the name it calls each one.
README_NAMESPACE: dict[str, Any] = {"Asset": Asset, "WebhookTopic": WebhookTopic}
# A line of a text block of names starts with a webhook's name and a slash.
NAME_LINE = re.compile(r"[a-z0-9][a-z0-9_-]*/")


@pytest.fixture(scope="module")
def offered_names() -> dict[str, list[str]]:
    """Post each delivery to its webhook, and return the names the endpoint records for each one."""
    connections = {
        f"AIRFLOW_CONN_WEBHOOK_{webhook.upper()}": json.dumps(
            {"conn_type": "inbound_webhook", "password": SECRET, "extra": extra}
        )
        for webhook, extra in CONNECTIONS.items()
    }
    config = {
        "AIRFLOW__INBOUND_WEBHOOKS__WEBHOOK_NAMES": ",".join(CONNECTIONS),
        # Read for each delivery, so that no connection stays cached for the tests that come later.
        "AIRFLOW__INBOUND_WEBHOOKS__CONNECTION_CACHE_TTL": "0",
    }
    client = TestClient(build_app())
    with mock.patch.dict(os.environ, {**connections, **config}):
        names = {delivery_id: offer_names(client, delivery) for delivery_id, delivery in DELIVERIES.items()}
    with create_session() as session:
        session.execute(delete(delivery_table))
    return names


@pytest.fixture(scope="module")
def recorded_names(offered_names) -> dict[str, list[str]]:
    if os.environ.get(RECORD_NAMES) == "1":
        CANDIDATE_NAMES.write_text(json.dumps(offered_names, indent=2) + "\n")
    return json.loads(CANDIDATE_NAMES.read_text())


@pytest.mark.parametrize("delivery_id", list(DELIVERIES))
def test_delivery_offers_the_names_recorded_for_it(offered_names, recorded_names, delivery_id):
    assert delivery_id in recorded_names, f"{delivery_id} has no names recorded. {RECORD_AGAIN}"
    recorded, names = recorded_names[delivery_id], offered_names[delivery_id]
    gone = [name for name in recorded if name not in names]
    new = [name for name in names if name not in recorded]
    if gone:
        verdict = BREAKING_CHANGE
    elif new:
        verdict = f"Adding names is not a breaking change. {RECORD_AGAIN}"
    else:
        verdict = f"It offers the same names in another order. {RECORD_AGAIN}"

    assert names == recorded, (
        f"{delivery_id} offers other names than tests/candidate_names.json records for it.\n"
        f"No longer offered: {gone}\nNewly offered: {new}\n{verdict}"
    )


def test_names_are_only_recorded_for_deliveries_that_are_tested(recorded_names):
    # Dropping a delivery drops the check of its names, so it must show in the recorded names too.
    assert sorted(recorded_names.keys() - DELIVERIES.keys()) == [], (
        "Record the names again, as this module's docstring says"
    )


def find_readme_webhooks(markdown: str) -> set[str]:
    """Return the webhooks the README sets up, by the connections it adds and the URLs it gives them."""
    # Commands rather than any webhook_<name>: the option webhook_names is not a connection.
    connections = re.findall(r"connections add webhook_(\S+)", markdown)
    urls = re.findall(r"/webhooks/([^\s/`'\"),]+)", markdown)
    # Not the routes that no webhook can be named after, such as /webhooks/_health, nor placeholders.
    return {webhook for webhook in connections + urls if WEBHOOK_NAME_PATTERN.fullmatch(webhook)}


def find_code_spans(markdown: str) -> list[str]:
    """Return the code spans in the README's text, as Markdown shows them: a line break is a space."""
    text = re.sub(r"```.*?```", "", markdown, flags=re.DOTALL)
    return [re.sub(r"\s*\n\s*", " ", span) for span in re.findall(r"`([^`]+)`", text)]


def is_call(source: str) -> bool:
    try:
        return isinstance(ast.parse(source, mode="eval").body, ast.Call)
    except SyntaxError:
        return False


def resolve(callee: ast.expr) -> Any:
    """Return what a call's callee, such as ``Asset`` or ``Asset.ref``, is in README_NAMESPACE, or None."""
    if isinstance(callee, ast.Name):
        return README_NAMESPACE.get(callee.id)
    if isinstance(callee, ast.Attribute) and (owner := resolve(callee.value)) is not None:
        return getattr(owner, callee.attr, None)
    return None


def build_names(source: str) -> list[str]:
    """Return the name of each asset that Python code builds with README_NAMESPACE, whatever its form."""
    names = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or (build := resolve(node.func)) is None:
            continue
        values = [*node.args, *(keyword.value for keyword in node.keywords)]
        # A placeholder, as in Asset.ref(name=...), stands for any name.
        if any(isinstance(value, ast.Constant) and value.value is Ellipsis for value in values):
            continue
        try:
            args = [ast.literal_eval(arg) for arg in node.args]
            kwargs = {keyword.arg: ast.literal_eval(keyword.value) for keyword in node.keywords}
        except ValueError:
            pytest.fail(f"The README builds an asset from more than literals: {ast.unparse(node)}")
        built = build(*args, **kwargs)
        # Asset.ref(uri=...) has no name, and webhook events are on assets whose URI is their name.
        names.append(getattr(built, "name", None) or built.uri)
    return names


def find_built_names(markdown: str) -> list[str]:
    """Return the names of the assets the README's Python builds: its python blocks, and calls in code."""
    blocks = re.findall(r"```python\n(.*?)^ *```", markdown, re.DOTALL | re.MULTILINE)
    sources = [textwrap.dedent(block) for block in blocks] + list(filter(is_call, find_code_spans(markdown)))
    return [name for source in sources for name in build_names(source)]


def is_misspelling(word: str, of: str) -> bool:
    """Whether ``word`` is ``of`` with one character added, left out or changed, or two adjacent swapped."""
    shorter, longer = sorted((word, of), key=len)
    if word == of or len(longer) > len(shorter) + 1:
        return False
    if len(longer) > len(shorter):
        return any(longer[:i] + longer[i + 1 :] == shorter for i in range(len(longer)))
    differ = [i for i, (a, b) in enumerate(zip(word, of)) if a != b]
    return len(differ) == 1 or (
        len(differ) == 2
        and differ[1] == differ[0] + 1
        and (word[differ[0]], word[differ[1]]) == (of[differ[1]], of[differ[0]])
    )


def find_name_spans(markdown: str, webhooks: set[str]) -> list[str]:
    """Return the code spans in the README's text that are names, or would be but for a misspelling."""
    names = []
    for span in find_code_spans(markdown):
        webhook, slash, _ = span.partition("/")
        # Placeholders, as in gitlab/<group>/*/..., are not names.
        if (
            slash
            and "<" not in span
            and any(webhook == known or is_misspelling(webhook, known) for known in webhooks)
        ):
            names.append(span)
    return names


def find_name_lists(markdown: str) -> list[list[str]]:
    """Return the names in each of the README's text blocks of names, one at the start of each line."""
    blocks = [
        [line.strip() for line in block.splitlines() if line.strip()]
        for block in re.findall(r"```text\n(.*?)```", markdown, re.DOTALL)
    ]
    # Two spaces or more end a name and start what it means, as names can hold single spaces.
    return [
        [re.split(r" {2,}", line)[0] for line in lines]
        for lines in blocks
        if lines and NAME_LINE.match(lines[0])
    ]


def find_example_names() -> list[tuple[str, str]]:
    """Return the name of each asset an example Dag file defines, however it is built, with the file's."""
    return [
        (path.name, value.name)
        for path in EXAMPLE_DAGS
        for value in runpy.run_path(str(path)).values()
        if isinstance(value, Asset)
    ]


def find_documented_names() -> list[tuple[str, str]]:
    """Return each name the README and the example Dags schedule on or mention, with its file's name."""
    markdown = README.read_text()
    webhooks = find_readme_webhooks(markdown) | {delivery.webhook for delivery in DELIVERIES.values()}
    in_readme = [
        *find_built_names(markdown),
        *find_name_spans(markdown, webhooks),
        # A block of one name shows that name, not all the names a delivery offers.
        *(names[0] for names in find_name_lists(markdown) if len(names) == 1),
    ]
    return [(README.name, name) for name in in_readme] + find_example_names()


def is_offered(name: str, offered: set[str]) -> bool:
    # "gitlab/platform/*/..." and "github/" stand for the names that start with them.
    if (prefix := name.removesuffix("...")).endswith("/"):
        return any(offered_name.startswith(prefix) for offered_name in offered)
    return name in offered


def test_webhooks_the_readme_sets_up_have_deliveries():
    webhooks = find_readme_webhooks(README.read_text())

    assert {"demo", "github"} <= webhooks, f"Found only these webhooks in the README: {sorted(webhooks)}"
    missing = sorted(webhooks - {delivery.webhook for delivery in DELIVERIES.values()})
    assert missing == [], (
        f"The README sets up webhooks that no delivery in tests/test_name_stability.py goes to: {missing}. "
        "Add deliveries of the events whose names it shows for them, with their connections."
    )


def test_names_the_readme_shows_a_delivery_offering_are_all_it_offers(offered_names):
    name_lists = [names for names in find_name_lists(README.read_text()) if len(names) > 1]

    # The quick start delivery's, a label added to a pull request's, and a Jira transition's.
    assert len(name_lists) >= 3, f"Found fewer lists of names in the README's text blocks: {name_lists}"
    unmatched = [names for names in name_lists if names not in offered_names.values()]
    assert unmatched == [], (
        "A text block of names in the README shows all the names one delivery offers, in the order it "
        f"offers them, and no delivery offers these: {unmatched}. Either the README is wrong, or "
        f"deliveries no longer offer the names it shows. {BREAKING_CHANGE}"
    )


def test_names_the_readme_and_the_example_dags_use_are_offered(offered_names):
    offered = {name for names in offered_names.values() for name in names}
    documented = find_documented_names()

    files = {README.name, *(path.name for path in EXAMPLE_DAGS)}
    assert {path for path, _ in documented} == files, f"Found names in fewer files than {files}: {documented}"
    missing = [(path, name) for path, name in documented if not is_offered(name, offered)]
    assert missing == [], (
        f"No delivery in tests/test_name_stability.py offers these names: {missing}. Add the delivery "
        f"that the README or the example Dag means; or, if deliveries offered them before: {BREAKING_CHANGE}"
    )
