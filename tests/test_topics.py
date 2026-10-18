# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import json

import attrs
import pytest
from airflow.sdk import DAG, Asset

from airflow_inbound_webhooks import WebhookTopic, topics
from airflow_inbound_webhooks.naming import build_candidate_names, can_be_offered
from airflow_inbound_webhooks.sources import WebhookEvent

try:
    from airflow.serialization.serialized_objects import DagSerialization
except ImportError:  # Airflow 3.1
    from airflow.serialization.serialized_objects import SerializedDAG as DagSerialization

# Assets have had access_control since Airflow 3.3.
HAS_ACCESS_CONTROL = "access_control" in attrs.fields_dict(Asset)
DEPLOY_PREVIEW = "github/my-org/web/pull_request/labeled/deploy-preview"


def build_event(path, *kinds) -> WebhookEvent:
    return WebhookEvent(path=path, kinds=list(kinds), delivery_id=None, payload={})


@pytest.mark.parametrize(
    ("segments", "event", "expected"),
    [
        pytest.param(
            {"path": "my-org/web", "event": "pull_request", "action": "labeled", "detail": "deploy-preview"},
            build_event("my-org/web", ("pull_request", "labeled", "deploy-preview")),
            DEPLOY_PREVIEW,
            id="every-segment",
        ),
        pytest.param(
            {"path": "my-org/web", "event": "pull_request"},
            build_event("my-org/web", ("pull_request", "labeled", "deploy-preview")),
            "github/my-org/web/pull_request",
            id="later-segments-left-out",
        ),
        pytest.param(
            {"path": "my-org/web", "event": "push", "detail": "main"},
            build_event("my-org/web", ("push", "main")),
            "github/my-org/web/push/main",
            id="detail-of-an-event-without-action",
        ),
        pytest.param(
            {"path": "my-org/*", "event": "pull_request", "action": "labeled"},
            build_event("my-org/web", ("pull_request", "labeled", "deploy-preview")),
            "github/my-org/*/pull_request/labeled",
            id="trailing-wildcard",
        ),
        pytest.param(
            {"path": "*", "event": "push", "detail": "main"},
            build_event("my-org/web", ("push", "main")),
            "github/*/push/main",
            id="any-path",
        ),
        pytest.param(
            {"path": "platform/backend/*", "event": "pipeline"},
            build_event("platform/backend/api", ("pipeline", "success")),
            "github/platform/backend/*/pipeline",
            id="deeper-path",
        ),
        pytest.param(
            {"event": "deployed"},
            build_event(None, ("deployed",)),
            "github/deployed",
            id="no-path",
        ),
        pytest.param(
            {"path": "o/web", "event": "pull_request", "action": "labeled", "detail": "\U0001f680 Café"},
            build_event("o/web", ("pull_request", "labeled", "\U0001f680 Café")),
            "github/o/web/pull_request/labeled/Cafe",
            id="non-ascii-folded",
        ),
        pytest.param(
            {"path": "ops/\U0001f680/api/", "event": "deployed"},
            build_event("ops/\U0001f680/api/", ("deployed",)),
            "github/ops/api/deployed",
            id="empty-path-segments-dropped",
        ),
        pytest.param(
            {"path": "my-org/web", "event": "push", "detail": "feature/x"},
            build_event("my-org/web", ("push", "feature/x")),
            "github/my-org/web/push/feature/x",
            id="branch-with-a-slash",
        ),
    ],
)
def test_segments_build_a_name_the_endpoint_offers(segments, event, expected):
    topic = WebhookTopic(webhook="github", **segments)

    assert topic.name == expected
    assert topic.name in build_candidate_names("github", event)


@pytest.mark.parametrize(
    "name",
    [
        DEPLOY_PREVIEW,
        "github/my-org/*/push/main",
        "github/*/pull_request",
        "huggingface/update",
        # A status holds spaces next to its slash, which the endpoint keeps.
        "jira/OPS/issue_updated/status/In Review / QA",
    ],
)
def test_whole_name_is_taken_as_it_is(name):
    assert WebhookTopic(name).name == WebhookTopic(name=name).name == WebhookTopic(name=name).uri == name


@pytest.mark.parametrize(
    ("name", "message"),
    [
        pytest.param(
            "github/o/web/pull_request/labeled/\U0001f680 Café",
            "never offered.*Did you mean 'github/o/web/pull_request/labeled/Cafe'",
            id="not-folded",
        ),
        pytest.param("github/my-org//web/push", "Did you mean 'github/my-org/web/push'", id="empty-part"),
        pytest.param("github/push/", "Did you mean 'github/push'", id="trailing-slash"),
        pytest.param("github/ push", "Did you mean 'github/push'", id="space-around-a-segment"),
        pytest.param("GitHub/push", "does not start with a webhook name", id="not-a-webhook-name"),
        pytest.param("/github/push", "does not start with a webhook name", id="leading-slash"),
        pytest.param("github", "has no event", id="webhook-name-only"),
        pytest.param("github/ / ", "has no event", id="blank-event"),
    ],
)
def test_name_no_delivery_can_offer_is_refused(name, message):
    with pytest.raises(ValueError, match=message):
        WebhookTopic(name)


@pytest.mark.parametrize(
    ("segments", "message"),
    [
        pytest.param({"webhook": "GitHub", "event": "push"}, "is not a webhook name", id="webhook-name"),
        pytest.param(
            {"webhook": "github", "event": "push", "detail": "\U0001f680"},
            "detail '\U0001f680' is empty once folded",
            id="detail-folds-to-nothing",
        ),
        pytest.param({"webhook": "github", "event": " "}, "event ' ' is empty", id="blank-event"),
        pytest.param({"webhook": "github", "event": 1}, "event must be a string", id="not-a-string"),
    ],
)
def test_segments_no_delivery_has_are_refused(segments, message):
    with pytest.raises(ValueError, match=message):
        WebhookTopic(**segments)


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        pytest.param({}, "requires a name", id="nothing"),
        pytest.param({"uri": DEPLOY_PREVIEW}, "requires a name", id="uri-only"),
        pytest.param({"name": DEPLOY_PREVIEW, "event": "push"}, "not both", id="name-and-segments"),
        pytest.param({"webhook": "github", "path": "my-org/web"}, "needs both", id="no-event"),
        pytest.param({"event": "push"}, "needs both", id="no-webhook"),
    ],
)
def test_arguments_that_make_no_name_are_refused(arguments, message):
    with pytest.raises(TypeError, match=message):
        WebhookTopic(**arguments)


def test_names_built_from_segments_can_be_offered():
    topic = WebhookTopic(webhook="jira", path=" OPS ", event="issue_updated", action="status", detail="Done ")

    assert topic.name == "jira/OPS/issue_updated/status/Done"
    assert can_be_offered(topic.name)


def test_topic_is_an_asset_in_the_group_webhook():
    topic = WebhookTopic(DEPLOY_PREVIEW)

    assert isinstance(topic, Asset)
    assert (topic.group, topic.uri, topic.extra, topic.watchers) == ("webhook", DEPLOY_PREVIEW, {}, [])


def test_uri_other_than_the_name_is_refused():
    with pytest.raises(ValueError, match="a topic's URI is its name"):
        WebhookTopic(DEPLOY_PREVIEW, "s3://bucket/key")


@pytest.mark.parametrize(
    ("argument", "value"),
    [("extra", {"team": "web"}), ("watchers", [object()])],
)
def test_extra_and_watchers_are_refused(argument, value):
    with pytest.raises(ValueError, match=f"takes no {argument}"):
        WebhookTopic(DEPLOY_PREVIEW, **{argument: value})


@pytest.mark.parametrize(
    ("argument", "value"),
    [("extra", None), ("extra", {}), ("watchers", None), ("watchers", [])],
)
def test_empty_extra_and_watchers_are_accepted(argument, value):
    assert WebhookTopic(DEPLOY_PREVIEW, **{argument: value}) == WebhookTopic(DEPLOY_PREVIEW)


@pytest.mark.parametrize(
    "duplicate",
    [attrs.evolve, copy.copy, copy.deepcopy],
    ids=["attrs-evolve", "copy", "deepcopy"],
)
def test_topic_survives_evolving_and_copying(duplicate):
    # attrs.evolve passes every field to __init__, the empty extra and watchers included.
    topic = WebhookTopic(webhook="github", path="my-org/*", event="push", detail="main")

    duplicated = duplicate(topic)

    assert type(duplicated) is WebhookTopic
    assert duplicated == topic


def test_evolving_a_topic_into_another_group_keeps_it_a_topic():
    evolved = attrs.evolve(WebhookTopic(DEPLOY_PREVIEW), group="deploys")

    assert (type(evolved), evolved.name, evolved.group) == (WebhookTopic, DEPLOY_PREVIEW, "deploys")


@pytest.mark.skipif(not HAS_ACCESS_CONTROL, reason="Assets have access_control from Airflow 3.3")
def test_access_control_is_kept_through_evolving():
    from airflow.sdk import AssetAccessControl

    access_control = AssetAccessControl(producer_teams=["web"])

    topic = attrs.evolve(WebhookTopic(DEPLOY_PREVIEW, access_control=access_control))

    assert topic.access_control == access_control


@pytest.mark.skipif(HAS_ACCESS_CONTROL, reason="Assets have access_control from Airflow 3.3")
def test_access_control_is_refused_where_assets_lack_it():
    with pytest.raises(TypeError, match="access_control"):
        WebhookTopic(DEPLOY_PREVIEW, access_control=object())


def test_dag_scheduled_on_a_topic_keeps_its_name_and_group_through_serialization():
    dag = DAG("deploy_preview", schedule=WebhookTopic(DEPLOY_PREVIEW))

    # Through JSON, as the serialized Dag is stored.
    serialized = DagSerialization.from_dict(json.loads(json.dumps(DagSerialization.to_dict(dag))))

    asset = serialized.timetable.asset_condition
    assert (asset.name, asset.uri, asset.group) == (DEPLOY_PREVIEW, DEPLOY_PREVIEW, "webhook")


def test_topic_is_exported_from_the_package():
    import airflow_inbound_webhooks

    assert airflow_inbound_webhooks.WebhookTopic is topics.WebhookTopic
    with pytest.raises(AttributeError, match="no attribute 'WebhookTopics'"):
        airflow_inbound_webhooks.WebhookTopics
