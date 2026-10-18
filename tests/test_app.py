# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from airflow import settings
from airflow.assets.manager import AssetManager
from airflow.listeners import hookimpl
from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import AssetDagRunQueue, AssetEvent
from airflow.models.connection import Connection as ConnectionModel
from airflow.models.dag import DagModel
from airflow.models.log import Log
from airflow.utils.session import create_session
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select, update
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session

from airflow_inbound_webhooks import app
from airflow_inbound_webhooks.app import build_app, get_asset_change_arguments
from airflow_inbound_webhooks.models import delivery_table
from airflow_inbound_webhooks.sources import GitHubSource

if TYPE_CHECKING:
    from sqlalchemy.engine import Row

pytestmark = pytest.mark.usefixtures("clean_db")

SECRET = "s3cret"


def build_conn_env(
    name: str, *, conn_type: str = "inbound_webhook", password: str = SECRET, **extra
) -> dict[str, str]:
    conn = {"conn_type": conn_type, "password": password, "extra": extra}
    return {f"AIRFLOW_CONN_WEBHOOK_{name.upper()}": json.dumps(conn)}


def build_config_env(**options: str) -> dict[str, str]:
    return {f"AIRFLOW__INBOUND_WEBHOOKS__{option.upper()}": value for option, value in options.items()}


GITHUB_CONN = build_conn_env("github", max_body_bytes=4096)
GITLAB_CONN = build_conn_env("gitlab")
DEPLOYBOT_EXTRA = {
    "source_type": "hmac_json",
    "signature_header": "X-Signature",
    "event_field": "event",
    "path_field": "service",
}
DEPLOYBOT_CONN = build_conn_env("deploybot", **DEPLOYBOT_EXTRA)
LABELED = {
    "action": "labeled",
    "label": {"name": "deploy-preview"},
    "number": 7,
    "repository": {"full_name": "my-org/web"},
}


def post_github(
    client: TestClient,
    payload: dict = LABELED,
    *,
    path: str = "/github",
    event: str = "pull_request",
    secret: str = SECRET,
    content_type: str = "application/json",
    body: bytes | None = None,
    delivery_id: str = "delivery-1",
):
    body = json.dumps(payload).encode() if body is None else body
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return client.post(
        path,
        content=body,
        headers={
            "content-type": content_type,
            "X-Hub-Signature-256": f"sha256={signature}",
            "X-GitHub-Event": event,
            "X-GitHub-Delivery": delivery_id,
        },
    )


def post_deploybot(client: TestClient, payload: dict):
    body = json.dumps(payload).encode()
    return client.post(
        "/deploybot",
        content=body,
        headers={
            "content-type": "application/json",
            "X-Signature": hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest(),
        },
    )


def count_asset_events(session) -> int:
    return session.scalar(select(func.count()).select_from(AssetEvent))


def get_deliveries(session) -> list[Row]:
    return list(session.execute(select(delivery_table).order_by(delivery_table.c.received_at)))


def get_audit_log(session) -> list[Log]:
    return list(session.scalars(select(Log).where(Log.event == "webhook_delivery")))


@pytest.fixture
def stats():
    with mock.patch.object(app, "get_stats") as get_stats:
        yield get_stats.return_value


def get_queued_dag_ids(session) -> set[str]:
    return set(session.scalars(select(AssetDagRunQueue.target_dag_id)))


@pytest.fixture
def client():
    return TestClient(build_app())


def forget_process_state() -> None:
    app.resolved_webhooks.clear()
    app.resolve_locks.clear()
    app.rereads_allowed_at.clear()
    app.warn_no_webhook_names.cache_clear()
    app.warn_any_webhook_name.cache_clear()
    app.warn_connection_type.cache_clear()
    app.rate_limit_buckets.clear()
    app.database_error_log.last = -math.inf


@pytest.fixture(autouse=True)
def fresh_webhook_cache():
    """Forget resolved connections and rate limits, which would otherwise outlive the test that set them."""
    forget_process_state()
    yield
    forget_process_state()


@pytest.fixture
def connection_lookups():
    """Count connection lookups: the cost an unauthenticated request could add to a secrets backend."""
    with mock.patch.object(app.Connection, "get", wraps=app.Connection.get) as get:
        yield get


class AssetListener:
    """An Airflow listener that notes the delivery record of each asset event it hears of."""

    def __init__(self) -> None:
        # (inbound_delivery_id, whether that record was committed when the listener heard of the event)
        self.heard: list[tuple[str, bool]] = []

    @hookimpl
    def on_asset_event_emitted(self, asset_event) -> None:
        record_id = asset_event.extra["inbound_delivery_id"]
        with create_session(scoped=False) as session:
            committed = session.scalar(select(delivery_table.c.id).where(delivery_table.c.id == record_id))
        self.heard.append((record_id, committed is not None))


@pytest.fixture
def listener():
    manager = get_listener_manager()
    listener = AssetListener()
    manager.add_listener(listener)
    yield listener
    manager.pm.unregister(listener)


# Before 3.3, Airflow notifies listeners from inside the transaction that records an event.
needs_callback_sink = pytest.mark.skipif(
    not app.supports_callback_sink(),
    reason="Listeners can only be notified after the commit from Airflow 3.3",
)


@pytest.fixture
def notification():
    """A listener notification that the mocked asset manager hands back, as Airflow 3.3 and later do."""
    notify = mock.Mock()

    def register_asset_change(*, callback_sink, **kwargs):
        callback_sink.append(notify)
        return mock.Mock(id=1)

    with (
        mock.patch("airflow.assets.manager.asset_manager", autospec=True) as asset_manager,
        mock.patch.object(app, "supports_callback_sink", return_value=True),
    ):
        asset_manager.register_asset_change.side_effect = register_asset_change
        yield notify


def post_copy_that_lost_the_race(client: TestClient):
    """Deliver LABELED again, as a copy that looked its delivery up before the first copy committed it."""
    find_delivery = app.find_delivery

    def find_after_the_first_delivery_committed(*args):
        # The first lookup misses, as when both requests check before either has committed.
        return None if find.call_count == 1 else find_delivery(*args)

    with mock.patch.object(app, "find_delivery", side_effect=find_after_the_first_delivery_committed) as find:
        return post_github(client)


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_github_label_triggers_only_subscribed_dags(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    subscribe("org_deploy_preview", "github/my-org/*/pull_request/labeled/deploy-preview")
    subscribe("any_deploy_preview", "github/*/pull_request/labeled/deploy-preview")
    subscribe("deploy_staging", "github/my-org/web/push/main")

    response = post_github(client)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["asset_events"].keys() == {
        "github/my-org/web/pull_request/labeled/deploy-preview",
        "github/my-org/*/pull_request/labeled/deploy-preview",
        "github/*/pull_request/labeled/deploy-preview",
    }
    assert len(body["candidates"]) == 9
    assert get_queued_dag_ids(session) == {"web_deploy_preview", "org_deploy_preview", "any_deploy_preview"}
    (delivery,) = get_deliveries(session)
    assert session.scalars(select(AssetEvent.extra)).first() == {
        "from_webhook": True,
        "webhook": "github",
        "source_type": "github",
        "source": "github",
        "path": "my-org/web",
        "delivery_id": "delivery-1",
        "inbound_delivery_id": delivery.id,
        "correlation_key": None,
        "payload": LABELED,
    }


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_url_with_trailing_slash_is_served(session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    # Not followed: a redirect would pass here, but senders such as GitHub do not follow one.
    client = TestClient(build_app(), follow_redirects=False)

    response = post_github(client, path="/github/")

    assert response.status_code == 202
    assert get_queued_dag_ids(session) == {"web_deploy_preview"}


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
def test_api_docs_are_not_served(client, path):
    # Airflow mounts the app without authentication, so it serves nothing but POST /<name>.
    assert client.get(path).status_code == HTTPStatus.METHOD_NOT_ALLOWED


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_non_ascii_label_matches_its_folded_name(client, subscribe):
    # MySQL stores asset names as latin1, so a name is only found there once folded to ASCII.
    subscribe("cafe_preview", "github/my-org/web/pull_request/labeled/Cafe")

    response = post_github(client, {**LABELED, "label": {"name": "\U0001f680 Café"}})

    assert response.json()["asset_events"].keys() == {"github/my-org/web/pull_request/labeled/Cafe"}


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_no_subscribers_is_acknowledged_with_candidates(client, session):
    response = post_github(client)

    assert response.status_code == 202
    assert response.json() == {
        "status": "no_subscribers",
        "outcome": "no_subscribers",
        "delivery_id": "delivery-1",
        "asset_events": {},
        "candidates": [
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
    }
    assert count_asset_events(session) == 0


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_no_subscribers_logs_candidates_at_debug_level(client, caplog):
    caplog.set_level(logging.DEBUG, logger=app.__name__)

    candidates = post_github(client).json()["candidates"]

    records = [record for record in caplog.records if record.name == app.__name__]
    assert [(record.levelno, record.getMessage()) for record in records] == [
        (logging.DEBUG, f"Webhook github: no Dag schedules on any of its candidates {candidates}")
    ]


@mock.patch.dict(os.environ, GITLAB_CONN)
def test_gitlab_label_added_to_merge_request(client, session, subscribe):
    subscribe("api_deploy_preview", "gitlab/platform/api/merge_request/labeled/deploy-preview")
    payload = {
        "object_kind": "merge_request",
        "object_attributes": {"action": "update"},
        "project": {"path_with_namespace": "platform/api"},
        "changes": {"labels": {"previous": [], "current": [{"title": "deploy-preview"}]}},
    }

    response = client.post(
        "/gitlab",
        json=payload,
        headers={"X-Gitlab-Token": SECRET, "X-Gitlab-Event": "Merge Request Hook"},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert get_queued_dag_ids(session) == {"api_deploy_preview"}


@mock.patch.dict(os.environ, build_conn_env("jira"))
def test_jira_transition_stores_issue_summary(client, session, subscribe):
    subscribe("deploy_ready_issues", "jira/*/issue_updated/status/Ready for Deploy")
    payload = {
        "webhookEvent": "jira:issue_updated",
        "issue": {
            "key": "OPS-42",
            "fields": {"project": {"key": "OPS"}, "description": "A long description"},
        },
        "changelog": {"items": [{"field": "status", "toString": "Ready for Deploy"}]},
    }
    body = json.dumps(payload).encode()

    response = client.post(
        "/jira",
        content=body,
        headers={
            "content-type": "application/json; charset=UTF-8",
            "X-Hub-Signature": f"sha256={hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()}",
            "X-Atlassian-Webhook-Identifier": "webhook-1",
        },
    )

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert get_queued_dag_ids(session) == {"deploy_ready_issues"}
    (delivery,) = get_deliveries(session)
    assert session.scalars(select(AssetEvent.extra)).first() == {
        "from_webhook": True,
        "webhook": "jira",
        "source_type": "jira",
        "source": "jira",
        "path": "OPS",
        "delivery_id": "webhook-1",
        "inbound_delivery_id": delivery.id,
        "correlation_key": None,
        "payload": {**payload, "issue": {"key": "OPS-42", "fields": {"project": {"key": "OPS"}}}},
    }


@pytest.mark.parametrize(
    ("conn_env", "expected_payload"),
    [
        pytest.param(build_conn_env("github", store_payload="none"), None, id="none"),
        pytest.param(build_conn_env("github", store_payload="full"), LABELED, id="full"),
        pytest.param(GITHUB_CONN, LABELED, id="full-by-default"),
    ],
)
def test_stored_payload_follows_the_connection(client, session, subscribe, conn_env, expected_payload):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    with mock.patch.dict(os.environ, conn_env):
        assert post_github(client).json()["outcome"] == "accepted"

    assert session.scalars(select(AssetEvent.extra)).one()["payload"] == expected_payload
    assert [delivery.payload for delivery in get_deliveries(session)] == [expected_payload]


@mock.patch.dict(os.environ, build_conn_env("github", store_payload="summary"))
def test_summary_on_a_source_type_without_one_keeps_the_whole_payload(client, session, subscribe, caplog):
    # 0.1.0's form offered summary for every source type, and only jira used it.
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    assert post_github(client).status_code == 202

    assert session.scalars(select(AssetEvent.extra)).one()["payload"] == LABELED
    assert any("store_payload to summary" in record.getMessage() for record in caplog.records)


@mock.patch.dict(os.environ, build_conn_env("jira", store_payload="full"))
def test_jira_stores_the_whole_issue_when_asked(client, session, subscribe):
    subscribe("new_bugs", "jira/OPS/issue_created")
    payload = {
        "webhookEvent": "jira:issue_created",
        "issue": {"key": "OPS-1", "fields": {"project": {"key": "OPS"}, "description": "A long description"}},
    }
    body = json.dumps(payload).encode()

    client.post(
        "/jira",
        content=body,
        headers={
            "content-type": "application/json",
            "X-Hub-Signature": f"sha256={hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()}",
        },
    )

    assert session.scalars(select(AssetEvent.extra)).one()["payload"] == payload


@mock.patch.dict(os.environ, DEPLOYBOT_CONN)
def test_any_hmac_service_configured_from_connection_extra(client, session, subscribe):
    subscribe("smoke_test_billing", "deploybot/billing/deployed")
    body = json.dumps({"event": "deployed", "service": "billing"}).encode()

    response = client.post(
        "/deploybot",
        content=body,
        headers={
            "content-type": "application/json",
            "X-Signature": hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest(),
        },
    )

    assert response.status_code == 202
    assert response.json()["candidates"] == ["deploybot/billing/deployed", "deploybot/*/deployed"]
    assert get_queued_dag_ids(session) == {"smoke_test_billing"}


@mock.patch.dict(
    os.environ,
    build_conn_env(
        "huggingface", source_type="token_header", token_header="X-Webhook-Secret", event_field="event.action"
    ),
)
def test_token_header_service_configured_from_connection_extra(client, session, subscribe):
    subscribe("evaluate_model", "huggingface/update")

    response = client.post(
        "/huggingface", json={"event": {"action": "update"}}, headers={"X-Webhook-Secret": SECRET}
    )

    assert response.status_code == 202
    assert response.json()["candidates"] == ["huggingface/update"]
    assert get_queued_dag_ids(session) == {"evaluate_model"}


@mock.patch.dict(
    os.environ,
    build_conn_env(
        "deploybot",
        source_type="hmac_json",
        signature_header="X-Signature",
        event_field="event",
        event_header=None,
        signature_prefix="",
        max_body_bytes=None,
    ),
)
def test_empty_extra_fields_are_unset(client, session, subscribe):
    subscribe("smoke_test_billing", "deploybot/deployed")
    body = json.dumps({"event": "deployed"}).encode()

    response = client.post(
        "/deploybot",
        content=body,
        headers={
            "content-type": "application/json",
            "X-Signature": hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest(),
        },
    )

    assert response.status_code == 202
    assert get_queued_dag_ids(session) == {"smoke_test_billing"}


@pytest.mark.parametrize(
    ("process_context", "expected_status"),
    [pytest.param("server", 202, id="api-server"), pytest.param("client", 404, id="other-process")],
)
def test_connection_stored_in_metadata_db(client, session, process_context, expected_status):
    session.add(ConnectionModel(conn_id="webhook_github", conn_type="inbound_webhook", password=SECRET))
    session.commit()

    with mock.patch.dict(os.environ, {"_AIRFLOW_PROCESS_CONTEXT": process_context}):
        response = post_github(client)

    assert response.status_code == expected_status


@mock.patch.dict(os.environ, GITHUB_CONN)
@pytest.mark.parametrize(
    ("delivery", "expected_status", "expected_detail"),
    [
        pytest.param({"secret": "wrong"}, 401, "Invalid webhook credentials", id="bad-signature"),
        pytest.param({"path": "/bitbucket"}, 404, "Unknown webhook", id="no-connection"),
        pytest.param({"path": "/GitHub"}, 404, "Unknown webhook", id="invalid-name"),
        pytest.param({"body": b"x" * 4097}, 413, "Body exceeds 4096 bytes", id="too-large"),
        pytest.param({"content_type": "text/plain"}, 400, mock.ANY, id="not-json"),
    ],
)
def test_rejected_delivery(client, session, subscribe, delivery, expected_status, expected_detail):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    response = post_github(client, **delivery)

    assert response.status_code == expected_status
    assert response.json() == {"detail": expected_detail}
    assert count_asset_events(session) == 0


@pytest.mark.parametrize(
    "conn",
    [
        pytest.param(
            {"conn_type": "inbound_webhook", "password": SECRET, "extra": {}}, id="unknown-source-type"
        ),
        pytest.param({"conn_type": "inbound_webhook", "extra": {"source_type": "github"}}, id="no-secret"),
        pytest.param(
            {"conn_type": "inbound_webhook", "password": SECRET, "extra": {"source_type": "hmac_json"}},
            id="incomplete-source-options",
        ),
        pytest.param(
            {
                "conn_type": "inbound_webhook",
                "password": SECRET,
                "extra": {"source_type": "github", "max_body_bytes": 0},
            },
            id="no-body-allowed",
        ),
        pytest.param(
            {
                "conn_type": "inbound_webhook",
                "password": SECRET,
                "extra": {"source_type": "github", "team_name": "t" * 51},
            },
            id="team-name-too-long",
        ),
        pytest.param(
            {
                "conn_type": "inbound_webhook",
                "password": SECRET,
                "extra": {"source_type": "github", "store_payload": "everything"},
            },
            id="unknown-store-payload",
        ),
    ],
)
def test_misconfigured_connection_fails_closed(client, conn):
    with mock.patch.dict(os.environ, {"AIRFLOW_CONN_WEBHOOK_DEPLOYBOT": json.dumps(conn)}):
        response = client.post("/deploybot", json={})

    assert response.status_code == 503
    assert response.json() == {"detail": "Webhook is misconfigured"}


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_delivery_is_not_acknowledged_when_its_events_are_not_stored(session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    commit_failure = OperationalError("COMMIT", {}, Exception("database is locked"))

    with mock.patch.object(Session, "commit", side_effect=commit_failure):
        response = post_github(TestClient(build_app(), raise_server_exceptions=False))

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "30"
    assert count_asset_events(session) == 0
    assert get_deliveries(session) == []


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_delivery_is_recorded_with_what_arrived(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    response = post_github(client)

    (delivery,) = get_deliveries(session)
    assert delivery._asdict() == {
        "id": mock.ANY,
        "webhook": "github",
        "delivery_id": "delivery-1",
        "body_digest": hashlib.sha256(json.dumps(LABELED).encode()).hexdigest(),
        "received_at": mock.ANY,
        "source_type": "github",
        "path": "my-org/web",
        "candidates": response.json()["candidates"],
        "asset_events": response.json()["asset_events"],
        "correlation_key": None,
        "outcome": "accepted",
        "team_name": None,
        "payload": LABELED,
    }
    assert delivery.received_at.tzinfo is not None


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_each_event_carries_the_id_of_its_delivery_record(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    subscribe("any_pull_request", "github/*/pull_request")

    post_github(client)

    (delivery,) = get_deliveries(session)
    extras = list(session.scalars(select(AssetEvent.extra)))
    assert [extra["inbound_delivery_id"] for extra in extras] == [delivery.id] * 2


@mock.patch.dict(os.environ, DEPLOYBOT_CONN)
def test_events_of_a_delivery_without_a_delivery_id_carry_the_id_of_its_record(client, session, subscribe):
    subscribe("smoke_test_billing", "deploybot/billing/deployed")

    post_deploybot(client, {"event": "deployed", "service": "billing"})

    (delivery,) = get_deliveries(session)
    extra = session.scalars(select(AssetEvent.extra)).one()
    assert (extra["delivery_id"], extra["inbound_delivery_id"]) == (None, delivery.id)


@mock.patch.dict(os.environ, build_conn_env("deploybot", **DEPLOYBOT_EXTRA, delivery_id_field="id"))
def test_generic_source_recognises_retries_by_the_delivery_id_it_is_configured_with(
    client, session, subscribe
):
    subscribe("smoke_test_billing", "deploybot/billing/deployed")
    payload = {"id": "evt-1", "event": "deployed", "service": "billing"}

    first, retry = post_deploybot(client, payload), post_deploybot(client, payload)

    assert (first.status_code, retry.status_code) == (202, 200)
    assert (first.json()["delivery_id"], retry.json()["outcome"]) == ("evt-1", "duplicate")
    assert count_asset_events(session) == 1


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_unmatched_delivery_keeps_its_names_but_not_its_payload(client, session):
    response = post_github(client)

    (delivery,) = get_deliveries(session)
    assert (delivery.outcome, delivery.candidates, delivery.payload) == (
        "no_subscribers",
        response.json()["candidates"],
        None,
    )


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_retried_delivery_is_answered_as_before_and_not_recorded_again(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    first, retry = post_github(client), post_github(client)

    assert (first.status_code, retry.status_code) == (202, 200)
    assert retry.json() == {**first.json(), "outcome": "duplicate"}
    assert count_asset_events(session) == 1
    assert len(get_deliveries(session)) == 1


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_retry_recorded_concurrently_gets_the_answer_of_the_delivery_it_repeats(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    first = post_github(client)

    retry = post_copy_that_lost_the_race(client)

    assert retry.status_code == 200
    assert retry.json() == {**first.json(), "outcome": "duplicate"}
    (delivery,) = get_deliveries(session)
    # The one event left is the first copy's, so it links to the record that was kept.
    assert session.scalars(select(AssetEvent.extra)).one()["inbound_delivery_id"] == delivery.id


@needs_callback_sink
@mock.patch.dict(os.environ, GITHUB_CONN)
def test_listeners_hear_of_events_once_their_delivery_is_committed(client, session, subscribe, listener):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    subscribe("any_pull_request", "github/*/pull_request")

    post_github(client)

    (delivery,) = get_deliveries(session)
    assert listener.heard == [(delivery.id, True)] * 2


@needs_callback_sink
@mock.patch.dict(os.environ, GITHUB_CONN)
def test_listeners_never_hear_of_a_copy_that_lost_the_race(client, session, subscribe, listener):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    post_github(client)

    # The copy recorded an event under a record ID of its own, which rolled back with it.
    assert post_copy_that_lost_the_race(client).json()["outcome"] == "duplicate"

    (delivery,) = get_deliveries(session)
    assert listener.heard == [(delivery.id, True)]


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_listeners_hear_nothing_of_a_delivery_that_fails_to_commit(session, subscribe, notification):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    commit_failure = OperationalError("COMMIT", {}, Exception("database is locked"))

    with mock.patch.object(Session, "commit", side_effect=commit_failure):
        response = post_github(TestClient(build_app(), raise_server_exceptions=False))

    assert response.status_code == 503
    notification.assert_not_called()


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_listener_that_fails_after_the_commit_is_logged(client, session, subscribe, notification, caplog):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    notification.side_effect = RuntimeError("listener failed")

    response = post_github(client)

    # Answered as recorded, which it is.
    assert (response.status_code, response.json()["outcome"]) == (202, "accepted")
    assert [delivery.outcome for delivery in get_deliveries(session)] == ["accepted"]
    errors = [
        record for record in caplog.records if record.name == app.__name__ and record.levelno == logging.ERROR
    ]
    assert [record.getMessage() for record in errors] == ["Webhook github: an asset listener failed"]


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_redelivery_after_subscribing_records_the_delivery(client, session, subscribe):
    # How the README's walkthrough ends: send the event once, subscribe to a name it offered, redeliver.
    assert post_github(client).json()["status"] == "no_subscribers"
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    redelivery = post_github(client)

    assert (redelivery.status_code, redelivery.json()["status"]) == (202, "accepted")

    assert count_asset_events(session) == 1
    assert [(delivery.outcome, delivery.payload) for delivery in get_deliveries(session)] == [
        ("accepted", LABELED)
    ]


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_events_of_a_redelivery_carry_the_id_of_the_record_it_updates(client, session, subscribe):
    post_github(client)
    (unmatched,) = get_deliveries(session)
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    assert post_github(client).json()["outcome"] == "accepted"

    assert [delivery.id for delivery in get_deliveries(session)] == [unmatched.id]
    assert session.scalars(select(AssetEvent.extra)).one()["inbound_delivery_id"] == unmatched.id


@mock.patch("airflow.assets.manager.asset_manager", autospec=True)
@mock.patch.dict(os.environ, build_config_env(connection_cache_ttl="0"))
def test_redelivery_records_the_team_its_events_are_now_attributed_to(
    mock_asset_manager, client, session, subscribe
):
    # Only readers of the record's team are shown it, so it must be the team of the events it links to.
    mock_asset_manager.register_asset_change.return_value.id = 1
    with mock.patch.object(app, "supports_team_attribution", return_value=True):
        with mock.patch.dict(os.environ, build_conn_env("github", team_name="team_a")):
            assert post_github(client).json()["outcome"] == "no_subscribers"
        subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
        with mock.patch.dict(os.environ, build_conn_env("github", team_name="team_b")):
            assert post_github(client).json()["outcome"] == "accepted"

    assert mock_asset_manager.register_asset_change.call_args.kwargs["api_user_teams"] == {"team_b"}
    assert [delivery.team_name for delivery in get_deliveries(session)] == ["team_b"]


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_redelivery_recorded_meanwhile_gets_the_answer_of_the_copy_that_recorded_it(
    client, session, subscribe
):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    first = post_github(client)
    (recorded,) = get_deliveries(session)
    find_delivery = app.find_delivery

    def find_before_the_other_copy_committed(*args):
        # The first lookup sees the row as it was before another copy recorded the delivery's events.
        if find.call_count == 1:
            return mock.Mock(id=recorded.id, outcome="no_subscribers", asset_events={}, candidates=[])
        return find_delivery(*args)

    with mock.patch.object(app, "find_delivery", side_effect=find_before_the_other_copy_committed) as find:
        retry = post_github(client)

    assert retry.status_code == 200
    assert retry.json() == {**first.json(), "outcome": "duplicate"}
    assert session.scalars(select(AssetEvent.extra)).one()["inbound_delivery_id"] == recorded.id


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_another_body_under_a_recorded_delivery_id_is_a_new_delivery(client, session, subscribe):
    # The delivery ID is a header no signature covers, so it cannot claim a body it did not come with.
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    post_github(client)
    post_github(client, {**LABELED, "number": 8})

    assert count_asset_events(session) == 2


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_empty_delivery_id_is_no_delivery_id(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    for _ in range(2):
        post_github(client, delivery_id="")

    assert count_asset_events(session) == 2
    assert [delivery.delivery_id for delivery in get_deliveries(session)] == [None, None]


@mock.patch.dict(os.environ, DEPLOYBOT_CONN)
def test_path_too_long_for_its_column_is_stored_shortened(client, session, subscribe):
    subscribe("smoke_test_any_service", "deploybot/*/deployed")

    post_deploybot(client, {"event": "deployed", "service": "x" * 2000})

    (delivery,) = get_deliveries(session)
    assert delivery.path == "x" * 1500
    assert count_asset_events(session) == 1


@mock.patch.dict(os.environ, DEPLOYBOT_CONN)
def test_event_names_its_webhook_and_source_type(client, session, subscribe):
    subscribe("smoke_test_billing", "deploybot/billing/deployed")

    post_deploybot(client, {"event": "deployed", "service": "billing"})

    extra = session.scalars(select(AssetEvent.extra)).one()
    assert (extra["from_webhook"], extra["webhook"], extra["source_type"]) == (True, "deploybot", "hmac_json")


@pytest.mark.skipif(
    not hasattr(AssetEvent, "partition_key"), reason="Asset events only have a partition key from Airflow 3.2"
)
@mock.patch.dict(os.environ, DEPLOYBOT_CONN)
def test_recorded_events_have_no_partition_key(client, session, subscribe):
    # Airflow queues only Dags with a partitioned timetable for an event with a partition key, so a key
    # would keep the Dags that schedule on a name from running. A sender cannot set one either.
    subscribe("smoke_test_billing", "deploybot/billing/deployed")
    subscribe("smoke_test_any_service", "deploybot/*/deployed")

    post_deploybot(client, {"event": "deployed", "service": "billing", "partition_key": "2026-10-18"})

    assert list(session.scalars(select(AssetEvent.partition_key))) == [None, None]


@mock.patch.dict(os.environ, DEPLOYBOT_CONN)
def test_deliveries_without_an_id_are_never_taken_for_retries(client, session, subscribe):
    subscribe("smoke_test_billing", "deploybot/billing/deployed")

    for _ in range(2):
        assert post_deploybot(client, {"event": "deployed", "service": "billing"}).status_code == 202

    assert count_asset_events(session) == 2
    assert [delivery.delivery_id for delivery in get_deliveries(session)] == [None, None]


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_conn_env("github_enterprise", source_type="github")})
def test_delivery_ids_are_unique_per_webhook(client, session):
    post_github(client)
    post_github(client, path="/github_enterprise")

    assert sorted(delivery.webhook for delivery in get_deliveries(session)) == ["github", "github_enterprise"]


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_delivery_id_longer_than_can_be_stored_is_refused(client, session):
    response = post_github(client, delivery_id="x" * 256)

    assert response.status_code == 400
    assert response.json() == {"detail": "Delivery ID is longer than 255 characters"}
    assert get_deliveries(session) == []


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_delivery_only_paused_dags_subscribe_to_records_nothing(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview", paused=True)

    body = post_github(client).json()

    assert (body["status"], body["outcome"], body["asset_events"]) == ("no_subscribers", "dag_paused", {})
    assert count_asset_events(session) == 0
    assert [delivery.outcome for delivery in get_deliveries(session)] == ["dag_paused"]


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_paused_dag_does_not_hold_back_a_live_one_on_the_same_name(client, session, subscribe):
    name = "github/my-org/web/pull_request/labeled/deploy-preview"
    subscribe("paused_preview", name, paused=True)
    subscribe("live_preview", name)

    assert post_github(client).json()["outcome"] == "accepted"
    # Airflow queues only the Dag that is not paused.
    assert get_queued_dag_ids(session) == {"live_preview"}


@mock.patch.dict(os.environ, GITHUB_CONN)
@pytest.mark.parametrize("ref", ["name", "uri"])
def test_dag_scheduled_through_an_asset_ref_is_a_subscriber(client, session, subscribe, ref):
    name = "github/my-org/web/pull_request/labeled/deploy-preview"
    # A Dag that schedules on the asset directly keeps it active; this one is paused.
    subscribe("paused_preview", name, paused=True)
    subscribe("ref_preview", name, ref=ref)

    assert post_github(client).json()["outcome"] == "accepted"
    # Airflow queues a Dag scheduled through Asset.ref(name=...) or Asset.ref(uri=...) too.
    assert get_queued_dag_ids(session) == {"ref_preview"}


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_paused_dag_scheduled_through_an_asset_ref_makes_the_delivery_dag_paused(client, subscribe):
    name = "github/my-org/web/pull_request/labeled/deploy-preview"
    subscribe("removed_preview", name, stale=True)
    subscribe("paused_ref_preview", name, paused=True, ref="name")

    assert post_github(client).json()["outcome"] == "dag_paused"


@mock.patch.dict(os.environ, GITHUB_CONN)
@pytest.mark.parametrize("paused", [False, True])
def test_stale_dag_is_no_subscriber(client, session, subscribe, paused):
    # A stale Dag's file is gone, though its schedule can still be in the database.
    name = "github/my-org/web/pull_request/labeled/deploy-preview"
    subscribe("removed_preview", name, stale=True, paused=paused)

    assert post_github(client).json()["outcome"] == "no_subscribers"
    assert count_asset_events(session) == 0


@pytest.mark.skipif(not hasattr(DagModel, "is_draining"), reason="Dags can only drain from Airflow 3.4")
@mock.patch.dict(os.environ, GITHUB_CONN)
def test_draining_dag_is_no_live_subscriber(client, session, subscribe):
    subscribe("draining_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    session.execute(update(DagModel).values(is_draining=True))
    session.commit()

    assert post_github(client).json()["outcome"] == "dag_paused"
    assert count_asset_events(session) == 0


@mock.patch.dict(os.environ, GITHUB_CONN)
@pytest.mark.parametrize(
    ("subscribed", "posts", "expected_status", "expected_outcome"),
    [
        pytest.param(True, 1, 202, "accepted", id="recorded"),
        pytest.param(False, 1, 202, "no_subscribers", id="unmatched"),
        pytest.param(True, 2, 200, "duplicate", id="retry"),
    ],
)
def test_response_says_what_became_of_the_delivery(
    client, subscribe, subscribed, posts, expected_status, expected_outcome
):
    if subscribed:
        subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    for _ in range(posts):
        response = post_github(client)

    body = response.json()
    assert (response.status_code, body["outcome"], body["delivery_id"]) == (
        expected_status,
        expected_outcome,
        "delivery-1",
    )


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(rate_limit_per_minute="2")})
def test_deliveries_over_the_rate_limit_are_refused_until_it_refills(client):
    with mock.patch("airflow_inbound_webhooks.app.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        responses = [post_github(client, delivery_id=f"delivery-{n}") for n in range(3)]
        # Two a minute is one more delivery every 30 seconds.
        mock_time.monotonic.return_value = 1030.0
        responses.append(post_github(client, delivery_id="delivery-3"))

    assert [response.status_code for response in responses] == [202, 202, 429, 202]
    assert responses[2].headers["Retry-After"] == "30"


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(rate_limit_per_minute="1")})
def test_requests_that_fail_verification_do_not_use_up_the_rate_limit(client):
    # Otherwise anyone could have the sender's own deliveries refused.
    statuses = [post_github(client, secret="wrong").status_code for _ in range(3)]
    statuses += [post_github(client, delivery_id=f"delivery-{n}").status_code for n in range(2)]

    assert statuses == [401, 401, 401, 202, 429]


@mock.patch.dict(
    os.environ,
    {
        **GITHUB_CONN,
        **build_conn_env("github_enterprise", source_type="github"),
        **build_config_env(rate_limit_per_minute="1"),
    },
)
def test_each_webhook_has_its_own_rate_limit(client):
    paths = ["/github", "/github", "/github_enterprise"]

    assert [post_github(client, path=path).status_code for path in paths] == [202, 429, 202]


@mock.patch.dict(
    os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github", rate_limit_per_minute="1")}
)
def test_unlisted_names_never_reach_the_rate_limit(client):
    for _ in range(3):
        assert client.post("/gitlab", json={}).status_code == 404

    assert post_github(client).status_code == 202
    assert app.rate_limit_buckets.keys() == {"github"}


@mock.patch.dict(os.environ, GITHUB_CONN)
@pytest.mark.parametrize(
    ("error", "table_exists"),
    [
        pytest.param(
            OperationalError("SELECT", {}, Exception("could not connect")), True, id="database-down"
        ),
        pytest.param(
            ProgrammingError("SELECT", {}, Exception("relation does not exist")),
            False,
            id="table-not-created",
        ),
    ],
)
def test_unusable_database_is_answered_as_temporarily_unavailable(client, caplog, error, table_exists):
    with (
        mock.patch.object(app, "find_delivery", side_effect=error),
        mock.patch.object(app, "has_delivery_table", return_value=table_exists),
    ):
        responses = [post_github(client) for _ in range(2)]

    assert [(r.status_code, r.headers.get("Retry-After"), r.json()) for r in responses] == [
        (503, "30", {"detail": "Temporarily unavailable"})
    ] * 2
    # Once, though both failed: while the database is down, every delivery fails the same way.
    errors = [
        record for record in caplog.records if record.name == app.__name__ and record.levelno == logging.ERROR
    ]
    assert len(errors) == 1


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_other_programming_errors_are_not_taken_for_an_unavailable_database():
    # With the table there, the error is a bug, which retrying would not fix.
    error = ProgrammingError("SELECT", {}, Exception("syntax error"))

    with mock.patch.object(app, "find_delivery", side_effect=error):
        response = post_github(TestClient(build_app(), raise_server_exceptions=False))

    assert response.status_code == 500


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_verifier_that_raises_is_answered_as_a_failed_verification(client, session):
    with mock.patch.object(GitHubSource, "verify", side_effect=KeyError("X-Hub-Signature-256")):
        response = post_github(client)

    assert (response.status_code, response.json()) == (401, {"detail": "Invalid webhook credentials"})
    assert get_deliveries(session) == []


@mock.patch.dict(os.environ, build_conn_env("github", team_name="team_a"))
def test_team_name_rejected_when_asset_manager_cannot_attribute_teams(client, session):
    with mock.patch("airflow_inbound_webhooks.app.supports_team_attribution", return_value=False):
        response = post_github(client)

    assert response.status_code == 503
    assert response.json() == {"detail": "Webhook is misconfigured"}
    assert count_asset_events(session) == 0


@mock.patch("airflow.assets.manager.asset_manager", autospec=True)
@pytest.mark.parametrize(("team_name", "expected_teams"), [(None, set()), ("team_a", {"team_a"})])
def test_events_are_attributed_to_connection_team(mock_asset_manager, subscribe, team_name, expected_teams):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    mock_asset_manager.register_asset_change.return_value.id = 1

    with (
        mock.patch("airflow_inbound_webhooks.app.supports_team_attribution", return_value=True),
        mock.patch.dict(os.environ, build_conn_env("github", team_name=team_name)),
    ):
        assert post_github(TestClient(build_app())).status_code == 202

    assert mock_asset_manager.register_asset_change.call_args.kwargs["api_user_teams"] == expected_teams


@pytest.fixture
def fresh_asset_change_arguments():
    get_asset_change_arguments.cache_clear()
    yield
    get_asset_change_arguments.cache_clear()


@pytest.mark.usefixtures("fresh_asset_change_arguments")
@mock.patch("airflow.assets.manager.asset_manager", autospec=True)
@mock.patch.dict(os.environ, GITHUB_CONN)
def test_team_arguments_are_left_out_where_register_asset_change_lacks_them(
    mock_asset_manager, client, subscribe
):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    mock_asset_manager.register_asset_change.return_value.id = 1

    def register_asset_change(cls, *, asset, extra, session, **kwargs): ...  # As in Airflow 3.1 and 3.2

    with mock.patch.object(AssetManager, "register_asset_change", classmethod(register_asset_change)):
        assert post_github(client).status_code == 202

    assert mock_asset_manager.register_asset_change.call_args.kwargs.keys() == {"asset", "extra", "session"}


@pytest.mark.usefixtures("fresh_asset_change_arguments")
@mock.patch.dict(os.environ, GITHUB_CONN)
def test_delivery_fails_when_register_asset_change_no_longer_names_an_argument(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    # "extra" renamed: **kwargs would swallow it, and events would be stored without the payload.
    def register_asset_change(cls, *, asset, session, **kwargs): ...

    with (
        mock.patch.object(AssetManager, "register_asset_change", classmethod(register_asset_change)),
        pytest.raises(RuntimeError, match=r"no longer takes \['extra'\]"),
    ):
        post_github(client)

    assert count_asset_events(session) == 0


@mock.patch.dict(os.environ, GITHUB_CONN)
@pytest.mark.parametrize(
    ("webhook_names", "expected_status", "expected_lookups"),
    [
        pytest.param("github", 202, 1, id="listed"),
        pytest.param(" gitlab , github ", 202, 1, id="listed-among-others"),
        pytest.param("gitlab", 404, 0, id="not-listed"),
        pytest.param("", 404, 0, id="none-listed"),
        pytest.param("*", 202, 1, id="any-name"),
    ],
)
def test_only_listed_webhooks_are_looked_up(
    client, connection_lookups, webhook_names, expected_status, expected_lookups
):
    with mock.patch.dict(os.environ, build_config_env(webhook_names=webhook_names)):
        response = post_github(client)

    assert response.status_code == expected_status
    assert connection_lookups.call_count == expected_lookups


@mock.patch.dict(os.environ, build_config_env(webhook_names=""))
def test_unset_webhook_names_is_logged_once(client, caplog):
    for _ in range(2):
        assert post_github(client).json() == {"detail": "Unknown webhook"}

    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "webhook_names is not set" in warnings[0]


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="*")})
def test_serving_any_webhook_name_is_deprecated(client, caplog):
    for _ in range(2):
        assert post_github(client).status_code == 202

    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    # Once per process: a warning on every delivery would flood the log.
    assert len(warnings) == 1
    assert "webhook_names = * is deprecated" in warnings[0]


@mock.patch.dict(
    os.environ,
    {**build_conn_env("github", conn_type="generic"), **build_config_env(connection_cache_ttl="0")},
)
def test_connection_of_another_type_is_served_but_deprecated(client, caplog):
    # Read for each delivery, and logged once.
    statuses = [post_github(client, delivery_id=f"delivery-{i}").status_code for i in range(2)]

    assert statuses == [202, 202]
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "webhook_github has the type generic, which is deprecated" in warnings[0]


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github")})
def test_connection_is_read_once_per_cache_ttl(client, connection_lookups):
    with mock.patch("airflow_inbound_webhooks.app.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        statuses = [post_github(client).status_code for _ in range(2)]
        assert connection_lookups.call_count == 1

        mock_time.monotonic.return_value = 1060.0
        statuses.append(post_github(client).status_code)

    assert statuses == [202, 202, 202]
    assert connection_lookups.call_count == 2


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github")})
def test_failed_verifications_read_the_connection_again_once_per_cache_ttl(client, connection_lookups):
    with mock.patch("airflow_inbound_webhooks.app.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        statuses = [post_github(client, secret=secret).status_code for secret in (SECRET, "wrong", "wrong")]
        # Once, however many fail: anyone can send a delivery that fails verification.
        assert connection_lookups.call_count == 2

        mock_time.monotonic.return_value = 1059.0
        statuses.append(post_github(client, secret="wrong").status_code)
        assert connection_lookups.call_count == 2

        mock_time.monotonic.return_value = 1100.0
        statuses.append(post_github(client).status_code)
        statuses.append(post_github(client, secret="wrong").status_code)

    assert statuses == [202, 401, 401, 401, 202, 401]
    # At 1100 the cache has expired, and a minute has passed since the connection was last read again.
    assert connection_lookups.call_count == 4


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github")})
def test_changed_secret_applies_to_the_first_delivery_signed_with_it(client, connection_lookups):
    assert post_github(client, delivery_id="before").status_code == 202

    with mock.patch.dict(os.environ, build_conn_env("github", password="rotated", max_body_bytes=4096)):
        statuses = [
            post_github(client, secret=secret, delivery_id=f"after-{secret}").status_code
            for secret in ("rotated", SECRET)
        ]

    assert statuses == [202, 401]
    assert connection_lookups.call_count == 2


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github")})
def test_reading_the_connection_again_does_not_keep_it_cached_for_longer(client, connection_lookups):
    with mock.patch("airflow_inbound_webhooks.app.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        assert post_github(client).status_code == 202
        # The service has its new secret before the connection does, so reading it again finds the old.
        mock_time.monotonic.return_value = 1030.0
        assert post_github(client, secret="rotated").status_code == 401

        with mock.patch.dict(os.environ, build_conn_env("github", password="rotated", max_body_bytes=4096)):
            mock_time.monotonic.return_value = 1059.0
            assert post_github(client, secret="rotated").status_code == 401
            # The connection read at 1000 expires at 1060, as it would have without the second read.
            mock_time.monotonic.return_value = 1060.0
            assert post_github(client, secret="rotated").status_code == 202

    assert connection_lookups.call_count == 3


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github")})
def test_failed_verification_uses_a_connection_read_since_without_reading_it_again(connection_lookups):
    cached = app.get_configured_webhook("github")
    # As when another delivery had the connection read again while this one was being verified.
    newer = app.resolve_webhook("github")
    app.resolved_webhooks["github"] = (app.resolved_webhooks["github"][0], newer)

    assert app.reread_webhook(cached) is newer
    assert connection_lookups.call_count == 2


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github,gitlab")})
def test_slow_lookup_holds_up_only_its_own_webhook(client):
    gitlab_lookup_started, finish_gitlab_lookup = threading.Event(), threading.Event()
    get_connection = app.Connection.get

    def get_slowly(conn_id: str):
        if conn_id == "webhook_gitlab":
            gitlab_lookup_started.set()
            finish_gitlab_lookup.wait(timeout=30)
        return get_connection(conn_id)

    with mock.patch.object(app.Connection, "get", side_effect=get_slowly), ThreadPoolExecutor(2) as executor:
        try:
            executor.submit(client.post, "/gitlab", json={})
            assert gitlab_lookup_started.wait(timeout=30)
            # A lock shared by every webhook would keep this waiting until the gitlab lookup finished.
            response = executor.submit(post_github, client).result(timeout=10)
        finally:
            finish_gitlab_lookup.set()

    assert response.status_code == 202


@pytest.mark.parametrize(
    ("conn_env", "expected_status"),
    [
        pytest.param({}, 404, id="no-connection"),
        pytest.param(build_conn_env("github", source_type="bitbucket"), 503, id="misconfigured"),
    ],
)
def test_failed_lookup_is_cached(client, connection_lookups, conn_env, expected_status):
    with mock.patch.dict(os.environ, {**conn_env, **build_config_env(webhook_names="github")}):
        statuses = [post_github(client).status_code for _ in range(2)]

    assert statuses == [expected_status] * 2
    assert connection_lookups.call_count == 1


@mock.patch.dict(os.environ, GITHUB_CONN)
@pytest.mark.parametrize(
    "config",
    [
        pytest.param({"webhook_names": "github", "connection_cache_ttl": "0"}, id="cache-disabled"),
        pytest.param({"webhook_names": "*"}, id="any-name"),
    ],
)
def test_connection_is_read_on_every_delivery(client, connection_lookups, config):
    with mock.patch.dict(os.environ, build_config_env(**config)):
        statuses = [post_github(client, secret=secret).status_code for secret in (SECRET, "wrong")]

    assert statuses == [202, 401]
    # Once each: a delivery that fails verification was verified against a connection read for it.
    assert connection_lookups.call_count == 2


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_recorded_delivery_is_written_to_the_audit_log(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    asset_events = post_github(client).json()["asset_events"]

    (entry,) = get_audit_log(session)
    # No Dag or run: the scheduler creates the runs later.
    assert (entry.owner, entry.owner_display_name, entry.dag_id, entry.run_id) == (
        "webhook:github",
        "Webhook github",
        None,
        None,
    )
    assert json.loads(entry.extra) == {"delivery_id": "delivery-1", "asset_events": asset_events}


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_audit_entry_rolls_back_with_its_delivery(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    # What the transaction holds when it fails: only once it holds all three is their rollback tested.
    written = []

    def fail_to_commit(self):
        # Flushed first, as a commit is, so whatever order they are added in, all are in the database.
        self.flush()
        written.append((count_asset_events(self), len(get_audit_log(self)), len(get_deliveries(self))))
        raise OperationalError("COMMIT", {}, Exception("database is locked"))

    with mock.patch.object(Session, "commit", fail_to_commit):
        response = post_github(client)

    assert (response.status_code, response.headers["Retry-After"]) == (503, "30")
    assert written == [(1, 1, 1)]
    assert get_audit_log(session) == []
    assert count_asset_events(session) == 0
    assert get_queued_dag_ids(session) == set()
    assert get_deliveries(session) == []


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_delivery_whose_audit_entry_cannot_be_written_records_nothing(client, session, subscribe):
    # A recorded delivery always has its audit entry, so the entry cannot be left out to save the rest.
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    failed = []

    def fail_to_write_the_audit_entry(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith(f"INSERT INTO {Log.__tablename__} "):
            failed.append(statement)
            raise OperationalError(statement, parameters, Exception("database is locked"))

    event.listen(settings.engine, "before_cursor_execute", fail_to_write_the_audit_entry)
    try:
        response = post_github(client)
    finally:
        event.remove(settings.engine, "before_cursor_execute", fail_to_write_the_audit_entry)

    assert (len(failed), response.status_code, response.headers["Retry-After"]) == (1, 503, "30")
    assert get_audit_log(session) == []
    assert count_asset_events(session) == 0
    assert get_queued_dag_ids(session) == set()
    assert get_deliveries(session) == []


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_deliveries_that_record_nothing_are_not_audited(client, session, subscribe):
    assert post_github(client, delivery_id="unmatched").json()["outcome"] == "no_subscribers"
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    assert post_github(client).json()["outcome"] == "accepted"
    assert post_github(client).json()["outcome"] == "duplicate"
    assert post_github(client, secret="wrong").status_code == 401

    assert len(get_audit_log(session)) == 1


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_answered_deliveries_are_counted_and_timed(client, subscribe, stats):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")

    for _ in range(2):
        post_github(client)

    assert stats.incr.call_args_list == [
        mock.call("webhooks.deliveries", tags={"webhook": "github", "outcome": "accepted"}),
        mock.call("webhooks.deliveries", tags={"webhook": "github", "outcome": "duplicate"}),
    ]
    stats.timer.assert_called_with("webhooks.delivery_duration", tags={"webhook": "github"})


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github")})
def test_refused_deliveries_are_counted_by_status_with_unlisted_names_together(client, stats):
    post_github(client, secret="wrong")
    client.post("/random-name", json={})
    # Not a delivery: reading deliveries without a login is refused, but not counted.
    assert client.get("/_deliveries/github").status_code == 401

    assert stats.incr.call_args_list == [
        mock.call("webhooks.rejections", tags={"webhook": "github", "status": "401"}),
        mock.call("webhooks.rejections", tags={"webhook": "other", "status": "404"}),
    ]
