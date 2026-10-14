# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from unittest import mock

import pytest
from airflow.assets.manager import AssetManager
from airflow.models.asset import AssetDagRunQueue, AssetEvent
from airflow.models.connection import Connection as ConnectionModel
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from airflow_inbound_webhooks import app
from airflow_inbound_webhooks.app import build_app, get_asset_change_arguments

pytestmark = pytest.mark.usefixtures("clean_db")

SECRET = "s3cret"


def build_conn_env(name: str, **extra) -> dict[str, str]:
    conn = {"conn_type": "generic", "password": SECRET, "extra": extra}
    return {f"AIRFLOW_CONN_WEBHOOK_{name.upper()}": json.dumps(conn)}


def build_config_env(**options: str) -> dict[str, str]:
    return {f"AIRFLOW__INBOUND_WEBHOOKS__{option.upper()}": value for option, value in options.items()}


GITHUB_CONN = build_conn_env("github", max_body_bytes=4096)
GITLAB_CONN = build_conn_env("gitlab")
DEPLOYBOT_CONN = build_conn_env(
    "deploybot",
    source_type="hmac_json",
    signature_header="X-Signature",
    event_field="event",
    path_field="service",
)
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
            "X-GitHub-Delivery": "delivery-1",
        },
    )


def count_asset_events(session) -> int:
    return session.scalar(select(func.count()).select_from(AssetEvent))


def get_queued_dag_ids(session) -> set[str]:
    return set(session.scalars(select(AssetDagRunQueue.target_dag_id)))


@pytest.fixture
def client():
    return TestClient(build_app())


@pytest.fixture(autouse=True)
def fresh_webhook_cache():
    """Forget resolved connections, which would otherwise outlive the test that created them."""
    app.resolved_webhooks.clear()
    app.warn_no_webhook_names.cache_clear()
    yield
    app.resolved_webhooks.clear()
    app.warn_no_webhook_names.cache_clear()


@pytest.fixture
def connection_lookups():
    """Count connection lookups: the cost an unauthenticated request could add to a secrets backend."""
    with mock.patch.object(app.Connection, "get", wraps=app.Connection.get) as get:
        yield get


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
    assert session.scalars(select(AssetEvent.extra)).first() == {
        "source": "github",
        "path": "my-org/web",
        "delivery_id": "delivery-1",
        "payload": LABELED,
    }


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_no_subscribers_is_acknowledged_with_candidates(client, session):
    response = post_github(client)

    assert response.status_code == 202
    assert response.json() == {
        "status": "no_subscribers",
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
    assert session.scalars(select(AssetEvent.extra)).first() == {
        "source": "jira",
        "path": "OPS",
        "delivery_id": "webhook-1",
        "payload": {**payload, "issue": {"key": "OPS-42", "fields": {"project": {"key": "OPS"}}}},
    }


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
    session.add(ConnectionModel(conn_id="webhook_github", conn_type="generic", password=SECRET))
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
        pytest.param({"conn_type": "generic", "password": SECRET, "extra": {}}, id="unknown-source-type"),
        pytest.param({"conn_type": "generic", "extra": {"source_type": "github"}}, id="no-secret"),
        pytest.param(
            {"conn_type": "generic", "password": SECRET, "extra": {"source_type": "hmac_json"}},
            id="incomplete-source-options",
        ),
        pytest.param(
            {
                "conn_type": "generic",
                "password": SECRET,
                "extra": {"source_type": "github", "max_body_bytes": 0},
            },
            id="no-body-allowed",
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

    assert response.status_code == 500
    assert count_asset_events(session) == 0


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


@mock.patch.dict(os.environ, {**GITHUB_CONN, **build_config_env(webhook_names="github")})
def test_connection_is_read_once_per_cache_ttl(client, connection_lookups):
    with mock.patch("airflow_inbound_webhooks.app.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        # A failed verification must not read the connection again, or it would not bound the cost.
        statuses = [post_github(client, secret=secret).status_code for secret in (SECRET, "wrong", SECRET)]
        assert connection_lookups.call_count == 1

        mock_time.monotonic.return_value = 1060.0
        statuses.append(post_github(client).status_code)

    assert statuses == [202, 401, 202, 202]
    assert connection_lookups.call_count == 2


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
        statuses = [post_github(client).status_code for _ in range(2)]

    assert statuses == [202, 202]
    assert connection_lookups.call_count == 2
