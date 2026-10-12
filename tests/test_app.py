# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import hmac
import json
import os
from unittest import mock

import pytest
from airflow import plugins_manager
from airflow.api_fastapi.app import create_app
from airflow.models.asset import (
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
)
from airflow.models.dag import DagModel
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_assets, clear_db_dags, clear_db_runs

from airflow_inbound_webhooks.app import build_app
from airflow_inbound_webhooks.plugin import WebhooksPlugin

pytestmark = pytest.mark.db_test

SECRET = "s3cret"


def build_conn_env(name: str, **extra) -> dict[str, str]:
    conn = {"conn_type": "generic", "password": SECRET, "extra": extra}
    return {f"AIRFLOW_CONN_WEBHOOK_{name.upper()}": json.dumps(conn)}


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


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    clear_db_dags()
    clear_db_assets()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_assets()


@pytest.fixture
def subscribe(session, testing_dag_bundle):
    """Make a Dag schedule on an asset name, as parsing a Dag file would."""

    def subscribe_dag(dag_id: str, asset_name: str) -> None:
        asset_model = AssetModel(name=asset_name, uri=asset_name, group="asset")
        asset_model.scheduled_dags = [DagScheduleAssetReference(dag_id=dag_id)]
        session.add_all(
            [
                DagModel(dag_id=dag_id, is_paused=False, bundle_name="testing"),
                asset_model,
                AssetActive.for_asset(asset_model),
            ]
        )
        session.commit()

    return subscribe_dag


@pytest.fixture
def client():
    return TestClient(build_app())


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_github_label_triggers_only_subscribed_dags(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    subscribe("org_deploy_preview", "github/my-org/*/pull_request/labeled/deploy-preview")
    subscribe("deploy_staging", "github/my-org/web/push/main")

    response = post_github(client)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["asset_events"].keys() == {
        "github/my-org/web/pull_request/labeled/deploy-preview",
        "github/my-org/*/pull_request/labeled/deploy-preview",
    }
    assert len(body["candidates"]) == 6
    assert get_queued_dag_ids(session) == {"web_deploy_preview", "org_deploy_preview"}
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
            "github/my-org/web/pull_request/labeled",
            "github/my-org/*/pull_request/labeled",
            "github/my-org/web/pull_request/labeled/deploy-preview",
            "github/my-org/*/pull_request/labeled/deploy-preview",
        ],
    }
    assert count_asset_events(session) == 0


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
    ],
)
def test_misconfigured_connection_fails_closed(client, conn):
    with mock.patch.dict(os.environ, {"AIRFLOW_CONN_WEBHOOK_DEPLOYBOT": json.dumps(conn)}):
        response = client.post("/deploybot", json={})

    assert response.status_code == 503
    assert response.json() == {"detail": "Webhook is misconfigured"}


@mock.patch("airflow_inbound_webhooks.app.asset_manager", autospec=True)
@pytest.mark.parametrize(("team_name", "expected_teams"), [(None, set()), ("team_a", {"team_a"})])
def test_events_are_attributed_to_connection_team(mock_asset_manager, subscribe, team_name, expected_teams):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    mock_asset_manager.register_asset_change.return_value.id = 1

    with mock.patch.dict(os.environ, build_conn_env("github", team_name=team_name)):
        assert post_github(TestClient(build_app())).status_code == 202

    assert mock_asset_manager.register_asset_change.call_args.kwargs["api_user_teams"] == expected_teams


@mock.patch.dict(os.environ, GITHUB_CONN)
@conf_vars(
    {
        (
            "core",
            "auth_manager",
        ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
    }
)
def test_plugin_is_reachable_on_api_server_without_airflow_credentials():
    with mock.patch.object(
        plugins_manager, "get_fastapi_plugins", return_value=(WebhooksPlugin.fastapi_apps, [])
    ):
        api_server = TestClient(create_app())

    response = post_github(api_server, path="/webhooks/github")

    assert response.status_code == 202
    assert response.json()["status"] == "no_subscribers"
