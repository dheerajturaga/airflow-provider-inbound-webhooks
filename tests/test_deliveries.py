# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
The deliveries API and page, in process, with the auth manager replaced.

tests/test_api_server.py checks their login against the real API server.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from airflow.api_fastapi.auth.managers.models.resource_details import ConnectionDetails
from airflow.models.asset import AssetEvent
from fastapi.testclient import TestClient
from sqlalchemy import select

from airflow_inbound_webhooks import app, deliveries
from airflow_inbound_webhooks.app import build_app
from airflow_inbound_webhooks.sources import MAX_JSON_DEPTH

pytestmark = pytest.mark.usefixtures("clean_db")

READER = object()
NOW = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)
CONNECTION = json.dumps({"conn_type": "inbound_webhook", "password": "s3cret"})
TEAM_A_CONNECTION = {
    "AIRFLOW_CONN_WEBHOOK_GITHUB": json.dumps(
        {"conn_type": "inbound_webhook", "extra": {"team_name": "team_a"}}
    )
}
LABELED = {
    "action": "labeled",
    "label": {"name": "deploy-preview"},
    "repository": {"full_name": "my-org/web"},
}


@pytest.fixture(autouse=True)
def webhook_connections():
    """Readers are checked against a webhook's connection, so the webhooks read here have one."""
    with mock.patch.dict(
        os.environ, {"AIRFLOW_CONN_WEBHOOK_GITHUB": CONNECTION, "AIRFLOW_CONN_WEBHOOK_GITLAB": CONNECTION}
    ):
        yield


@pytest.fixture
def auth_manager():
    manager = mock.Mock()
    manager.is_authorized_connection.return_value = True
    with mock.patch("airflow.api_fastapi.app.get_auth_manager", return_value=manager):
        yield manager


@pytest.fixture
def client(auth_manager):
    """A client logged in as READER, whose access auth_manager decides."""
    webhooks = build_app()
    webhooks.dependency_overrides[deliveries.get_reader] = lambda: READER
    return TestClient(webhooks)


def test_health_needs_no_login():
    response = TestClient(build_app()).get("/_health")

    assert (response.status_code, response.json()) == (200, {"status": "healthy"})


@pytest.mark.parametrize("path", ["/_deliveries/github", "/_ui/deliveries"])
def test_reading_deliveries_needs_a_login(path):
    assert TestClient(build_app()).get(path).status_code == 401


def test_reading_needs_read_access_to_the_webhooks_connection(client, auth_manager):
    auth_manager.is_authorized_connection.return_value = False

    response = client.get("/_deliveries/github")

    assert response.status_code == 403
    auth_manager.is_authorized_connection.assert_called_once_with(
        method="GET", details=ConnectionDetails(conn_id="webhook_github", team_name=None), user=READER
    )


@mock.patch.dict(os.environ, TEAM_A_CONNECTION)
def test_access_is_checked_for_the_team_of_the_webhooks_connection(client, auth_manager):
    client.get("/_deliveries/github")

    assert auth_manager.is_authorized_connection.call_args.kwargs["details"].team_name == "team_a"


def test_webhook_whose_connection_cannot_be_read_shows_nothing(client, auth_manager, add_delivery):
    # Without the connection there is no team to check the reader against, and airflow.sdk's
    # Connection also reports a secrets backend it could not read as a missing connection.
    add_delivery(webhook="jira", source_type="jira")

    assert client.get("/_deliveries/jira").status_code == 404
    assert "?webhook=jira" not in client.get("/_ui/deliveries").text
    checked = {
        call.kwargs["details"].conn_id for call in auth_manager.is_authorized_connection.call_args_list
    }
    assert "webhook_jira" not in checked


@mock.patch.dict(os.environ, TEAM_A_CONNECTION)
def test_deliveries_attributed_to_another_team_are_not_shown(client, add_delivery):
    ours = add_delivery(team_name="team_a")
    theirs = add_delivery(team_name="team_b")
    add_delivery(team_name=None)

    listed = client.get("/_deliveries/github").json()["deliveries"]

    assert [delivery["id"] for delivery in listed] == [ours]
    assert client.get(f"/_deliveries/github/{theirs}").status_code == 404


def test_names_no_webhook_can_have_are_not_looked_up(client, auth_manager):
    assert client.get("/_deliveries/GitHub").status_code == 404
    auth_manager.is_authorized_connection.assert_not_called()


def test_deliveries_are_listed_newest_first_without_payloads(client, add_delivery):
    older = add_delivery(received_at=NOW - timedelta(minutes=1))
    newer = add_delivery(received_at=NOW)
    add_delivery(webhook="gitlab", source_type="gitlab")

    listed = client.get("/_deliveries/github").json()["deliveries"]

    assert [delivery["id"] for delivery in listed] == [newer, older]
    assert "payload" not in listed[0]
    assert listed[0]["received_at"] == "2026-10-17T12:00:00+00:00"


@pytest.mark.parametrize(
    "before", ["2026-10-17T12:00:00+00:00", "2026-10-17T12:00:00"], ids=["utc", "no-zone"]
)
def test_before_pages_back(client, add_delivery, before):
    older = add_delivery(received_at=NOW - timedelta(minutes=1))
    add_delivery(received_at=NOW)

    listed = client.get("/_deliveries/github", params={"before": before}).json()["deliveries"]

    assert [delivery["id"] for delivery in listed] == [older]


def test_deliveries_received_at_the_same_time_are_paged_without_gaps(client, add_delivery):
    ids = {add_delivery(received_at=NOW) for _ in range(3)}

    first = client.get("/_deliveries/github", params={"limit": 2}).json()["deliveries"]
    params = {"limit": 2, "before": first[-1]["received_at"], "before_id": first[-1]["id"]}
    second = client.get("/_deliveries/github", params=params).json()["deliveries"]

    assert len(first) == 2
    assert {delivery["id"] for delivery in first + second} == ids


def test_deliveries_are_found_by_the_delivery_id_the_sender_gave_them(client, add_delivery):
    # A retry is recognised by its body too, so several deliveries can have one delivery ID.
    first = add_delivery(delivery_id="d-1", received_at=NOW - timedelta(minutes=1))
    second = add_delivery(delivery_id="d-1", body_digest="1" * 64, received_at=NOW)
    add_delivery(delivery_id="d-2")

    listed = client.get("/_deliveries/github", params={"delivery_id": "d-1"}).json()["deliveries"]

    assert [delivery["id"] for delivery in listed] == [second, first]


@mock.patch.dict(os.environ, TEAM_A_CONNECTION)
@pytest.mark.parametrize("path", ["/_deliveries/github", "/_ui/deliveries"])
def test_deliveries_found_by_delivery_id_are_only_the_webhooks_teams(client, add_delivery, path):
    ours = add_delivery(delivery_id="d-1", team_name="team_a")
    theirs = add_delivery(delivery_id="d-1", body_digest="1" * 64, team_name="team_b")
    other = add_delivery(delivery_id="d-2", team_name="team_a")

    found = client.get(path, params={"webhook": "github", "delivery_id": "d-1"}).text

    assert ours in found
    assert theirs not in found
    assert other not in found


def test_page_finds_deliveries_by_delivery_id(client, add_delivery):
    found = add_delivery(delivery_id="d-1")
    other = add_delivery(delivery_id="d-2")

    page = client.get("/_ui/deliveries", params={"webhook": "github", "delivery_id": "d-1"}).text

    assert found in page
    assert other not in page
    # The search field keeps what was searched for.
    assert "name='delivery_id' value='d-1'" in page
    unknown = client.get("/_ui/deliveries", params={"webhook": "github", "delivery_id": "d-3"})
    assert "No deliveries with this delivery ID" in unknown.text


def test_page_search_left_blank_shows_every_delivery(client, add_delivery):
    ids = [add_delivery(delivery_id="d-1"), add_delivery(delivery_id=None)]

    page = client.get("/_ui/deliveries", params={"webhook": "github", "delivery_id": ""}).text

    assert all(delivery in page for delivery in ids)


def test_page_links_to_older_deliveries_with_the_delivery_id_searched_for(client, add_delivery):
    oldest = add_delivery(delivery_id="d-1", received_at=NOW - timedelta(hours=1))
    for minute in (1, 2):
        digest = str(minute) * 64
        add_delivery(delivery_id="d-1", body_digest=digest, received_at=NOW + timedelta(minutes=minute))
    older_without_it = add_delivery(delivery_id="d-2", received_at=NOW - timedelta(hours=2))

    with mock.patch.object(deliveries, "DEFAULT_LIMIT", 2):
        page = client.get("/_ui/deliveries", params={"webhook": "github", "delivery_id": "d-1"}).text
        link = page.split("<a href='?")[-1].split("'")[0].replace("&amp;", "&")
        older = client.get(f"/_ui/deliveries?{link}").text

    assert "delivery_id=d-1" in link
    assert oldest in older
    assert older_without_it not in older


def test_page_escapes_the_delivery_id_searched_for(client):
    # A link can carry any delivery ID to a reader.
    searched = "'><img src=x onerror=alert(1)>"

    page = client.get("/_ui/deliveries", params={"webhook": "github", "delivery_id": searched}).text

    assert "<img" not in page
    assert "value='&#x27;&gt;&lt;img src=x onerror=alert(1)&gt;'" in page


def test_one_delivery_comes_with_its_payload(client, add_delivery):
    delivery = add_delivery()

    assert client.get(f"/_deliveries/github/{delivery}").json()["payload"] == {"ref": "refs/heads/main"}
    # Another webhook's delivery is not found under this one.
    assert client.get(f"/_deliveries/gitlab/{delivery}").status_code == 404


def post_labeled(client: TestClient, payload: dict = LABELED) -> None:
    """Deliver ``payload`` to the github webhook, signed with its connection's secret."""
    body = json.dumps(payload).encode()
    headers = {
        "content-type": "application/json",
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "delivery-1",
        "X-Hub-Signature-256": f"sha256={hmac.new(b's3cret', body, hashlib.sha256).hexdigest()}",
    }
    # So the connection the delivery is verified with is not kept for later tests.
    with mock.patch.dict(app.resolved_webhooks):
        assert client.post("/github", content=body, headers=headers).status_code == 202


def test_asset_event_finds_its_delivery_by_its_inbound_delivery_id(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    post_labeled(client)

    (extra,) = session.scalars(select(AssetEvent.extra))
    delivery = client.get(f"/_deliveries/github/{extra['inbound_delivery_id']}").json()

    assert (delivery["delivery_id"], delivery["payload"]) == ("delivery-1", LABELED)


@mock.patch.dict(os.environ, {"AIRFLOW__INBOUND_WEBHOOKS__MAX_EVENT_PAYLOAD_BYTES": "1"})
def test_payload_left_out_of_an_asset_event_is_found_with_its_delivery(client, session, subscribe):
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    post_labeled(client)

    (extra,) = session.scalars(select(AssetEvent.extra))
    delivery = client.get(f"/_deliveries/github/{extra['inbound_delivery_id']}").json()

    assert (extra["payload"], extra["payload_truncated"], delivery["payload"]) == (None, True, LABELED)


def test_payload_nested_as_deeply_as_allowed_is_shown(client, session, subscribe):
    # The API encodes the payload, and the page indents it, each walking it once per level.
    subscribe("web_deploy_preview", "github/my-org/web/pull_request/labeled/deploy-preview")
    payload = {**LABELED, "added": json.loads("[" * (MAX_JSON_DEPTH - 1) + "]" * (MAX_JSON_DEPTH - 1))}
    post_labeled(client, payload)
    (extra,) = session.scalars(select(AssetEvent.extra))

    delivery = client.get(f"/_deliveries/github/{extra['inbound_delivery_id']}")
    page = client.get(f"/_ui/deliveries/github/{extra['inbound_delivery_id']}")

    assert (delivery.status_code, delivery.json()["payload"]) == (200, payload)
    assert page.status_code == 200


@mock.patch.dict(os.environ, {"AIRFLOW__INBOUND_WEBHOOKS__WEBHOOK_NAMES": "github,gitlab"})
def test_page_lists_the_webhooks_the_reader_may_read(client, auth_manager, add_delivery):
    add_delivery()
    add_delivery(webhook="gitlab", source_type="gitlab")
    auth_manager.is_authorized_connection.side_effect = lambda details, **_: (
        details.conn_id == "webhook_github"
    )

    page = client.get("/_ui/deliveries").text

    assert "?webhook=github" in page
    assert "gitlab" not in page


def test_pages_escape_what_senders_wrote(client, add_delivery):
    delivery = add_delivery(
        path="<script>alert(1)</script>", payload={"title": "<img src=x onerror=alert(1)>"}
    )

    for response in (
        client.get("/_ui/deliveries", params={"webhook": "github"}),
        client.get(f"/_ui/deliveries/github/{delivery}"),
    ):
        assert "<script>" not in response.text
        assert "<img" not in response.text
        assert "default-src 'none'" in response.headers["Content-Security-Policy"]
    assert "&lt;img src=x onerror=alert(1)&gt;" in response.text


def test_page_links_to_older_deliveries(client, add_delivery):
    oldest = add_delivery(received_at=NOW - timedelta(hours=1))
    for minute in range(deliveries.DEFAULT_LIMIT):
        add_delivery(received_at=NOW + timedelta(minutes=minute))

    page = client.get("/_ui/deliveries", params={"webhook": "github"}).text
    # The time's "+" is encoded, or the link would turn it into a space.
    link = page.split("<a href='?")[-1].split("'")[0].replace("&amp;", "&")

    assert "%2B00%3A00" in link
    assert oldest in client.get(f"/_ui/deliveries?{link}").text
