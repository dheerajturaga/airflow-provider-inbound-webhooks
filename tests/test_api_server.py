# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Run the real ``airflow api-server`` with the installed plugin and send it signed deliveries over HTTP.

This is the only test of the production path: the plugin loaded through its entry point, mounted
without Airflow authentication, reading its secret from a metadata-database connection in the API
server's server context.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request
from importlib.util import find_spec
from pathlib import Path
from typing import Any

import airflow
import pytest
from airflow.models.asset import AssetDagRunQueue, AssetEvent
from airflow.models.connection import Connection
from packaging.version import Version
from sqlalchemy import select

from airflow_inbound_webhooks.app import supports_team_attribution
from airflow_inbound_webhooks.connection_form import CONN_TYPE, FIELDS, HOOK_NAME
from airflow_inbound_webhooks.models import delivery_table

pytestmark = pytest.mark.usefixtures("clean_db")

SECRET = "s3cret"
ASSET_NAME = "github/my-org/web/pull_request/labeled/deploy-preview"
LABELED = {
    "action": "labeled",
    "label": {"name": "deploy-preview"},
    "repository": {"full_name": "my-org/web"},
}
# Airflow 3.1.1 and 3.1.2 can only build connection forms when Flask-AppBuilder is installed.
CONNECTION_FORMS_AVAILABLE = Version(airflow.__version__) >= Version("3.1.3") or bool(
    find_spec("flask_appbuilder")
)
# Airflow 3.1 builds forms with WTForms replaced by mocks when it is not installed. On Python 3.10,
# mock.patch reaches wtforms.validators through the wtforms mock rather than the module the hook
# imports, so the any_of mock is missed and choice fields become text fields.
CHOICES_SHOWN = not (
    Version(airflow.__version__) < Version("3.2") and sys.version_info < (3, 11) and not find_spec("wtforms")
)


def find_free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def api_server_url():
    port = find_free_port()
    url = f"http://127.0.0.1:{port}"
    log_path = Path(os.environ["AIRFLOW_HOME"]) / "api-server.log"
    env = {
        **os.environ,
        "AIRFLOW__INBOUND_WEBHOOKS__WEBHOOK_NAMES": "github,demo",
        # Tests change a webhook's connection between deliveries, so it must be read on each.
        "AIRFLOW__INBOUND_WEBHOOKS__CONNECTION_CACHE_TTL": "0",
    }
    with log_path.open("w") as log:
        # Its own process group, so stopping it also stops the uvicorn workers it starts.
        server = subprocess.Popen(
            [sys.executable, "-m", "airflow", "api-server", "--port", str(port)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    try:
        deadline = time.monotonic() + 120
        while True:
            try:
                urllib.request.urlopen(f"{url}/api/v2/monitor/health", timeout=2)
                break
            except OSError:
                if server.poll() is not None or time.monotonic() > deadline:
                    pytest.fail(f"airflow api-server did not start:\n{log_path.read_text()[-3000:]}")
                time.sleep(1)
        yield url
    finally:
        os.killpg(server.pid, signal.SIGTERM)
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(server.pid, signal.SIGKILL)


def post_github(url: str, *, secret: str = SECRET) -> tuple[int, dict]:
    body = json.dumps(LABELED).encode()
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    request = urllib.request.Request(
        f"{url}/webhooks/github",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": f"sha256={signature}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


@pytest.fixture
def webhook_connection(session):
    def add_connection(name: str, **extra) -> None:
        session.add(
            Connection(
                conn_id=f"webhook_{name}", conn_type=CONN_TYPE, password=SECRET, extra=json.dumps(extra)
            )
        )
        session.commit()

    return add_connection


def test_signed_delivery_queues_subscribed_dag(api_server_url, session, subscribe, webhook_connection):
    webhook_connection("github")
    subscribe("web_deploy_preview", ASSET_NAME)

    status, body = post_github(api_server_url)

    assert (status, body) == (202, {"delivery_id": None, "outcome": "accepted"})
    assert [extra["source"] for extra in session.scalars(select(AssetEvent.extra))] == ["github"]
    assert list(session.scalars(select(AssetDagRunQueue.target_dag_id))) == ["web_deploy_preview"]
    # The table exists where Airflow put it, and the API server process can write to it.
    deliveries = session.execute(select(delivery_table.c.outcome, delivery_table.c.asset_events))
    assert [(outcome, list(asset_events)) for outcome, asset_events in deliveries] == [
        ("accepted", [ASSET_NAME])
    ]


def test_wrong_secret_is_rejected(api_server_url, session, subscribe, webhook_connection):
    webhook_connection("github")
    subscribe("web_deploy_preview", ASSET_NAME)

    status, body = post_github(api_server_url, secret="wrong")

    assert (status, body) == (401, {"detail": "Invalid webhook credentials"})
    assert list(session.scalars(select(AssetEvent.id))) == []


def test_team_name_needs_team_attribution(api_server_url, subscribe, webhook_connection):
    webhook_connection("github", team_name="team_a")
    subscribe("web_deploy_preview", ASSET_NAME)

    status, _ = post_github(api_server_url)

    assert status == (202 if supports_team_attribution() else 503)


def request_json(
    url: str, *, method: str = "GET", body: Any = None, token: str | None = None
) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def test_unlisted_webhook_is_unknown(api_server_url, webhook_connection):
    webhook_connection("gitlab")

    status, body = request_json(f"{api_server_url}/webhooks/gitlab", method="POST", body={})

    assert (status, body) == (404, {"detail": "Unknown webhook"})


@pytest.fixture(scope="module")
def api_token(api_server_url) -> str:
    # tests/conftest.py sets the "admin" user's password.
    status, body = request_json(
        f"{api_server_url}/auth/token", method="POST", body={"username": "admin", "password": "admin"}
    )
    assert status == 201, body
    return body["access_token"]


def send_test_event(url: str, data: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "airflow_inbound_webhooks.send_test_event"]
        + ["--url", url, "--secret", SECRET, "--data", json.dumps(data)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_connection_form_is_served(api_server_url, api_token):
    status, hooks = request_json(f"{api_server_url}/ui/connections/hook_meta", token=api_token)

    assert status == 200
    hook = next(hook for hook in hooks if hook["connection_type"] == CONN_TYPE)
    assert hook["hook_name"] == HOOK_NAME
    if not CONNECTION_FORMS_AVAILABLE:
        assert not hook["extra_fields"]
        return
    assert list(hook["extra_fields"]) == [field.name for field in FIELDS]
    source_type = hook["extra_fields"]["source_type"]["schema"]
    if CHOICES_SHOWN:
        assert source_type["enum"] == ["hmac_json", "token_header", "github", "gitlab", "jira"]
    else:
        assert "enum" not in source_type
    assert hook["standard_fields"]["password"]["title"] == "Shared secret"
    assert all(hook["standard_fields"][field]["hidden"] for field in ("host", "port", "login"))


def test_connection_saved_by_the_form_receives_deliveries(api_server_url, api_token, session, subscribe):
    # The form saves every field, with null for those left empty.
    extra = {field.name: None for field in FIELDS} | {
        "source_type": "hmac_json",
        "signature_header": "X-Signature",
        "event_field": "event",
        "path_field": "service",
    }
    connection = {
        "connection_id": "webhook_demo",
        "conn_type": CONN_TYPE,
        "password": SECRET,
        "extra": json.dumps(extra),
    }
    status, body = request_json(
        f"{api_server_url}/api/v2/connections", method="POST", token=api_token, body=connection
    )
    assert status == 201, body
    subscribe("smoke_test_billing", "demo/billing/deployed")

    result = send_test_event(f"{api_server_url}/webhooks/demo", {"event": "deployed", "service": "billing"})

    assert result.returncode == 0, result.stdout + result.stderr
    assert list(session.scalars(select(AssetDagRunQueue.target_dag_id))) == ["smoke_test_billing"]


def test_readme_quick_start(api_server_url, session, subscribe, webhook_connection):
    webhook_connection(
        "demo",
        source_type="hmac_json",
        signature_header="X-Signature",
        event_field="event",
        path_field="service",
    )
    subscribe("smoke_test_billing", "demo/billing/deployed")

    result = send_test_event(f"{api_server_url}/webhooks/demo", {"event": "deployed", "service": "billing"})

    assert result.returncode == 0, result.stdout + result.stderr
    # The output the README shows, indented as in its list of steps.
    assert textwrap.indent(result.stdout, "   ") in (Path(__file__).parents[1] / "README.md").read_text()
    assert list(session.scalars(select(AssetDagRunQueue.target_dag_id))) == ["smoke_test_billing"]


def test_health_route_needs_no_login(api_server_url):
    assert request_json(f"{api_server_url}/webhooks/_health") == (200, {"status": "healthy"})


def test_deliveries_api_takes_an_api_token(api_server_url, api_token, subscribe, webhook_connection):
    webhook_connection("github")
    subscribe("web_deploy_preview", ASSET_NAME)
    assert post_github(api_server_url)[0] == 202

    status, body = request_json(f"{api_server_url}/webhooks/_deliveries/github", token=api_token)

    assert status == 200
    assert [delivery["outcome"] for delivery in body["deliveries"]] == ["accepted"]
    assert request_json(f"{api_server_url}/webhooks/_deliveries/github")[0] == 401


def test_deliveries_page_takes_the_ui_session_cookie(api_server_url, api_token, webhook_connection):
    # The UI frames the page, so the browser sends the session cookie, not a bearer token.
    webhook_connection("github")
    request = urllib.request.Request(
        f"{api_server_url}/webhooks/_ui/deliveries?webhook=github", headers={"Cookie": f"_token={api_token}"}
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == 200
        assert "Deliveries to github" in response.read().decode()
