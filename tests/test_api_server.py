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
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from airflow.models.asset import AssetDagRunQueue, AssetEvent
from airflow.models.connection import Connection
from sqlalchemy import select

from airflow_inbound_webhooks.app import SUPPORTS_TEAM_ATTRIBUTION

pytestmark = pytest.mark.usefixtures("clean_db")

SECRET = "s3cret"
ASSET_NAME = "github/my-org/web/pull_request/labeled/deploy-preview"
LABELED = {
    "action": "labeled",
    "label": {"name": "deploy-preview"},
    "repository": {"full_name": "my-org/web"},
}


def find_free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def api_server_url():
    port = find_free_port()
    url = f"http://127.0.0.1:{port}"
    log_path = Path(os.environ["AIRFLOW_HOME"]) / "api-server.log"
    with log_path.open("w") as log:
        # Its own process group, so stopping it also stops the uvicorn workers it starts.
        server = subprocess.Popen(
            [sys.executable, "-m", "airflow", "api-server", "--port", str(port)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
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
                conn_id=f"webhook_{name}", conn_type="generic", password=SECRET, extra=json.dumps(extra)
            )
        )
        session.commit()

    return add_connection


def test_signed_delivery_queues_subscribed_dag(api_server_url, session, subscribe, webhook_connection):
    webhook_connection("github")
    subscribe("web_deploy_preview", ASSET_NAME)

    status, body = post_github(api_server_url)

    assert (status, body["status"], list(body["asset_events"])) == (202, "accepted", [ASSET_NAME])
    assert [extra["source"] for extra in session.scalars(select(AssetEvent.extra))] == ["github"]
    assert list(session.scalars(select(AssetDagRunQueue.target_dag_id))) == ["web_deploy_preview"]


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

    assert status == (202 if SUPPORTS_TEAM_ATTRIBUTION else 503)


def test_readme_quick_start(api_server_url, session, subscribe, webhook_connection):
    webhook_connection(
        "demo",
        source_type="hmac_json",
        signature_header="X-Signature",
        event_field="event",
        path_field="service",
    )
    subscribe("smoke_test_billing", "demo/billing/deployed")
    script = Path(__file__).parents[1] / "examples" / "send_test_event.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--url",
            f"{api_server_url}/webhooks/demo",
            "--secret",
            SECRET,
            "--data",
            json.dumps({"event": "deployed", "service": "billing"}),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert '"status":"accepted"' in result.stdout
    assert list(session.scalars(select(AssetDagRunQueue.target_dag_id))) == ["smoke_test_billing"]
