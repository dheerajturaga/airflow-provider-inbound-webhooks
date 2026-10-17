# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""``airflow webhooks``: its commands, through the parser both ways of running them share."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
from unittest import mock

import pytest
from airflow.models.connection import Connection as ConnectionModel
from packaging.version import Version
from sqlalchemy import select

from airflow_inbound_webhooks import cli, send_test_event
from airflow_inbound_webhooks.models import delivery_table

pytestmark = pytest.mark.usefixtures("clean_db")

GITHUB_CONN = {"AIRFLOW_CONN_WEBHOOK_GITHUB": json.dumps({"conn_type": "generic", "password": "s3cret"})}


def get_delivery_ids(session) -> set[str]:
    return set(session.scalars(select(delivery_table.c.id)))


@mock.patch.dict(os.environ, GITHUB_CONN)
def test_check_reports_a_webhook_ready_for_deliveries(capsys):
    cli.main(["check", "github"])

    out = capsys.readouterr().out
    assert "Source type: github" in out
    assert "Webhook github is ready" in out


def test_check_finds_connections_stored_in_the_metadata_database(session, capsys):
    # airflow.sdk's Connection would not: it reads them only in the API server.
    session.add(ConnectionModel(conn_id="webhook_github", conn_type="inbound_webhook", password="s3cret"))
    session.commit()

    cli.main(["check", "github"])

    assert "Webhook github is ready" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("env", "expected_problem"),
    [
        pytest.param({}, "there is no connection webhook_github", id="no-connection"),
        pytest.param(
            {
                "AIRFLOW_CONN_WEBHOOK_GITHUB": json.dumps(
                    {"conn_type": "generic", "password": "s3cret", "extra": {"source_type": "bitbucket"}}
                )
            },
            "connection webhook_github is misconfigured: unknown source_type 'bitbucket'",
            id="misconfigured",
        ),
        pytest.param(
            {**GITHUB_CONN, "AIRFLOW__INBOUND_WEBHOOKS__WEBHOOK_NAMES": "gitlab"},
            "webhook_names does not list github",
            id="not-listed",
        ),
    ],
)
def test_check_fails_on_each_problem(capsys, env, expected_problem):
    with mock.patch.dict(os.environ, env), pytest.raises(SystemExit) as exit_info:
        cli.main(["check", "github"])

    assert exit_info.value.code == 1
    assert expected_problem in capsys.readouterr().err


def test_check_refuses_names_no_webhook_can_have():
    with pytest.raises(SystemExit, match="cannot be a webhook name"):
        cli.main(["check", "GitHub"])


def test_deliveries_are_listed_newest_first(add_delivery, capsys):
    now = datetime.now(tz=timezone.utc)
    older = add_delivery(received_at=now - timedelta(minutes=1))
    newer = add_delivery(received_at=now)

    cli.main(["deliveries", "github", "--output", "json"])

    assert [delivery["id"] for delivery in json.loads(capsys.readouterr().out)] == [newer, older]


def test_one_delivery_is_shown_with_its_payload(add_delivery, capsys):
    delivery = add_delivery()

    cli.main(["deliveries", "github", "--id", delivery])

    assert json.loads(capsys.readouterr().out)["payload"] == {"ref": "refs/heads/main"}


def test_unknown_delivery_fails(add_delivery):
    with pytest.raises(SystemExit, match="has no delivery"):
        cli.main(["deliveries", "github", "--id", "no-such-delivery"])


@pytest.fixture
def old_and_new_deliveries(add_delivery):
    now = datetime.now(tz=timezone.utc)
    return add_delivery(received_at=now - timedelta(days=10)), add_delivery(
        received_at=now - timedelta(days=1)
    )


def test_clean_deletes_only_deliveries_older_than_the_retention(session, old_and_new_deliveries, capsys):
    old, new = old_and_new_deliveries

    cli.main(["clean", "--older-than", "7", "--yes"])

    assert get_delivery_ids(session) == {new}
    assert "Deleted 1 deliveries" in capsys.readouterr().out


@mock.patch.dict(os.environ, {"AIRFLOW__INBOUND_WEBHOOKS__DELIVERY_RETENTION_DAYS": "0"})
def test_clean_keeps_the_configured_retention_by_default(session, old_and_new_deliveries):
    cli.main(["clean", "--yes"])

    assert get_delivery_ids(session) == set()


def test_clean_dry_run_deletes_nothing(session, old_and_new_deliveries, capsys):
    cli.main(["clean", "--older-than", "7", "--dry-run"])

    assert get_delivery_ids(session) == set(old_and_new_deliveries)
    assert "1 deliveries were received before" in capsys.readouterr().out


def test_clean_asks_before_deleting(session, old_and_new_deliveries):
    with mock.patch("builtins.input", return_value="n"), pytest.raises(SystemExit, match="Nothing deleted"):
        cli.main(["clean", "--older-than", "7"])

    assert get_delivery_ids(session) == set(old_and_new_deliveries)


def test_clean_without_a_terminal_to_ask_deletes_nothing(session, old_and_new_deliveries):
    # As from a schedule, without --yes.
    with (
        mock.patch("builtins.input", side_effect=EOFError),
        pytest.raises(SystemExit, match="pass --yes"),
    ):
        cli.main(["clean", "--older-than", "7"])

    assert get_delivery_ids(session) == set(old_and_new_deliveries)


@pytest.mark.parametrize("limit", ["0", "-1", "501", "ten"])
def test_deliveries_limit_is_bounded(limit, capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["deliveries", "github", "--limit", limit])

    assert exit_info.value.code == 2
    assert "must be a number from 1 to 500" in capsys.readouterr().err


def test_clean_deletes_in_batches(session, add_delivery):
    long_ago = datetime.now(tz=timezone.utc) - timedelta(days=30)
    for _ in range(5):
        add_delivery(received_at=long_ago)

    with mock.patch.object(cli, "CLEAN_BATCH_SIZE", 2):
        cli.main(["clean", "--older-than", "7", "--yes"])

    assert get_delivery_ids(session) == set()


def test_send_test_takes_the_options_of_the_standalone_sender():
    # send_test_event keeps a parser of its own, so it also runs where only the standard library is.
    argv = ["--url", "http://localhost:8080/webhooks/demo", "--secret", "s", "--data", "{}"]
    argv += ["--signature-header", "X-Hub-Signature-256", "--signature-prefix", "sha256=", "--header", "A: b"]

    parsed = vars(cli.build_parser().parse_args(["send-test", *argv]))

    assert {
        key: parsed[key]
        for key in ("url", "secret", "data", "signature_header", "signature_prefix", "header")
    } == vars(send_test_event.build_parser().parse_args(argv))


@pytest.mark.skipif(
    Version(version("apache-airflow")) < Version("3.2"), reason="Airflow 3.1 reads no provider cli"
)
def test_airflow_offers_the_commands():
    result = subprocess.run(
        [sys.executable, "-m", "airflow", "webhooks", "--help"], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
    assert all(command in result.stdout for command in ("check", "deliveries", "clean", "send-test"))


def test_commands_run_without_airflow_webhooks():
    result = subprocess.run(
        [sys.executable, "-m", "airflow_inbound_webhooks.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "deliveries" in result.stdout
