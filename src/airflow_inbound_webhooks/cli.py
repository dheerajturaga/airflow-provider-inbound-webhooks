# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
``airflow webhooks``: check a webhook, read its deliveries, delete old ones and send a test delivery.

Airflow 3.2 and later add the commands through ``cli`` in the provider metadata. Airflow 3.1 does not
read that key, so ``python -m airflow_inbound_webhooks.cli`` runs the same commands there.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.cli.cli_config import ARG_OUTPUT, ARG_YES, ActionCommand, Arg, GroupCommand, lazy_load_command
from airflow.configuration import conf

from airflow_inbound_webhooks import configuration

# Rows deleted per transaction, so cleaning a large table holds no long lock and keeps its progress.
CLEAN_BATCH_SIZE = 1000
# As for the deliveries API: SQLite takes a negative limit as none, PostgreSQL and MySQL refuse it.
MAX_LIMIT = 500


def read_limit(value: str) -> int:
    if not (value.isdigit() and 1 <= int(value) <= MAX_LIMIT):
        raise argparse.ArgumentTypeError(f"must be a number from 1 to {MAX_LIMIT}")
    return int(value)


ARG_NAME = Arg(("name",), help="The webhook's name, as in /webhooks/<name>")
ARG_LIMIT = Arg(
    ("--limit",),
    type=read_limit,
    default=20,
    help=f"How many of the latest deliveries to show, 1 to {MAX_LIMIT} (default 20)",
)
ARG_DELIVERY = Arg(("--id",), dest="delivery", help="Show this delivery as JSON, with its payload")
ARG_OLDER_THAN = Arg(
    ("--older-than",),
    type=int,
    metavar="DAYS",
    help=f"Delete deliveries older than DAYS days (default: [{configuration.SECTION}] "
    f"{configuration.DELIVERY_RETENTION})",
)
ARG_DRY_RUN = Arg(("--dry-run",), action="store_true", help="Count the deliveries to delete, and delete none")
# The options of send_test_event's own parser, which stays free of Airflow imports to run on its own.
ARG_URL = Arg(("--url",), required=True, help="Webhook URL, e.g. http://localhost:8080/webhooks/demo")
ARG_SECRET = Arg(("--secret",), required=True, help="Shared secret, the webhook connection's password")
ARG_DATA = Arg(("--data",), required=True, help="JSON body to send")
ARG_SIGNATURE_HEADER = Arg(
    ("--signature-header",), default="X-Signature", help="Header carrying the signature"
)
ARG_SIGNATURE_PREFIX = Arg(
    ("--signature-prefix",), default="", help="Prefix before the hex digest, e.g. sha256="
)
ARG_HEADER = Arg(
    ("--header",), action="append", default=[], metavar="'NAME: VALUE'", help="Extra header; repeatable"
)

WEBHOOKS_COMMANDS = (
    ActionCommand(
        name="check",
        help="Check that a webhook is served, and that its connection and the delivery table are ready",
        func=lazy_load_command("airflow_inbound_webhooks.cli.check"),
        args=(ARG_NAME,),
    ),
    ActionCommand(
        name="deliveries",
        help="Show a webhook's latest deliveries, or one delivery with its payload",
        func=lazy_load_command("airflow_inbound_webhooks.cli.deliveries"),
        args=(ARG_NAME, ARG_LIMIT, ARG_DELIVERY, ARG_OUTPUT),
    ),
    ActionCommand(
        name="clean",
        help="Delete delivery records older than the retention period",
        func=lazy_load_command("airflow_inbound_webhooks.cli.clean"),
        args=(ARG_OLDER_THAN, ARG_DRY_RUN, ARG_YES),
    ),
    ActionCommand(
        name="send-test",
        help="Send a signed test delivery to a webhook, as a service would",
        func=lazy_load_command("airflow_inbound_webhooks.cli.send_test"),
        args=(ARG_URL, ARG_SECRET, ARG_DATA, ARG_SIGNATURE_HEADER, ARG_SIGNATURE_PREFIX, ARG_HEADER),
    ),
)


def get_webhooks_cli_commands() -> list[GroupCommand]:
    """Return ``airflow webhooks``, which Airflow 3.2 and later find in the provider metadata."""
    return [
        GroupCommand(
            name="webhooks",
            help="Inbound webhooks: check them, read their deliveries and delete old ones",
            subcommands=WEBHOOKS_COMMANDS,
        )
    ]


def check(args: argparse.Namespace) -> None:
    """Report how deliveries to a webhook would be served, and exit with 1 if they would fail."""
    from airflow.exceptions import AirflowNotFoundException
    from airflow.models.connection import Connection
    from airflow.utils.session import create_session
    from fastapi import HTTPException

    from airflow_inbound_webhooks.app import (
        CONN_ID_PREFIX,
        WEBHOOK_NAME_PATTERN,
        configure_webhook,
        get_webhook_names,
    )
    from airflow_inbound_webhooks.db import WebhooksDBManager

    if not WEBHOOK_NAME_PATTERN.fullmatch(args.name):
        sys.exit(f"{args.name!r} cannot be a webhook name: use lowercase letters, digits, - and _")
    problems = []
    names = get_webhook_names()
    if args.name not in names and configuration.ANY_WEBHOOK_NAME not in names:
        problems.append(
            f"[{configuration.SECTION}] {configuration.WEBHOOK_NAMES} does not list {args.name}, so its "
            "deliveries are answered with 404"
        )
    conn_id = f"{CONN_ID_PREFIX}{args.name}"
    webhook = None
    try:
        # The core model, not airflow.sdk's Connection, which reads the metadata database only in the API
        # server.
        webhook = configure_webhook(args.name, Connection.get_connection_from_secrets(conn_id))
    except AirflowNotFoundException:
        problems.append(f"there is no connection {conn_id}")
    except HTTPException as e:
        problems.append(f"connection {conn_id} is misconfigured: {e.__cause__}")
    with create_session() as session:
        if not WebhooksDBManager(session).check_migration():
            problems.append("the delivery table is missing or out of date: run airflow db migrate")
    if webhook is not None:
        print(f"Source type: {webhook.source_type}")
        print(f"Team: {webhook.team_name or 'none'}")
        print(f"Stored payload: {webhook.store_payload}")
        print(f"Maximum body size: {webhook.max_body_bytes} bytes")
    for problem in problems:
        print(f"Problem: {problem}", file=sys.stderr)
    if problems:
        sys.exit(1)
    print(f"Webhook {args.name} is ready to receive deliveries at /webhooks/{args.name}")


def deliveries(args: argparse.Namespace) -> None:
    """Print a webhook's latest deliveries, or one delivery, with its payload, as JSON."""
    from airflow.cli.simple_table import AirflowConsole
    from fastapi.encoders import jsonable_encoder

    from airflow_inbound_webhooks.deliveries import find_row, list_rows

    # The command line reads the database itself, so it shows every team's deliveries.
    if args.delivery is not None:
        if (row := find_row(args.name, None, args.delivery, every_team=True)) is None:
            sys.exit(f"Webhook {args.name} has no delivery {args.delivery}")
        print(json.dumps(jsonable_encoder(row), indent=2, sort_keys=True))
        return
    rows = jsonable_encoder(list_rows(args.name, None, args.limit, every_team=True))
    AirflowConsole().print_as(
        data=rows, output=args.output, mapper=summarize if args.output == "table" else None
    )


def summarize(row: dict[str, Any]) -> dict[str, Any]:
    """Return a delivery's fields that fit a table row."""
    return {
        "id": row["id"],
        "received_at": row["received_at"],
        "outcome": row["outcome"],
        "delivery_id": row["delivery_id"],
        "path": row["path"],
        "asset_events": ", ".join(row["asset_events"]),
    }


def clean(args: argparse.Namespace) -> None:
    """Delete delivery records received before the retention period, in batches."""
    from airflow.utils.session import create_session
    from sqlalchemy import delete, func, select

    from airflow_inbound_webhooks.models import delivery_table

    days = args.older_than
    if days is None:
        days = conf.getint(
            configuration.SECTION,
            configuration.DELIVERY_RETENTION,
            fallback=configuration.DEFAULT_DELIVERY_RETENTION_DAYS,
        )
    if days < 0:
        sys.exit("The retention period must be at least 0 days")
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    expired = delivery_table.c.received_at < cutoff
    with create_session(scoped=False) as session:
        count = session.scalar(select(func.count()).select_from(delivery_table).where(expired))
    if args.dry_run or not count:
        print(f"{count} deliveries were received before {cutoff.isoformat()}; none deleted")
        return
    if not args.yes:
        try:
            answer = input(f"Delete {count} deliveries received before {cutoff.isoformat()}? [y/N] ")
        except EOFError:
            # No terminal to ask, as when it runs on a schedule.
            sys.exit("Nothing deleted: pass --yes to delete without being asked")
        if answer.strip().lower() not in ("y", "yes"):
            sys.exit("Nothing deleted")
    deleted = 0
    while True:
        with create_session(scoped=False) as session:
            ids = list(session.scalars(select(delivery_table.c.id).where(expired).limit(CLEAN_BATCH_SIZE)))
            if ids:
                session.execute(delete(delivery_table).where(delivery_table.c.id.in_(ids)))
        if not ids:
            break
        deleted += len(ids)
    print(f"Deleted {deleted} deliveries received before {cutoff.isoformat()}")


def send_test(args: argparse.Namespace) -> None:
    from airflow_inbound_webhooks import send_test_event

    if send_test_event.run(args):
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    """Return a parser for the commands, for where ``airflow webhooks`` is not available."""
    parser = argparse.ArgumentParser(prog="python -m airflow_inbound_webhooks.cli", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in WEBHOOKS_COMMANDS:
        subparser = subparsers.add_parser(command.name, help=command.help)
        for arg in command.args:
            arg.add_to_parser(subparser)
        subparser.set_defaults(func=command.func)
    return parser


def main(argv: list[str] | None = None) -> None:
    """Run a command where ``airflow webhooks`` is not available: Airflow 3.1 reads no provider ``cli``."""
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
