# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Reading delivery records: a JSON API under ``/webhooks/_deliveries`` and a page for the Airflow UI.

Unlike ``POST /webhooks/<name>``, these routes need an Airflow login, from a bearer token or the UI's
session cookie, and the right to read the webhook's connection, which holds its secret.
"""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated, Any
from urllib.parse import quote, urlencode

from airflow.exceptions import AirflowNotFoundException
from airflow.sdk import Connection
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.security.utils import get_authorization_scheme_param
from sqlalchemy import and_, false, or_, select

from airflow_inbound_webhooks import configuration
from airflow_inbound_webhooks.app import CONN_ID_PREFIX, WEBHOOK_NAME_PATTERN, get_webhook_names

if TYPE_CHECKING:
    from sqlalchemy import ColumnElement

DEFAULT_LIMIT = 50
MAX_LIMIT = 500
# Payloads hold text that anyone can write, such as pull request titles: the page runs no script
# and loads nothing, and only the Airflow UI may frame it.
PAGE_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
}
PAGE_STYLE = """
:root { color-scheme: light dark; font-family: system-ui, sans-serif; font-size: 14px; }
body { margin: 1rem; }
table { border-collapse: collapse; width: 100%; }
th, td { border-bottom: 1px solid #8884; padding: 0.3rem 0.5rem; text-align: left; vertical-align: top; }
pre { overflow-x: auto; white-space: pre-wrap; }
"""


async def get_reader(request: Request) -> Any:
    """Return the Airflow user making the request, from a bearer token or the UI's session cookie."""
    from airflow.api_fastapi.core_api.security import get_user

    scheme, token = get_authorization_scheme_param(request.headers.get("Authorization"))
    credentials = HTTPAuthorizationCredentials(scheme=scheme, credentials=token) if scheme and token else None
    # Positionally: get_user's second parameter has a different name in different Airflow versions.
    return await get_user(request, None, credentials)


ReaderDep = Annotated[Any, Depends(get_reader)]


def get_webhook_team(name: str) -> str | None:
    """
    Return the team the webhook's connection names, the team its deliveries were attributed to.

    A webhook whose connection cannot be read is not shown at all, rather than checked as one with no
    team: airflow.sdk's Connection also reports a secrets backend it could not read as a missing
    connection.
    """
    try:
        conn = Connection.get(f"{CONN_ID_PREFIX}{name}")
    except AirflowNotFoundException:
        raise HTTPException(HTTPStatus.NOT_FOUND, "Unknown webhook") from None
    return conn.extra_dejson.get("team_name") or None


def can_read(user: Any, name: str, team: str | None) -> bool:
    """Return whether ``user`` may read the webhook's deliveries: whether they may read its connection."""
    from airflow.api_fastapi.app import get_auth_manager
    from airflow.api_fastapi.auth.managers.models.resource_details import ConnectionDetails

    details = ConnectionDetails(conn_id=f"{CONN_ID_PREFIX}{name}", team_name=team)
    return get_auth_manager().is_authorized_connection(method="GET", details=details, user=user)


def require_read(user: Any, name: str) -> str | None:
    """Refuse a reader who may not read the webhook's deliveries, and return the webhook's team."""
    if not WEBHOOK_NAME_PATTERN.fullmatch(name):
        raise HTTPException(HTTPStatus.NOT_FOUND, "Unknown webhook")
    team = get_webhook_team(name)
    if not can_read(user, name, team):
        raise HTTPException(HTTPStatus.FORBIDDEN, "Forbidden")
    return team


def as_utc(moment: datetime) -> datetime:
    # A time without a zone is taken as UTC, the zone the table stores times in.
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def of_webhook(name: str, team: str | None) -> ColumnElement[bool]:
    """Match a webhook's deliveries that were attributed to ``team``, the team the reader may read."""
    from airflow_inbound_webhooks.models import delivery_table

    columns = delivery_table.c
    return and_(columns.webhook == name, columns.team_name == team if team else columns.team_name.is_(None))


def list_rows(
    name: str, team: str | None, limit: int, before: datetime | None = None, before_id: str | None = None
) -> list[dict[str, Any]]:
    """
    Return a webhook's deliveries, newest first and without their payloads.

    ``before`` and ``before_id`` page back from the last delivery of a page, so deliveries received at
    the same time are neither skipped nor shown twice.
    """
    from airflow.utils.session import create_session

    from airflow_inbound_webhooks.models import delivery_table

    columns = delivery_table.c
    query = select(*(column for column in columns if column.name != "payload")).where(of_webhook(name, team))
    if before is not None:
        before = as_utc(before)
        query = query.where(
            or_(
                columns.received_at < before,
                and_(columns.received_at == before, columns.id < before_id) if before_id else false(),
            )
        )
    # Both descending, so the database can read the (webhook, received_at) index backwards.
    query = query.order_by(columns.received_at.desc(), columns.id.desc()).limit(limit)
    with create_session(scoped=False) as session:
        return [row._asdict() for row in session.execute(query)]


def find_row(name: str, team: str | None, delivery: str) -> dict[str, Any] | None:
    from airflow.utils.session import create_session

    from airflow_inbound_webhooks.models import delivery_table

    query = select(delivery_table).where(of_webhook(name, team), delivery_table.c.id == delivery)
    with create_session(scoped=False) as session:
        row = session.execute(query).one_or_none()
    return None if row is None else row._asdict()


router = APIRouter()


@router.get("/_deliveries/{name}")
def list_deliveries(
    name: str,
    user: ReaderDep,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    before: datetime | None = None,
    before_id: str | None = None,
) -> dict[str, Any]:
    """Return a webhook's deliveries, newest first, without payloads; ``before`` and ``before_id`` page back."""
    team = require_read(user, name)
    return {"deliveries": jsonable_encoder(list_rows(name, team, limit, before, before_id))}


@router.get("/_deliveries/{name}/{delivery}")
def get_delivery(name: str, delivery: str, user: ReaderDep) -> dict[str, Any]:
    """Return one delivery, with its payload."""
    team = require_read(user, name)
    if (row := find_row(name, team, delivery)) is None:
        raise HTTPException(HTTPStatus.NOT_FOUND, "No such delivery")
    return jsonable_encoder(row)


def escape(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def render_page(title: str, body: str) -> HTMLResponse:
    page = (
        f"<!doctype html><html><head><meta charset='utf-8'><title>{escape(title)}</title>"
        f"<style>{PAGE_STYLE}</style></head><body><h1>{escape(title)}</h1>{body}</body></html>"
    )
    return HTMLResponse(page, headers=PAGE_HEADERS)


def get_readable_webhooks(user: Any) -> list[str]:
    """Return the listed webhooks whose connections ``user`` may read."""
    names = []
    for name in sorted(get_webhook_names() - {configuration.ANY_WEBHOOK_NAME}):
        try:
            team = get_webhook_team(name)
        except HTTPException:
            continue
        if can_read(user, name, team):
            names.append(name)
    return names


@router.get("/_ui/deliveries", response_class=HTMLResponse)
def deliveries_page(
    user: ReaderDep, webhook: str | None = None, before: datetime | None = None, before_id: str | None = None
) -> HTMLResponse:
    """List the webhooks the user may read, or one webhook's latest deliveries."""
    if webhook is None:
        names = get_readable_webhooks(user)
        items = "".join(
            f"<li><a href='?{escape(urlencode({'webhook': name}))}'>{escape(name)}</a></li>" for name in names
        )
        # Webhooks that are no longer listed, or listed only through *, are reached by name.
        form = (
            "<form method='get'><label>Webhook <input name='webhook' required></label> "
            "<button>Show deliveries</button></form>"
        )
        return render_page("Webhook deliveries", (f"<ul>{items}</ul>" if names else "") + form)
    team = require_read(user, webhook)
    rows = list_rows(webhook, team, DEFAULT_LIMIT, before, before_id)
    lines = "".join(
        "<tr>"
        f"<td><a href='deliveries/{escape(quote(webhook))}/{escape(quote(row['id']))}'>"
        f"{escape(row['received_at'])}</a></td>"
        f"<td>{escape(row['outcome'])}</td><td>{escape(row['delivery_id'])}</td><td>{escape(row['path'])}</td>"
        f"<td>{'<br>'.join(escape(name) for name in row['asset_events'])}</td>"
        "</tr>"
        for row in rows
    )
    table = (
        "<table><tr><th>Received</th><th>Outcome</th><th>Delivery ID</th><th>Path</th>"
        f"<th>Asset events</th></tr>{lines}</table>"
    )
    older = ""
    if len(rows) == DEFAULT_LIMIT:
        last = rows[-1]
        query = urlencode(
            {"webhook": webhook, "before": last["received_at"].isoformat(), "before_id": last["id"]}
        )
        older = f"<p><a href='?{escape(query)}'>Older deliveries</a></p>"
    return render_page(f"Deliveries to {webhook}", (table + older) if rows else "<p>No deliveries yet.</p>")


@router.get("/_ui/deliveries/{name}/{delivery}", response_class=HTMLResponse)
def delivery_page(name: str, delivery: str, user: ReaderDep) -> HTMLResponse:
    """Show one delivery, with the names it offered and its payload."""
    team = require_read(user, name)
    if (row := find_row(name, team, delivery)) is None:
        raise HTTPException(HTTPStatus.NOT_FOUND, "No such delivery")
    fields = "".join(
        f"<tr><th>{escape(key)}</th><td>{escape(row[key])}</td></tr>"
        for key in (
            "received_at",
            "outcome",
            "delivery_id",
            "source_type",
            "path",
            "correlation_key",
            "team_name",
        )
    )
    candidates = "".join(f"<li>{escape(name)}</li>" for name in row["candidates"])
    asset_events = "".join(f"<li>{escape(name)}</li>" for name in row["asset_events"])
    payload = (
        "No payload is stored."
        if row["payload"] is None
        else json.dumps(row["payload"], indent=2, sort_keys=True)
    )
    body = (
        f"<p><a href='../../deliveries?{escape(urlencode({'webhook': name}))}'>"
        f"All deliveries to {escape(name)}</a></p>"
        f"<table>{fields}</table><h2>Asset events</h2><ul>{asset_events}</ul>"
        f"<h2>Names offered</h2><ul>{candidates}</ul><h2>Payload</h2><pre>{escape(payload)}</pre>"
    )
    return render_page(f"Delivery to {name}", body)
