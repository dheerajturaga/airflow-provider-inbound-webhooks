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
from typing import Annotated, Any
from urllib.parse import quote, urlencode

from airflow.exceptions import AirflowNotFoundException
from airflow.sdk import Connection
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.security.utils import get_authorization_scheme_param
from sqlalchemy import select

from airflow_inbound_webhooks import configuration
from airflow_inbound_webhooks.app import CONN_ID_PREFIX, WEBHOOK_NAME_PATTERN, get_webhook_names

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
    """Return the team the webhook's connection names, the team its events are attributed to."""
    try:
        conn = Connection.get(f"{CONN_ID_PREFIX}{name}")
    except AirflowNotFoundException:
        return None
    return conn.extra_dejson.get("team_name") or None


def can_read(user: Any, name: str) -> bool:
    """Return whether ``user`` may read the webhook's deliveries: whether they may read its connection."""
    from airflow.api_fastapi.app import get_auth_manager
    from airflow.api_fastapi.auth.managers.models.resource_details import ConnectionDetails

    details = ConnectionDetails(conn_id=f"{CONN_ID_PREFIX}{name}", team_name=get_webhook_team(name))
    return get_auth_manager().is_authorized_connection(method="GET", details=details, user=user)


def require_read(user: Any, name: str) -> None:
    if not WEBHOOK_NAME_PATTERN.fullmatch(name):
        raise HTTPException(HTTPStatus.NOT_FOUND, "Unknown webhook")
    if not can_read(user, name):
        raise HTTPException(HTTPStatus.FORBIDDEN, "Forbidden")


def as_utc(moment: datetime) -> datetime:
    # A time without a zone is taken as UTC, the zone the table stores times in.
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def list_rows(name: str, limit: int, before: datetime | None) -> list[dict[str, Any]]:
    """Return a webhook's deliveries, newest first and without their payloads."""
    from airflow.utils.session import create_session

    from airflow_inbound_webhooks.models import delivery_table

    query = select(*(column for column in delivery_table.c if column.name != "payload")).where(
        delivery_table.c.webhook == name
    )
    if before is not None:
        query = query.where(delivery_table.c.received_at < as_utc(before))
    query = query.order_by(delivery_table.c.received_at.desc(), delivery_table.c.id).limit(limit)
    with create_session(scoped=False) as session:
        return [row._asdict() for row in session.execute(query)]


def find_row(name: str, delivery: str) -> dict[str, Any] | None:
    from airflow.utils.session import create_session

    from airflow_inbound_webhooks.models import delivery_table

    query = select(delivery_table).where(delivery_table.c.webhook == name, delivery_table.c.id == delivery)
    with create_session(scoped=False) as session:
        row = session.execute(query).one_or_none()
    return None if row is None else row._asdict()


def get_known_webhooks() -> list[str]:
    """Return the listed webhooks and those with stored deliveries, which can outlive their listing."""
    from airflow.utils.session import create_session

    from airflow_inbound_webhooks.models import delivery_table

    with create_session(scoped=False) as session:
        stored = set(session.scalars(select(delivery_table.c.webhook).distinct()))
    return sorted((get_webhook_names() - {configuration.ANY_WEBHOOK_NAME}) | stored)


router = APIRouter()


@router.get("/_deliveries/{name}")
def list_deliveries(
    name: str,
    user: ReaderDep,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    before: datetime | None = None,
) -> dict[str, Any]:
    """Return a webhook's deliveries, newest first, without payloads; ``before`` pages back."""
    require_read(user, name)
    return {"deliveries": jsonable_encoder(list_rows(name, limit, before))}


@router.get("/_deliveries/{name}/{delivery}")
def get_delivery(name: str, delivery: str, user: ReaderDep) -> dict[str, Any]:
    """Return one delivery, with its payload."""
    require_read(user, name)
    if (row := find_row(name, delivery)) is None:
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


@router.get("/_ui/deliveries", response_class=HTMLResponse)
def deliveries_page(
    user: ReaderDep, webhook: str | None = None, before: datetime | None = None
) -> HTMLResponse:
    """List the webhooks the user may read, or one webhook's latest deliveries."""
    if webhook is None:
        names = [name for name in get_known_webhooks() if can_read(user, name)]
        items = "".join(
            f"<li><a href='?{escape(urlencode({'webhook': name}))}'>{escape(name)}</a></li>" for name in names
        )
        return render_page(
            "Webhook deliveries", f"<ul>{items}</ul>" if names else "<p>No webhooks to show.</p>"
        )
    require_read(user, webhook)
    rows = list_rows(webhook, DEFAULT_LIMIT, before)
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
        query = urlencode({"webhook": webhook, "before": rows[-1]["received_at"].isoformat()})
        older = f"<p><a href='?{escape(query)}'>Older deliveries</a></p>"
    return render_page(f"Deliveries to {webhook}", (table + older) if rows else "<p>No deliveries yet.</p>")


@router.get("/_ui/deliveries/{name}/{delivery}", response_class=HTMLResponse)
def delivery_page(name: str, delivery: str, user: ReaderDep) -> HTMLResponse:
    """Show one delivery, with the names it offered and its payload."""
    require_read(user, name)
    if (row := find_row(name, delivery)) is None:
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
