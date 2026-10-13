# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""FastAPI app that records verified webhook deliveries as events on the assets Dags subscribe to."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from http import HTTPStatus
from typing import Annotated, Any

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.assets.manager import asset_manager
from airflow.exceptions import AirflowNotFoundException
from airflow.models.asset import AssetModel
from airflow.sdk import Connection
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from sqlalchemy import select

from airflow_inbound_webhooks.naming import build_candidate_names
from airflow_inbound_webhooks.sources import SOURCE_TYPES, WebhookSource, WebhookVerificationError

log = logging.getLogger(__name__)

CONN_ID_PREFIX = "webhook_"
DEFAULT_MAX_BODY_BYTES = 1024 * 1024
WEBHOOK_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


@dataclass(frozen=True)
class ConfiguredWebhook:
    """A webhook resolved from its ``webhook_<name>`` connection, the only configuration it has."""

    name: str
    source: WebhookSource
    secret: str
    team_name: str | None
    max_body_bytes: int


def get_configured_webhook(name: str) -> ConfiguredWebhook:
    """
    Resolve ``POST /webhooks/<name>`` from connection ``webhook_<name>``.

    The password is the shared secret. The extra may set ``source_type`` (defaults to ``<name>``,
    so ``webhook_github`` needs nothing else), source options, ``team_name`` and ``max_body_bytes``.

    Connections stored in the metadata database resolve only because ``airflow api-server`` runs as a
    server context; environment variables and external secrets backends resolve in any process.
    """
    if not WEBHOOK_NAME_PATTERN.fullmatch(name):
        raise HTTPException(HTTPStatus.NOT_FOUND, "Unknown webhook")
    conn_id = f"{CONN_ID_PREFIX}{name}"
    try:
        conn = Connection.get(conn_id)
    except AirflowNotFoundException:
        raise HTTPException(HTTPStatus.NOT_FOUND, "Unknown webhook") from None
    extra = conn.extra_dejson
    try:
        source_type = extra.get("source_type", name)
        if source_type not in SOURCE_TYPES:
            raise ValueError(f"unknown source_type {source_type!r}; expected one of {sorted(SOURCE_TYPES)}")
        if not conn.password:
            raise ValueError("the password, which holds the shared secret, is empty")
        return ConfiguredWebhook(
            name=name,
            source=SOURCE_TYPES[source_type].from_extra(extra),
            secret=conn.password,
            team_name=extra.get("team_name"),
            max_body_bytes=int(extra.get("max_body_bytes", DEFAULT_MAX_BODY_BYTES)),
        )
    except ValueError as e:
        log.error("Webhook connection %s is misconfigured: %s", conn_id, e)
        raise HTTPException(HTTPStatus.SERVICE_UNAVAILABLE, "Webhook is misconfigured") from e


WebhookDep = Annotated[ConfiguredWebhook, Depends(get_configured_webhook)]


async def read_limited_body(request: Request, webhook: WebhookDep) -> bytes:
    too_large = HTTPException(
        HTTPStatus.REQUEST_ENTITY_TOO_LARGE, f"Body exceeds {webhook.max_body_bytes} bytes"
    )
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > webhook.max_body_bytes:
        raise too_large
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > webhook.max_body_bytes:
            raise too_large
    return bytes(body)


router = APIRouter()


@router.post("/{name}", status_code=HTTPStatus.ACCEPTED)
def receive_webhook(
    request: Request,
    webhook: WebhookDep,
    body: Annotated[bytes, Depends(read_limited_body)],
    session: SessionDep,
) -> dict[str, Any]:
    try:
        webhook.source.verify(request.headers, body, webhook.secret)
    except WebhookVerificationError as e:
        log.warning("Rejected delivery for webhook %s: %s", webhook.name, e)
        raise HTTPException(HTTPStatus.UNAUTHORIZED, "Invalid webhook credentials") from e
    try:
        event = webhook.source.parse(request.headers, body)
    except ValueError as e:
        raise HTTPException(HTTPStatus.BAD_REQUEST, str(e)) from e

    candidates = build_candidate_names(webhook.name, event)
    subscribed = session.scalars(
        select(AssetModel).where(AssetModel.name.in_(candidates), AssetModel.active.has())
    ).all()
    extra = {
        "source": webhook.name,
        "path": event.path,
        "delivery_id": event.delivery_id,
        "payload": event.payload,
    }
    asset_events: dict[str, int] = {}
    for asset_model in subscribed:
        asset_event = asset_manager.register_asset_change(
            asset=asset_model,
            extra=extra,
            source_is_api=True,
            api_user_teams={webhook.team_name} if webhook.team_name else set(),
            session=session,
        )
        if asset_event is not None:
            asset_events[asset_model.name] = asset_event.id
    return {
        "status": "accepted" if asset_events else "no_subscribers",
        "asset_events": asset_events,
        "candidates": candidates,
    }


def build_app() -> FastAPI:
    app = FastAPI(title="Airflow webhooks")
    app.include_router(router)
    return app
