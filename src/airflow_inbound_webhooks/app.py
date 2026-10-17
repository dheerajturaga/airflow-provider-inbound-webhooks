# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""FastAPI app that records verified webhook deliveries as events on the assets Dags subscribe to."""

from __future__ import annotations

import functools
import inspect
import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowNotFoundException
from airflow.sdk import Connection
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from airflow_inbound_webhooks import configuration
from airflow_inbound_webhooks.naming import build_candidate_names
from airflow_inbound_webhooks.sources import SOURCE_TYPES, WebhookSource, WebhookVerificationError

if TYPE_CHECKING:
    from sqlalchemy.engine import Row
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

CONN_ID_PREFIX = "webhook_"
DEFAULT_MAX_BODY_BYTES = 1024 * 1024
WEBHOOK_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
# The length of the delivery table's delivery_id column.
MAX_DELIVERY_ID_LENGTH = 255
# register_asset_change accepts and drops unknown keyword arguments, so an argument a later Airflow
# renamed would be lost silently instead of failing. The team arguments only exist from Airflow 3.3.
REQUIRED_ASSET_CHANGE_ARGUMENTS = frozenset({"asset", "extra", "session"})
TEAM_ATTRIBUTION_ARGUMENTS = frozenset({"source_is_api", "api_user_teams"})


@functools.cache
def get_asset_change_arguments() -> frozenset[str]:
    """
    Return the arguments ``register_asset_change`` names, failing if one the endpoint needs is gone.

    This inspects the ``AssetManager`` Airflow ships rather than the configured ``asset_manager``, so a
    ``[core] asset_manager_class`` that forwards ``**kwargs`` to it keeps working.
    """
    from airflow.assets.manager import AssetManager

    named = frozenset(inspect.signature(AssetManager.register_asset_change).parameters)
    if missing := REQUIRED_ASSET_CHANGE_ARGUMENTS - named:
        raise RuntimeError(
            f"AssetManager.register_asset_change no longer takes {sorted(missing)}; "
            "this Airflow version is not supported"
        )
    return named


def supports_team_attribution() -> bool:
    return get_asset_change_arguments() >= TEAM_ATTRIBUTION_ARGUMENTS


@dataclass(frozen=True)
class ConfiguredWebhook:
    """A webhook resolved from its ``webhook_<name>`` connection, the only configuration it has."""

    name: str
    source_type: str
    source: WebhookSource
    secret: str
    team_name: str | None
    max_body_bytes: int


def resolve_webhook(name: str) -> ConfiguredWebhook:
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
    # The connection form saves fields left empty as null or "", which mean "not set".
    extra = {key: value for key, value in conn.extra_dejson.items() if value not in (None, "")}
    try:
        source_type = extra.get("source_type", name)
        if source_type not in SOURCE_TYPES:
            raise ValueError(f"unknown source_type {source_type!r}; expected one of {sorted(SOURCE_TYPES)}")
        if not conn.password:
            raise ValueError("the password, which holds the shared secret, is empty")
        if extra.get("team_name") and not supports_team_attribution():
            raise ValueError("team_name needs Airflow 3.3 or later to attribute events to a team")
        # The connection form enforces this, but connections from JSON or environment variables skip it.
        if (max_body_bytes := int(extra.get("max_body_bytes", DEFAULT_MAX_BODY_BYTES))) < 1:
            raise ValueError("max_body_bytes must be at least 1")
        return ConfiguredWebhook(
            name=name,
            source_type=source_type,
            source=SOURCE_TYPES[source_type].from_extra(extra),
            secret=conn.password,
            team_name=extra.get("team_name"),
            max_body_bytes=max_body_bytes,
        )
    except ValueError as e:
        log.error("Webhook connection %s is misconfigured: %s", conn_id, e)
        raise HTTPException(HTTPStatus.SERVICE_UNAVAILABLE, "Webhook is misconfigured") from e


# Each listed webhook's resolution, or the HTTPException it raised, with its time.monotonic() expiry.
resolved_webhooks: dict[str, tuple[float, ConfiguredWebhook | HTTPException]] = {}
# One per listed webhook, held while resolving it, so the requests that find its entry expired make one
# lookup, not one each, and a slow lookup holds up only its own webhook. Only listed names get one.
resolve_locks: dict[str, threading.Lock] = {}


def get_webhook_names() -> frozenset[str]:
    value = conf.get(configuration.SECTION, configuration.WEBHOOK_NAMES, fallback="")
    return frozenset(filter(None, (name.strip() for name in value.split(","))))


@functools.cache
def warn_no_webhook_names() -> None:
    # Once per process: logging every refused request would let anyone flood the log.
    log.warning(
        "[%s] %s is not set, so no webhook is served; list the webhook names to serve",
        configuration.SECTION,
        configuration.WEBHOOK_NAMES,
    )


def get_cached_resolution(name: str) -> ConfiguredWebhook | HTTPException | None:
    expires_at, resolution = resolved_webhooks.get(name, (0.0, None))
    return resolution if time.monotonic() < expires_at else None


def get_configured_webhook(name: str) -> ConfiguredWebhook:
    """
    Resolve ``POST /webhooks/<name>``, reading its connection at most once per cache TTL.

    Requests are unauthenticated, so a name not in ``webhook_names`` is refused without a lookup, and
    neither a failed lookup nor a failed verification reads the connection again before the TTL is
    up. Otherwise anyone could make every request cost a call to the secrets backend.
    """
    names = get_webhook_names()
    if configuration.ANY_WEBHOOK_NAME in names:
        # Any name can be looked up, so a cache would keep an entry for every name anyone sends.
        return resolve_webhook(name)
    if name not in names:
        if not names:
            warn_no_webhook_names()
        # Debug only, and repr: anyone can send any name.
        log.debug(
            "Refused webhook %r: not in [%s] %s", name, configuration.SECTION, configuration.WEBHOOK_NAMES
        )
        raise HTTPException(HTTPStatus.NOT_FOUND, "Unknown webhook")
    ttl = conf.getint(
        configuration.SECTION,
        configuration.CONNECTION_CACHE_TTL,
        fallback=configuration.DEFAULT_CONNECTION_CACHE_TTL,
    )
    if ttl <= 0:
        return resolve_webhook(name)
    if (resolution := get_cached_resolution(name)) is None:
        # setdefault is atomic, so concurrent first requests for a name share one lock.
        with resolve_locks.setdefault(name, threading.Lock()):
            if (resolution := get_cached_resolution(name)) is None:
                try:
                    resolution = resolve_webhook(name)
                except HTTPException as e:
                    resolution = e
                resolved_webhooks[name] = (time.monotonic() + ttl, resolution)
    if isinstance(resolution, HTTPException):
        # A new exception each time: raising the cached one again would grow its traceback.
        raise HTTPException(resolution.status_code, resolution.detail)
    return resolution


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


def find_delivery(session: Session, webhook_name: str, delivery_id: str | None) -> Row[Any] | None:
    """Return the stored delivery that a sender's delivery ID repeats, if any."""
    from airflow_inbound_webhooks.models import delivery_table

    if delivery_id is None:
        return None
    return session.execute(
        select(delivery_table.c.asset_events, delivery_table.c.candidates).where(
            delivery_table.c.webhook == webhook_name, delivery_table.c.delivery_id == delivery_id
        )
    ).one_or_none()


def build_response(asset_events: dict[str, int], candidates: list[str]) -> dict[str, Any]:
    return {
        "status": "accepted" if asset_events else "no_subscribers",
        "asset_events": asset_events,
        "candidates": candidates,
    }


router = APIRouter()


# Also with a trailing slash: otherwise the router answers it with a redirect, which senders such as
# GitHub do not follow, so a URL pasted with a slash would fail every delivery.
@router.post("/{name}/", status_code=HTTPStatus.ACCEPTED)
@router.post("/{name}", status_code=HTTPStatus.ACCEPTED)
def receive_webhook(
    request: Request,
    webhook: WebhookDep,
    body: Annotated[bytes, Depends(read_limited_body)],
) -> dict[str, Any]:
    # Imported here rather than at module level: Airflow imports plugins in every process, including
    # each task run, and only the API server needs these.
    from airflow.assets.manager import asset_manager
    from airflow.models.asset import AssetModel
    from airflow.utils.session import create_session

    from airflow_inbound_webhooks.models import EVENTS_RECORDED, NO_SUBSCRIBERS, delivery_table

    try:
        webhook.source.verify(request.headers, body, webhook.secret)
    except WebhookVerificationError as e:
        log.warning("Rejected delivery for webhook %s: %s", webhook.name, e)
        raise HTTPException(HTTPStatus.UNAUTHORIZED, "Invalid webhook credentials") from e
    try:
        event = webhook.source.parse(request.headers, body)
    except ValueError as e:
        raise HTTPException(HTTPStatus.BAD_REQUEST, str(e)) from e
    if event.delivery_id is not None and len(event.delivery_id) > MAX_DELIVERY_ID_LENGTH:
        raise HTTPException(
            HTTPStatus.BAD_REQUEST, f"Delivery ID is longer than {MAX_DELIVERY_ID_LENGTH} characters"
        )

    candidates = build_candidate_names(webhook.name, event)
    extra = {
        "source": webhook.name,
        "path": event.path,
        "delivery_id": event.delivery_id,
        "payload": event.payload,
    }
    team_arguments: dict[str, Any] = (
        {"source_is_api": True, "api_user_teams": {webhook.team_name} if webhook.team_name else set()}
        if supports_team_attribution()
        else {}
    )
    asset_events: dict[str, int] = {}
    try:
        # Leaving the block commits, so a delivery is only acknowledged once its events and its row are
        # stored, which is also what lets a retry find the row.
        with create_session(scoped=False) as session:
            if (original := find_delivery(session, webhook.name, event.delivery_id)) is not None:
                log.debug("Webhook %s: delivery %r was recorded before", webhook.name, event.delivery_id)
                return build_response(original.asset_events, original.candidates)
            subscribed = session.scalars(
                select(AssetModel).where(AssetModel.name.in_(candidates), AssetModel.active.has())
            ).all()
            for asset_model in subscribed:
                asset_event = asset_manager.register_asset_change(
                    asset=asset_model, extra=extra, session=session, **team_arguments
                )
                if asset_event is not None:
                    asset_events[asset_model.name] = asset_event.id
            session.execute(
                delivery_table.insert().values(
                    id=str(uuid.uuid4()),
                    webhook=webhook.name,
                    delivery_id=event.delivery_id,
                    received_at=datetime.now(tz=timezone.utc),
                    source_type=webhook.source_type,
                    path=event.path,
                    candidates=candidates,
                    asset_events=asset_events,
                    outcome=EVENTS_RECORDED if asset_events else NO_SUBSCRIBERS,
                    team_name=webhook.team_name,
                    # A delivery no Dag subscribes to keeps its names, for troubleshooting, but not its
                    # payload, so a sender that sends every event does not fill the table.
                    payload=event.payload if asset_events else None,
                )
            )
    except IntegrityError:
        # A retry that arrived while the delivery it repeats was being recorded: the delivery ID's
        # unique constraint rolled back its events, and it gets the answer that delivery got.
        with create_session(scoped=False) as session:
            original = find_delivery(session, webhook.name, event.delivery_id)
        if original is None:
            raise
        return build_response(original.asset_events, original.candidates)
    if not asset_events:
        # Debug only: a webhook that sends every event would otherwise log most deliveries.
        log.debug("Webhook %s: no Dag schedules on any of its candidates %s", webhook.name, candidates)
    return build_response(asset_events, candidates)


def build_app() -> FastAPI:
    # Airflow mounts plugin apps without authentication, so FastAPI's interactive docs and OpenAPI
    # schema would be served to anyone; only POST /webhooks/<name> should be.
    app = FastAPI(title="Airflow webhooks", docs_url=None, redoc_url=None, openapi_url=None)
    app.include_router(router)
    return app
