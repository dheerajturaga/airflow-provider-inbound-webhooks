# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""FastAPI app that records verified webhook deliveries as events on the assets Dags subscribe to."""

from __future__ import annotations

import contextlib
import functools
import hashlib
import importlib
import inspect
import json
import logging
import math
import re
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated, Any, cast

from airflow.configuration import conf
from airflow.exceptions import AirflowNotFoundException
from airflow.sdk import Connection
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response
from fastapi.exception_handlers import http_exception_handler
from sqlalchemy import exists, or_, select, update
from sqlalchemy.exc import IntegrityError, InterfaceError, OperationalError, ProgrammingError
from starlette.exceptions import HTTPException as StarletteHTTPException

from airflow_inbound_webhooks import configuration
from airflow_inbound_webhooks.naming import build_candidate_names
from airflow_inbound_webhooks.sources import (
    SOURCE_TYPES,
    STORED_PAYLOADS,
    WebhookEvent,
    WebhookSource,
    WebhookVerificationError,
)

if TYPE_CHECKING:
    from airflow.models.asset import AssetModel
    from airflow.models.log import Log
    from sqlalchemy import ColumnElement, Select
    from sqlalchemy.engine import CursorResult, Row
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

CONN_ID_PREFIX = "webhook_"
DEFAULT_MAX_BODY_BYTES = 1024 * 1024
WEBHOOK_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
# The lengths of the delivery table's delivery_id, path and team_name columns.
MAX_DELIVERY_ID_LENGTH = 255
MAX_PATH_LENGTH = 1500
MAX_TEAM_NAME_LENGTH = 50
# The response's outcome for a retry, which writes no row and is answered 200 rather than 202; other
# outcomes are stored on the row.
DUPLICATE = "duplicate"
# The database is down or locked. SQLite also reports a missing table this way; PostgreSQL and MySQL
# report one as a ProgrammingError, which is only answered as unavailable when the table is missing.
UNAVAILABLE_DATABASE_ERRORS = (OperationalError, InterfaceError)
UNAVAILABLE_RETRY_AFTER_SECONDS = 30
# The tag value metrics give webhook names that are not listed, so random names add no metric series.
UNLISTED_WEBHOOK_TAG = "other"
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


@functools.cache
def get_stats() -> Any:
    """Return Airflow's ``Stats``, which moved to the Task SDK in Airflow 3.2."""
    for module in ("airflow.sdk.observability.stats", "airflow.stats"):
        with contextlib.suppress(ImportError):
            return importlib.import_module(module).Stats
    raise ImportError("Airflow's Stats is in neither airflow.sdk.observability.stats nor airflow.stats")


@dataclass(frozen=True)
class ConfiguredWebhook:
    """A webhook resolved from its ``webhook_<name>`` connection, the only configuration it has."""

    name: str
    source_type: str
    source: WebhookSource
    secret: str
    team_name: str | None
    max_body_bytes: int
    store_payload: str

    def select_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Return what the connection's ``store_payload`` keeps of a delivery's payload."""
        if self.store_payload == "summary":
            return self.source.summarize(payload)
        return payload if self.store_payload == "full" else None


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
    try:
        conn = Connection.get(f"{CONN_ID_PREFIX}{name}")
    except AirflowNotFoundException:
        raise HTTPException(HTTPStatus.NOT_FOUND, "Unknown webhook") from None
    return configure_webhook(name, conn)


def configure_webhook(name: str, conn: Any) -> ConfiguredWebhook:
    """
    Build webhook ``name`` from its connection, answering ``503`` if the connection is misconfigured.

    The ``ValueError`` saying why is the ``HTTPException``'s cause.
    """
    conn_id = f"{CONN_ID_PREFIX}{name}"
    # The connection form saves fields left empty as null or "", which mean "not set".
    extra = {key: value for key, value in conn.extra_dejson.items() if value not in (None, "")}
    try:
        source_type = extra.get("source_type", name)
        if source_type not in SOURCE_TYPES:
            raise ValueError(f"unknown source_type {source_type!r}; expected one of {sorted(SOURCE_TYPES)}")
        source_class = SOURCE_TYPES[source_type]
        store_payload = extra.get("store_payload", source_class.default_store_payload)
        if store_payload not in STORED_PAYLOADS:
            raise ValueError(f"store_payload must be one of {STORED_PAYLOADS}")
        if store_payload == "summary" and not source_class.has_summary:
            # 0.1.0's connection form offered summary for every source type, and only jira used it, so
            # existing connections can set it: they keep the whole payload, as they did then.
            log.warning(
                "Webhook connection %s sets store_payload to summary, which source_type %s does not have; "
                "it stores the whole payload",
                conn_id,
                source_type,
            )
            store_payload = "full"
        if not conn.password:
            raise ValueError("the password, which holds the shared secret, is empty")
        if extra.get("team_name") and not supports_team_attribution():
            raise ValueError("team_name needs Airflow 3.3 or later to attribute events to a team")
        if len(extra.get("team_name") or "") > MAX_TEAM_NAME_LENGTH:
            raise ValueError(f"team_name is longer than Airflow's {MAX_TEAM_NAME_LENGTH} characters")
        # The connection form enforces this, but connections from JSON or environment variables skip it.
        if (max_body_bytes := int(extra.get("max_body_bytes", DEFAULT_MAX_BODY_BYTES))) < 1:
            raise ValueError("max_body_bytes must be at least 1")
        return ConfiguredWebhook(
            name=name,
            source_type=source_type,
            source=source_class.from_extra(extra),
            secret=conn.password,
            team_name=extra.get("team_name"),
            max_body_bytes=max_body_bytes,
            store_payload=store_payload,
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


@functools.cache
def warn_any_webhook_name() -> None:
    # Once per process, like warn_no_webhook_names.
    log.warning(
        "[%s] %s = %s is deprecated and will be removed in a later release: it serves any name, so every "
        "delivery reads a connection. List the webhook names to serve instead",
        configuration.SECTION,
        configuration.WEBHOOK_NAMES,
        configuration.ANY_WEBHOOK_NAME,
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
        warn_any_webhook_name()
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


@dataclass
class TokenBucket:
    """The deliveries one webhook can still make in this process, refilled continuously to the limit."""

    tokens: float
    updated_at: float


# One per webhook that resolved to a connection, so unlisted names never add one.
rate_limit_buckets: dict[str, TokenBucket] = {}
rate_limit_lock = threading.Lock()


def limit_rate(webhook: ConfiguredWebhook) -> None:
    """
    Refuse a verified delivery over ``rate_limit_per_minute``, before it writes anything.

    Only verified deliveries count: a bucket that unsigned requests could empty would let anyone refuse
    the sender's own deliveries, and GitLab disables a webhook whose deliveries keep failing.
    """
    limit = conf.getint(configuration.SECTION, configuration.RATE_LIMIT_PER_MINUTE, fallback=0)
    if limit <= 0:
        return
    now = time.monotonic()
    with rate_limit_lock:
        bucket = rate_limit_buckets.setdefault(webhook.name, TokenBucket(tokens=limit, updated_at=now))
        bucket.tokens = min(limit, bucket.tokens + (now - bucket.updated_at) * limit / 60)
        bucket.updated_at = now
        if bucket.tokens >= 1:
            bucket.tokens -= 1
            return
        retry_after = math.ceil((1 - bucket.tokens) * 60 / limit)
    # Debug only: logging every refused delivery would let a flood fill the log.
    log.debug("Webhook %s is over its rate limit of %d deliveries a minute", webhook.name, limit)
    raise HTTPException(
        HTTPStatus.TOO_MANY_REQUESTS, "Too many deliveries", headers={"Retry-After": str(retry_after)}
    )


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


class LogThrottle:
    """Lets a message through at most once per interval, so a failure on every request logs once."""

    def __init__(self, interval: float) -> None:
        self.interval = interval
        self.last = -math.inf
        self.lock = threading.Lock()

    def ready(self) -> bool:
        now = time.monotonic()
        with self.lock:
            if now - self.last < self.interval:
                return False
            self.last = now
            return True


database_error_log = LogThrottle(interval=60)


def has_delivery_table() -> bool:
    from airflow import settings
    from sqlalchemy import inspect as inspect_database

    from airflow_inbound_webhooks.models import delivery_table

    if settings.engine is None:
        return False
    with settings.engine.connect() as connection:
        return inspect_database(connection).has_table(delivery_table.name, schema=delivery_table.schema)


class RecordedMeanwhile(Exception):
    """Another copy of a redelivered delivery recorded its events first."""


def find_delivery(
    session: Session, webhook_name: str, delivery_id: str | None, body_digest: str
) -> Row[Any] | None:
    """
    Return the stored delivery that a sender's delivery repeats, if any.

    A repeat has the delivery's ID and its body: the ID is a header no signature covers, so a captured
    body sent again under a delivery ID it never had is not taken for that delivery.
    """
    from airflow_inbound_webhooks.models import delivery_table

    if delivery_id is None:
        return None
    columns = delivery_table.c
    return session.execute(
        select(columns.id, columns.outcome, columns.asset_events, columns.candidates).where(
            columns.webhook == webhook_name,
            columns.delivery_id == delivery_id,
            columns.body_digest == body_digest,
        )
    ).one_or_none()


def find_subscribed_assets(session: Session, candidates: list[str]) -> tuple[list[AssetModel], bool]:
    """
    Return the candidates a Dag that is not stale, paused or draining schedules on.

    That is the test Airflow applies before it runs an asset's watchers, plus draining, which Airflow's
    queueing also skips, so an event is never recorded on a name no Dag would run for. A Dag schedules
    on an asset directly, or through ``Asset.ref(name=...)`` or ``Asset.ref(uri=...)``, which Airflow
    queues too. When there are none, the second value says whether a paused or draining Dag schedules
    on a candidate.
    """
    from airflow.models.asset import (
        AssetModel,
        DagScheduleAssetNameReference,
        DagScheduleAssetReference,
        DagScheduleAssetUriReference,
    )
    from airflow.models.dag import DagModel

    not_stale = ~DagModel.is_stale
    live = not_stale & ~DagModel.is_paused
    # The draining state only exists from Airflow 3.4.
    if hasattr(DagModel, "is_draining"):
        live &= ~DagModel.is_draining

    def scheduled_by(condition: ColumnElement[bool]) -> Select[tuple[AssetModel]]:
        return select(AssetModel).where(
            AssetModel.name.in_(candidates),
            AssetModel.active.has(),
            or_(
                AssetModel.scheduled_dags.any(DagScheduleAssetReference.dag.has(condition)),
                exists().where(
                    DagScheduleAssetNameReference.name == AssetModel.name,
                    DagScheduleAssetNameReference.dag.has(condition),
                ),
                exists().where(
                    DagScheduleAssetUriReference.uri == AssetModel.uri,
                    DagScheduleAssetUriReference.dag.has(condition),
                ),
            ),
        )

    if subscribed := list(session.scalars(scheduled_by(live))):
        return subscribed, False
    return [], session.scalars(scheduled_by(not_stale).limit(1)).first() is not None


def build_audit_log(webhook: ConfiguredWebhook, event: WebhookEvent, asset_events: dict[str, int]) -> Log:
    """
    Return the audit log entry for a delivery that recorded asset events.

    It has no Dag or run, which the scheduler creates later, and a display name, without which
    Airflow's audit log page shows no owner.
    """
    from airflow.models.log import Log

    entry = Log(
        event="webhook_delivery",
        owner=f"webhook:{webhook.name}",
        owner_display_name=f"Webhook {webhook.name}",
        extra=json.dumps({"delivery_id": event.delivery_id, "asset_events": asset_events}),
    )
    # Audit log entries have a team only from Airflow 3.4.
    if webhook.team_name and hasattr(Log, "team_name"):
        setattr(entry, "team_name", webhook.team_name)
    return entry


def build_response(
    delivery_id: str | None, outcome: str, asset_events: dict[str, int], candidates: list[str]
) -> dict[str, Any]:
    return {
        "status": "accepted" if asset_events else "no_subscribers",
        "outcome": outcome,
        "delivery_id": delivery_id,
        "asset_events": asset_events,
        "candidates": candidates,
    }


def record_delivery(webhook: ConfiguredWebhook, event: WebhookEvent, body_digest: str) -> dict[str, Any]:
    """
    Record a verified delivery's asset events and its row in one transaction, and answer it.

    A retry, which repeats a delivery that recorded events, gets the answer that delivery got and
    records nothing. A delivery that recorded nothing is processed again.
    """
    # Imported here rather than at module level: Airflow imports plugins in every process, including
    # each task run, and only the API server needs these.
    from airflow.assets.manager import asset_manager
    from airflow.utils.session import create_session

    from airflow_inbound_webhooks.models import ACCEPTED, DAG_PAUSED, NO_SUBSCRIBERS, delivery_table

    candidates = build_candidate_names(webhook.name, event)
    payload = webhook.select_payload(event.payload)
    extra = {
        # Set here, as Airflow sets from_rest_api, so Dags can tell webhook events apart. Anyone allowed to
        # create asset events can set it too, so it marks where an event says it came from, not proof.
        "from_webhook": True,
        "webhook": webhook.name,
        "source_type": webhook.source_type,
        # The same as webhook, kept because it was public first.
        "source": webhook.name,
        "path": event.path,
        "delivery_id": event.delivery_id,
        "correlation_key": event.correlation_key,
        "payload": payload,
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
            original = find_delivery(session, webhook.name, event.delivery_id, body_digest)
            # A retry. A delivery that recorded nothing is processed again instead: a redelivery after
            # subscribing to one of its names is how its README walkthrough ends.
            if original is not None and original.outcome == ACCEPTED:
                log.debug("Webhook %s: delivery %r was recorded before", webhook.name, event.delivery_id)
                return build_response(
                    event.delivery_id, DUPLICATE, original.asset_events, original.candidates
                )
            subscribed, only_paused_subscribers = find_subscribed_assets(session, candidates)
            for asset_model in subscribed:
                asset_event = asset_manager.register_asset_change(
                    asset=asset_model, extra=extra, session=session, **team_arguments
                )
                if asset_event is not None:
                    asset_events[asset_model.name] = asset_event.id
            if asset_events:
                outcome = ACCEPTED
                session.add(build_audit_log(webhook, event, asset_events))
            else:
                # Nothing is recorded for a paused Dag, so unpausing it never replays the delivery.
                outcome = DAG_PAUSED if only_paused_subscribers else NO_SUBSCRIBERS
            recorded = {
                "candidates": candidates,
                "asset_events": asset_events,
                "outcome": outcome,
                # A delivery no Dag subscribes to keeps its names, for troubleshooting, but not its
                # payload, so a sender that sends every event does not fill the table.
                "payload": payload if asset_events else None,
            }
            if original is None:
                session.execute(
                    delivery_table.insert().values(
                        id=str(uuid.uuid4()),
                        webhook=webhook.name,
                        delivery_id=event.delivery_id,
                        body_digest=body_digest,
                        received_at=datetime.now(tz=timezone.utc),
                        source_type=webhook.source_type,
                        correlation_key=event.correlation_key,
                        # Longer paths cannot be in an asset name anyway.
                        path=None if event.path is None else event.path[:MAX_PATH_LENGTH],
                        team_name=webhook.team_name,
                        **recorded,
                    )
                )
            elif asset_events:
                # Only if no other copy of the redelivery has recorded its events in the meantime:
                # the update waits for that copy's transaction, and then matches nothing.
                result = session.execute(
                    update(delivery_table)
                    .where(delivery_table.c.id == original.id, delivery_table.c.outcome != ACCEPTED)
                    .values(**recorded)
                )
                if cast("CursorResult[Any]", result).rowcount != 1:
                    raise RecordedMeanwhile
    except (IntegrityError, RecordedMeanwhile):
        # A copy that arrived while another was being recorded: rolling back the transaction undid its
        # events, and it gets the answer that copy got.
        with create_session(scoped=False) as session:
            original = find_delivery(session, webhook.name, event.delivery_id, body_digest)
        if original is None:
            raise
        return build_response(event.delivery_id, DUPLICATE, original.asset_events, original.candidates)
    if not asset_events:
        # Debug only: a webhook that sends every event would otherwise log most deliveries.
        log.debug("Webhook %s: no Dag schedules on any of its candidates %s", webhook.name, candidates)
    return build_response(event.delivery_id, outcome, asset_events, candidates)


router = APIRouter()


@router.get("/_health")
def health() -> dict[str, str]:
    """Answer whether the endpoint is up, for load balancers and probes; no webhook can be named _health."""
    # No database query: anyone can call it.
    return {"status": "healthy"}


# Also with a trailing slash: otherwise the router answers it with a redirect, which senders such as
# GitHub do not follow, so a URL pasted with a slash would fail every delivery.
@router.post("/{name}/", status_code=HTTPStatus.ACCEPTED)
@router.post("/{name}", status_code=HTTPStatus.ACCEPTED)
def receive_webhook(
    request: Request,
    response: Response,
    webhook: WebhookDep,
    body: Annotated[bytes, Depends(read_limited_body)],
) -> dict[str, Any]:
    try:
        webhook.source.verify(request.headers, body, webhook.secret)
    except WebhookVerificationError as e:
        log.warning("Rejected delivery for webhook %s: %s", webhook.name, e)
        raise HTTPException(HTTPStatus.UNAUTHORIZED, "Invalid webhook credentials") from e
    except Exception as e:
        # Answered as a failed verification, so what made the verifier fail cannot be told from outside.
        log.exception("Rejected delivery for webhook %s: verifying it raised", webhook.name)
        raise HTTPException(HTTPStatus.UNAUTHORIZED, "Invalid webhook credentials") from e
    limit_rate(webhook)
    try:
        event = webhook.source.parse(request.headers, body)
    except ValueError as e:
        raise HTTPException(HTTPStatus.BAD_REQUEST, str(e)) from e
    # An empty header is no delivery ID; it would make every later delivery without one a retry.
    event = replace(event, delivery_id=event.delivery_id or None)
    if event.delivery_id is not None and len(event.delivery_id) > MAX_DELIVERY_ID_LENGTH:
        raise HTTPException(
            HTTPStatus.BAD_REQUEST, f"Delivery ID is longer than {MAX_DELIVERY_ID_LENGTH} characters"
        )
    try:
        with get_stats().timer("webhooks.delivery_duration", tags={"webhook": webhook.name}):
            answer = record_delivery(webhook, event, hashlib.sha256(body).hexdigest())
    except (*UNAVAILABLE_DATABASE_ERRORS, ProgrammingError) as e:
        # A ProgrammingError that is not about a missing table is a bug, which a sender's retries would
        # not fix.
        if isinstance(e, ProgrammingError) and has_delivery_table():
            raise
        # Once a minute at most: while the database is down, every delivery fails the same way.
        if database_error_log.ready():
            log.exception(
                "Webhook %s: could not record a delivery. Check that the database is reachable and that "
                "airflow db migrate has created the delivery table",
                webhook.name,
            )
        # A status senders retry, and no detail, which would tell anyone about the database.
        raise HTTPException(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "Temporarily unavailable",
            headers={"Retry-After": str(UNAVAILABLE_RETRY_AFTER_SECONDS)},
        ) from e
    if answer["outcome"] == DUPLICATE:
        # Nothing new was accepted.
        response.status_code = HTTPStatus.OK
    get_stats().incr("webhooks.deliveries", tags={"webhook": webhook.name, "outcome": answer["outcome"]})
    return answer


async def count_rejection(request: Request, exc: Exception) -> Response:
    """Count a refused delivery by its status code, then answer it as FastAPI would."""
    if not isinstance(exc, StarletteHTTPException):
        raise exc
    # Deliveries are POSTs; the deliveries API and page refuse readers, not deliveries.
    if request.method == "POST":
        name = request.path_params.get("name")
        tag = name if name in get_webhook_names() else UNLISTED_WEBHOOK_TAG
        # int() first: before Python 3.11, str() of an HTTPStatus is its name, such as HTTPStatus.NOT_FOUND.
        status = str(int(exc.status_code))
        get_stats().incr("webhooks.rejections", tags={"webhook": tag, "status": status})
    return await http_exception_handler(request, exc)


def build_app() -> FastAPI:
    # Airflow mounts plugin apps without authentication, so FastAPI's interactive docs and OpenAPI
    # schema would be served to anyone; only POST /webhooks/<name> should be.
    # Imported here: it imports this module.
    from airflow_inbound_webhooks.deliveries import router as deliveries_router

    app = FastAPI(title="Airflow webhooks", docs_url=None, redoc_url=None, openapi_url=None)
    # First, so its paths are matched before /{name}.
    app.include_router(deliveries_router)
    app.include_router(router)
    # Starlette's, so a path or method the app does not serve is counted too.
    app.add_exception_handler(StarletteHTTPException, count_rejection)
    return app
