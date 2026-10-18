# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
The provider's own tables in the Airflow metadata database.

``airflow db migrate`` creates and migrates them through ``WebhooksDBManager``. They have a metadata of
their own because Airflow refuses an external DB manager whose tables are in Airflow's.
"""

from __future__ import annotations

from airflow.models.base import _get_schema, naming_convention
from airflow.utils.sqlalchemy import UtcDateTime
from sqlalchemy import JSON, Column, Index, MetaData, String, Table, UniqueConstraint

metadata = MetaData(schema=_get_schema(), naming_convention=naming_convention)

# Values of delivery_table.c.outcome. A delivery is accepted when it records asset events.
ACCEPTED = "accepted"
NO_SUBSCRIBERS = "no_subscribers"
# A Dag schedules on a candidate, but every such Dag is paused or draining, so nothing is recorded.
DAG_PAUSED = "dag_paused"

# One row per verified delivery, written in the transaction that records its asset events.
delivery_table = Table(
    "inbound_webhooks_delivery",
    metadata,
    # Assigned by the endpoint, since deliveries from senders without delivery IDs need rows too. The
    # delivery's asset events carry it in their extra, as inbound_delivery_id.
    Column("id", String(36), primary_key=True),
    Column("webhook", String(64), nullable=False),
    # The sender's ID for the delivery, or null if it sends none. With the body's SHA-256, unique per
    # webhook, so a retry is recognised; databases treat nulls as distinct, so deliveries without an ID
    # never collide.
    Column("delivery_id", String(255)),
    Column("body_digest", String(64), nullable=False),
    Column("received_at", UtcDateTime, nullable=False),
    Column("source_type", String(64), nullable=False),
    Column("path", String(1500)),
    Column("candidates", JSON, nullable=False),
    # {asset name: asset event ID}, as in the audit log entry.
    Column("asset_events", JSON, nullable=False),
    Column("correlation_key", String(255)),
    Column("outcome", String(32), nullable=False),
    Column("team_name", String(50)),
    Column("payload", JSON),
    UniqueConstraint("webhook", "delivery_id", "body_digest"),
    Index("idx_inbound_webhooks_delivery_webhook_received_at", "webhook", "received_at"),
    # Deleting old records filters on the time alone.
    Index("idx_inbound_webhooks_delivery_received_at", "received_at"),
    Index("idx_inbound_webhooks_delivery_correlation_key", "correlation_key"),
)
