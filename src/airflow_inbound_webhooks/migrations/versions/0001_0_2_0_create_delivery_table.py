# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Create the delivery table.

Revision ID: dd5122e3e3b0
Revises:
Create Date: 2026-10-17 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "dd5122e3e3b0"
down_revision = None
branch_labels = None
depends_on = None
# The provider release that adds this revision.
inbound_webhooks_version = "0.2.0"

TABLE = "inbound_webhooks_delivery"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("webhook", sa.String(length=64), nullable=False),
        sa.Column("delivery_id", sa.String(length=255), nullable=True),
        # The type of Airflow's UtcDateTime, with microseconds on MySQL as on the other databases.
        sa.Column(
            "received_at",
            sa.TIMESTAMP(timezone=True).with_variant(mysql.TIMESTAMP(fsp=6), "mysql"),
            nullable=False,
        ),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("path", sa.String(length=1500), nullable=True),
        sa.Column("candidates", sa.JSON(), nullable=False),
        sa.Column("asset_events", sa.JSON(), nullable=False),
        sa.Column("correlation_key", sa.String(length=255), nullable=True),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("team_name", sa.String(length=50), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="inbound_webhooks_delivery_pkey"),
        sa.UniqueConstraint(
            "webhook", "delivery_id", name="inbound_webhooks_delivery_webhook_delivery_id_uq"
        ),
    )
    op.create_index("idx_inbound_webhooks_delivery_webhook_received_at", TABLE, ["webhook", "received_at"])
    op.create_index("idx_inbound_webhooks_delivery_correlation_key", TABLE, ["correlation_key"])


def downgrade() -> None:
    op.drop_table(TABLE)
