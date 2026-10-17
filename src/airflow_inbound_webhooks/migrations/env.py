# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""Alembic environment for the provider's tables, run by ``WebhooksDBManager``."""

from __future__ import annotations

import contextlib
from typing import Any

from airflow import settings
from alembic import context

from airflow_inbound_webhooks.db import WebhooksDBManager

config = context.config
target_metadata = WebhooksDBManager.metadata
# Bare names: with [database] sql_alchemy_schema set, the metadata's keys carry the schema.
TABLE_NAMES = {table.name for table in target_metadata.tables.values()}


def include_object(_object: Any, name: str | None, type_: str, *_args: Any) -> bool:
    # The metadata database holds Airflow's tables too, which this environment must never touch.
    return type_ != "table" or name in TABLE_NAMES


def run_migrations_offline() -> None:
    context.configure(
        url=settings.SQL_ALCHEMY_CONN,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        render_as_batch=True,
        version_table=WebhooksDBManager.version_table_name,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    with contextlib.ExitStack() as stack:
        # A caller can pass its own connection, as Airflow does for its own migrations.
        if (connection := config.attributes.get("connection")) is None:
            if settings.engine is None:
                raise RuntimeError("Airflow's database engine is not configured")
            connection = stack.enter_context(settings.engine.connect())
        context.configure(
            connection=connection,
            transaction_per_migration=True,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=True,
            version_table=WebhooksDBManager.version_table_name,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
