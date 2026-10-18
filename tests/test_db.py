# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""The provider's tables, as ``airflow db migrate`` creates them through ``WebhooksDBManager``."""

from __future__ import annotations

import airflow
import pytest
from airflow import settings
from airflow.utils.session import create_session
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from packaging.version import Version
from sqlalchemy import inspect

from airflow_inbound_webhooks.app import MAX_DELIVERY_ID_LENGTH, MAX_PATH_LENGTH, MAX_TEAM_NAME_LENGTH
from airflow_inbound_webhooks.db import WebhooksDBManager
from airflow_inbound_webhooks.models import delivery_table, metadata

TABLE_NAMES = {table.name for table in metadata.tables.values()}


def include_own_tables(_object, name, type_, *_args) -> bool:
    return type_ != "table" or name in TABLE_NAMES


def compare_with_metadata() -> list:
    # A connection of its own, closed before anything else runs DDL: an open transaction would hold
    # locks that SQLite and MySQL make DDL wait for.
    with settings.engine.connect() as connection:
        context = MigrationContext.configure(
            connection, opts={"compare_type": True, "include_object": include_own_tables}
        )
        return compare_metadata(context, metadata)


def has_delivery_table() -> bool:
    with settings.engine.connect() as connection:
        return inspect(connection).has_table(delivery_table.name, schema=delivery_table.schema)


def test_airflow_db_migrate_creates_the_tables_at_the_latest_revision():
    # The session fixture in conftest.py ran airflow db migrate.
    with create_session() as session:
        assert WebhooksDBManager(session).check_migration()
    assert compare_with_metadata() == []


def test_migrations_build_the_tables_the_metadata_describes():
    # Depending on the Airflow version, and on whether Airflow's own tables exist already, airflow db
    # migrate builds the tables from the metadata or by running the migrations, so both must agree.
    with create_session() as session:
        config = WebhooksDBManager(session).get_alembic_config()
    command.downgrade(config, "base")
    try:
        assert not has_delivery_table()
        command.upgrade(config, "heads")
        assert compare_with_metadata() == []
    finally:
        command.upgrade(config, "heads")


def test_endpoint_bounds_values_as_the_columns_do():
    # The endpoint cannot import the model at module level, which every Airflow process would pay for.
    columns = delivery_table.c
    assert (columns.delivery_id.type.length, columns.path.type.length, columns.team_name.type.length) == (
        MAX_DELIVERY_ID_LENGTH,
        MAX_PATH_LENGTH,
        MAX_TEAM_NAME_LENGTH,
    )


def test_airflow_accepts_the_manager():
    from airflow.utils.db_manager import RunDBManager

    # Raises if a table is also in Airflow's metadata, or the version table is Airflow's.
    RunDBManager._validate(WebhooksDBManager)


@pytest.mark.skipif(
    Version(airflow.__version__) < Version("3.2"), reason="Airflow 3.1 runs only configured DB managers"
)
def test_airflow_finds_the_manager_in_the_provider_metadata():
    from airflow.providers_manager import ProvidersManager

    assert "airflow_inbound_webhooks.db.WebhooksDBManager" in ProvidersManager().db_managers
