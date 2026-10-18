# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Creates and migrates the provider's tables as part of ``airflow db migrate``.

Airflow 3.2 and later find the manager through ``db-managers`` in the provider metadata. Airflow 3.1 only
runs it when ``[database] external_db_managers`` names it.
"""

from __future__ import annotations

from pathlib import Path

from airflow.utils.db_manager import BaseDBManager

from airflow_inbound_webhooks.models import metadata

PACKAGE_DIR = Path(__file__).parent


class WebhooksDBManager(BaseDBManager):
    """The external DB manager for the tables in ``airflow_inbound_webhooks.models``."""

    metadata = metadata
    version_table_name = "alembic_version_inbound_webhooks"
    migration_dir = (PACKAGE_DIR / "migrations").as_posix()
    alembic_file = (PACKAGE_DIR / "alembic.ini").as_posix()
    # Drop the tables with Airflow's on airflow db reset.
    supports_table_dropping = True
