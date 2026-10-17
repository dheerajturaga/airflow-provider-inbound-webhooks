# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
${imports if imports else ""}
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}
# The provider release that adds this revision.
inbound_webhooks_version = "TODO"


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
