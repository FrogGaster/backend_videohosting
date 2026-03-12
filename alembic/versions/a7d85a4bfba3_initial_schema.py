"""initial schema

Revision ID: a7d85a4bfba3
Revises: 
Create Date: 2026-03-13 00:15:52.698674
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from main import Base


revision = 'a7d85a4bfba3'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
