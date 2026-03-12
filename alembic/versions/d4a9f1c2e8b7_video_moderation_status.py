"""add video moderation status

Revision ID: d4a9f1c2e8b7
Revises: b3f12b9e2b64
Create Date: 2026-03-13 01:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "d4a9f1c2e8b7"
down_revision = "b3f12b9e2b64"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("videos", sa.Column("moderation_status", sa.String(length=16), nullable=False, server_default="approved"))
    op.add_column("videos", sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_videos_moderation_status", "videos", ["moderation_status"], unique=False)
    op.execute("UPDATE videos SET moderation_status = 'approved' WHERE moderation_status IS NULL")
    op.alter_column("videos", "moderation_status", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_videos_moderation_status", table_name="videos")
    op.drop_column("videos", "approved_at")
    op.drop_column("videos", "moderation_status")
