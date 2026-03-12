"""add performance indexes

Revision ID: b3f12b9e2b64
Revises: a7d85a4bfba3
Create Date: 2026-03-13 00:33:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "b3f12b9e2b64"
down_revision = "a7d85a4bfba3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_videos_created_at_owner", "videos", ["created_at", "owner_id"], unique=False)
    op.create_index("ix_videos_genre_created_at", "videos", ["genre", "created_at"], unique=False)
    op.create_index("ix_video_impressions_video_created", "video_impressions", ["video_id", "created_at"], unique=False)
    op.create_index("ix_watch_progress_video_user", "video_watch_progress", ["video_id", "user_id"], unique=False)
    op.create_index("ix_watch_progress_video_viewer", "video_watch_progress", ["video_id", "viewer_key"], unique=False)
    op.create_index(
        "ix_notifications_user_read_created",
        "notifications",
        ["user_id", "is_read", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_notifications_user_read_created", table_name="notifications")
    op.drop_index("ix_watch_progress_video_viewer", table_name="video_watch_progress")
    op.drop_index("ix_watch_progress_video_user", table_name="video_watch_progress")
    op.drop_index("ix_video_impressions_video_created", table_name="video_impressions")
    op.drop_index("ix_videos_genre_created_at", table_name="videos")
    op.drop_index("ix_videos_created_at_owner", table_name="videos")
