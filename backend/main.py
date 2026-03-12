from __future__ import annotations

import os
from hashlib import sha256
import math
import shutil
import subprocess
import json
import logging
import re
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict, EmailStr, Field
from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, create_engine, desc, event, func, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

BASE_DIR = Path(__file__).resolve().parent
MEDIA_DIR = BASE_DIR / "media"
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
(MEDIA_DIR / "videos").mkdir(parents=True, exist_ok=True)
(MEDIA_DIR / "thumbs").mkdir(parents=True, exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{(BASE_DIR / 'app.db').as_posix()}")
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
ACCESS_TOKEN_MINUTES = 60 * 24 * 7
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local").lower()
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
CDN_BASE_URL = os.getenv("CDN_BASE_URL", "").rstrip("/")
MEDIA_REDIRECT_BASE_URL = os.getenv("MEDIA_REDIRECT_BASE_URL", "").rstrip("/")
USE_CELERY = os.getenv("USE_CELERY", "0") == "1"
ANALYTICS_BATCH_SIZE = int(os.getenv("ANALYTICS_BATCH_SIZE", "40"))
ANALYTICS_FLUSH_INTERVAL_SEC = float(os.getenv("ANALYTICS_FLUSH_INTERVAL_SEC", "2.5"))
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
S3_CONNECT_TIMEOUT_SEC = int(os.getenv("S3_CONNECT_TIMEOUT_SEC", "3"))
S3_READ_TIMEOUT_SEC = int(os.getenv("S3_READ_TIMEOUT_SEC", "20"))
S3_MAX_RETRIES = int(os.getenv("S3_MAX_RETRIES", "5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_VIDEO_BYTES = int(os.getenv("MAX_VIDEO_BYTES", str(1024 * 1024 * 1024)))

is_sqlite = DATABASE_URL.startswith("sqlite")
engine_kwargs: dict = {"future": True}
if is_sqlite:
    engine_kwargs["connect_args"] = {"check_same_thread": False}
else:
    engine_kwargs.update(
        {
            "pool_size": DB_POOL_SIZE,
            "max_overflow": DB_MAX_OVERFLOW,
            "pool_timeout": DB_POOL_TIMEOUT,
            "pool_recycle": DB_POOL_RECYCLE,
            "pool_pre_ping": True,
        }
    )
engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

try:
    import boto3
    from botocore.config import Config as BotoConfig
except Exception:  # pragma: no cover
    boto3 = None
    BotoConfig = None


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    can_upload: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    channels: Mapped[list["Channel"]] = relationship(back_populates="owner")


class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    owner_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(120))
    description: Mapped[str] = mapped_column(Text, default="")
    avatar_url: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    owner: Mapped[User] = relationship(back_populates="channels")
    videos: Mapped[list["Video"]] = relationship(back_populates="channel")


class Video(Base):
    __tablename__ = "videos"
    __table_args__ = (
        Index("ix_videos_created_at_owner", "created_at", "owner_id"),
        Index("ix_videos_genre_created_at", "genre", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    owner_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id", ondelete="CASCADE"), index=True)
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(Text, default="")
    genre: Mapped[str] = mapped_column(String(64), default="hoi4_letsplay", index=True)
    file_path: Mapped[str] = mapped_column(String(500))
    thumbnail_path: Mapped[str] = mapped_column(String(500), default="")
    moderation_status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    views_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

    channel: Mapped[Channel] = relationship(back_populates="videos")


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (UniqueConstraint("subscriber_id", "channel_id", name="uq_subscriber_channel"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class VideoLike(Base):
    __tablename__ = "video_likes"
    __table_args__ = (UniqueConstraint("user_id", "video_id", name="uq_user_video_like"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Comment(Base):
    __tablename__ = "comments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    text: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class VideoView(Base):
    __tablename__ = "video_views"
    __table_args__ = (
        UniqueConstraint("video_id", "user_id", name="uq_video_user_view"),
        UniqueConstraint("video_id", "viewer_key", name="uq_video_viewer_key_view"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    viewer_key: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class VideoImpression(Base):
    __tablename__ = "video_impressions"
    __table_args__ = (
        UniqueConstraint("video_id", "viewer_key", name="uq_video_viewer_impression"),
        Index("ix_video_impressions_video_created", "video_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    viewer_key: Mapped[str] = mapped_column(String(128), index=True)
    source: Mapped[str] = mapped_column(String(30), default="feed")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class VideoWatchProgress(Base):
    __tablename__ = "video_watch_progress"
    __table_args__ = (
        UniqueConstraint("video_id", "viewer_key", name="uq_video_viewer_progress"),
        Index("ix_watch_progress_video_user", "video_id", "user_id"),
        Index("ix_watch_progress_video_viewer", "video_id", "viewer_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    viewer_key: Mapped[str] = mapped_column(String(128), index=True)
    max_progress_pct: Mapped[float] = mapped_column(default=0.0)
    max_seconds_watched: Mapped[float] = mapped_column(default=0.0)
    duration_seconds: Mapped[float] = mapped_column(default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class WatchLater(Base):
    __tablename__ = "watch_later"
    __table_args__ = (UniqueConstraint("user_id", "video_id", name="uq_watch_later_user_video"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    status: Mapped[str] = mapped_column(String(32), default="processing", index=True)
    error_message: Mapped[str] = mapped_column(Text, default="")
    output_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class BlockedVideo(Base):
    __tablename__ = "blocked_videos"
    __table_args__ = (UniqueConstraint("video_id", name="uq_blocked_video"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    reason: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class BlockedComment(Base):
    __tablename__ = "blocked_comments"
    __table_args__ = (UniqueConstraint("comment_id", name="uq_blocked_comment"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    comment_id: Mapped[int] = mapped_column(ForeignKey("comments.id", ondelete="CASCADE"), index=True)
    reason: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class VideoReport(Base):
    __tablename__ = "video_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    reporter_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    video_id: Mapped[int] = mapped_column(ForeignKey("videos.id", ondelete="CASCADE"), index=True)
    reason: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class CommentReport(Base):
    __tablename__ = "comment_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    reporter_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    comment_id: Mapped[int] = mapped_column(ForeignKey("comments.id", ondelete="CASCADE"), index=True)
    reason: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (Index("ix_notifications_user_read_created", "user_id", "is_read", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    type: Mapped[str] = mapped_column(String(64), index=True)
    message: Mapped[str] = mapped_column(String(500))
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserRegisterIn(BaseModel):
    username: str = Field(min_length=2, max_length=50)
    email: EmailStr
    password: str = Field(min_length=6, max_length=128)


class UserLoginIn(BaseModel):
    email: str = Field(min_length=1, max_length=255)
    password: str = Field(min_length=1, max_length=128)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    username: str
    email: EmailStr
    is_admin: bool
    can_upload: bool


class ToggleUploadIn(BaseModel):
    can_upload: bool


class ChannelOut(BaseModel):
    id: int
    owner_id: int
    name: str
    description: str
    avatar_url: str
    subscribers_count: int = 0


class VideoOut(BaseModel):
    id: int
    owner_id: int
    channel_id: int
    channel_name: str
    title: str
    description: str
    genre: str
    views_count: int
    likes_count: int
    comments_count: int
    thumbnail_url: str
    moderation_status: str
    created_at: datetime


class CommentIn(BaseModel):
    text: str = Field(min_length=1, max_length=2000)


class CommentOut(BaseModel):
    id: int
    user_id: int
    username: str
    video_id: int
    text: str
    created_at: datetime


class VideoListOut(BaseModel):
    items: list[VideoOut]
    next_cursor: Optional[int]


class VideoQualitiesOut(BaseModel):
    items: list[str]


class ContinueWatchingItemOut(BaseModel):
    video: VideoOut
    progress_pct: float
    seconds_watched: float


class SearchSuggestionsOut(BaseModel):
    items: list[str]


class ImpressionIn(BaseModel):
    video_id: int
    source: str = Field(default="feed", min_length=1, max_length=30)


class WatchProgressIn(BaseModel):
    video_id: int
    seconds_watched: float
    duration_seconds: float
    progress_pct: float


class AnalyticsBatchIn(BaseModel):
    impressions: list[ImpressionIn] = []
    watch_progress: list[WatchProgressIn] = []


class RetentionPoint(BaseModel):
    bucket: str
    viewers: int


class VideoAnalyticsOut(BaseModel):
    video_id: int
    impressions: int
    unique_views: int
    ctr_percent: float
    avg_watch_percent: float
    retention: list[RetentionPoint]


class ProcessingJobOut(BaseModel):
    video_id: int
    status: str
    error_message: str = ""
    output: dict = {}


class LikeStatusOut(BaseModel):
    is_liked: bool
    likes_count: int


class SubscriptionStatusOut(BaseModel):
    is_subscribed: bool


class ReportIn(BaseModel):
    reason: str = Field(min_length=1, max_length=255)


class ModerationActionIn(BaseModel):
    action: str


class VideoModerationIn(BaseModel):
    action: str = Field(min_length=1, max_length=16)


class ModerationReportOut(BaseModel):
    kind: str
    report_id: int
    target_id: int
    reason: str
    status: str
    created_at: datetime


class NotificationOut(BaseModel):
    id: int
    type: str
    message: str
    is_read: bool
    created_at: datetime
    payload: dict = {}


class DailyPointOut(BaseModel):
    day: str
    value: float


class TrafficSourceOut(BaseModel):
    source: str
    impressions: int


class TopVideoOut(BaseModel):
    video_id: int
    title: str
    views: int
    avg_watch_percent: float


class StudioDashboardOut(BaseModel):
    daily_views: list[DailyPointOut]
    daily_watch_time_minutes: list[DailyPointOut]
    traffic_sources: list[TrafficSourceOut]
    watch_time_curve: list[DailyPointOut]
    top_videos: list[TopVideoOut]


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


rate_limit_store: dict[str, list[float]] = {}
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("moorluck.api")
app_started_at = time.time()
request_durations_ms: deque[float] = deque(maxlen=2000)
metrics_lock = threading.Lock()
metrics_state = {
    "requests_total": 0,
    "errors_total": 0,
    "db_query_count": 0,
    "db_query_time_ms": 0.0,
}
analytics_lock = threading.Lock()
impression_events_buffer: list[dict] = []
progress_events_buffer: list[dict] = []
analytics_stop_event = threading.Event()
HTML_TAG_RE = re.compile(r"<[^>]*>")
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")


@event.listens_for(engine, "before_cursor_execute")
def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
    context._query_start_time = time.perf_counter()


@event.listens_for(engine, "after_cursor_execute")
def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
    elapsed = (time.perf_counter() - getattr(context, "_query_start_time", time.perf_counter())) * 1000.0
    with metrics_lock:
        metrics_state["db_query_count"] += 1
        metrics_state["db_query_time_ms"] += elapsed


def check_rate_limit(request: Request, key: str, max_requests: int, window_seconds: int) -> None:
    now_ts = datetime.now(timezone.utc).timestamp()
    client_host = request.client.host if request.client else "unknown"
    bucket_key = f"{key}:{client_host}"
    entries = rate_limit_store.get(bucket_key, [])
    alive_from = now_ts - window_seconds
    entries = [item for item in entries if item >= alive_from]
    if len(entries) >= max_requests:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many requests")
    entries.append(now_ts)
    rate_limit_store[bucket_key] = entries


def sanitize_text_input(value: str, *, max_len: int, allow_newlines: bool = False) -> str:
    normalized = value or ""
    normalized = CONTROL_CHARS_RE.sub("", normalized)
    normalized = HTML_TAG_RE.sub("", normalized)
    if not allow_newlines:
        normalized = normalized.replace("\n", " ").replace("\r", " ")
    return normalized.strip()[:max_len]


def build_safe_ilike_pattern(query: str) -> str:
    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(user_id: int) -> str:
    payload = {"sub": str(user_id), "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_MINUTES)}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def get_current_user(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> User:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        user_id = int(payload.get("sub", "0"))
    except (JWTError, ValueError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from None
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


def get_optional_user(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> User | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        user_id = int(payload.get("sub", "0"))
    except (JWTError, ValueError):
        return None
    return db.get(User, user_id)


def require_admin(user: User = Depends(get_current_user)) -> User:
    if not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin required")
    return user


def require_uploader(user: User = Depends(get_current_user)) -> User:
    # Upload is open for any authenticated user; visibility is controlled by moderation_status.
    return user


def video_to_out(db: Session, video: Video) -> VideoOut:
    likes_count = db.query(func.count(VideoLike.id)).filter(VideoLike.video_id == video.id).scalar() or 0
    comments_count = db.query(func.count(Comment.id)).filter(Comment.video_id == video.id).scalar() or 0
    channel = db.get(Channel, video.channel_id)
    return VideoOut(
        id=video.id,
        owner_id=video.owner_id,
        channel_id=video.channel_id,
        channel_name=channel.name if channel else "",
        title=video.title,
        description=video.description,
        genre=video.genre,
        views_count=video.views_count,
        likes_count=likes_count,
        comments_count=comments_count,
        thumbnail_url=storage_public_url(video.thumbnail_path) if video.thumbnail_path else "",
        moderation_status=video.moderation_status,
        created_at=video.created_at,
    )


def can_access_video(video: Video, user: User | None) -> bool:
    if video.moderation_status == "approved":
        return True
    if not user:
        return False
    return user.is_admin or video.owner_id == user.id


def read_upload_limited(upload: UploadFile, max_bytes: int) -> bytes:
    buffer = bytearray()
    while True:
        chunk = upload.file.read(1024 * 1024)
        if not chunk:
            break
        buffer.extend(chunk)
        if len(buffer) > max_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"Video file is too large. Max size is {max_bytes // (1024 * 1024)} MB",
            )
    return bytes(buffer)


def build_viewer_key(request: Request, user: User | None) -> str:
    if user:
        return f"user:{user.id}"
    ip = request.client.host if request.client else "unknown"
    ua = request.headers.get("user-agent", "na")
    return "anon:" + sha256(f"{ip}|{ua}".encode("utf-8")).hexdigest()


def get_ab_bucket(user: User | None, viewer_key: str, feature_name: str = "ranking_v2") -> str:
    stable = f"{feature_name}:{user.id if user else viewer_key}"
    digest = sha256(stable.encode("utf-8")).hexdigest()
    return "A" if int(digest[-2:], 16) % 2 == 0 else "B"


def count_unique_views(db: Session, video_id: int) -> int:
    return db.query(func.count(VideoView.id)).filter(VideoView.video_id == video_id).scalar() or 0


def apply_unique_view(db: Session, request: Request, video: Video, user: User | None) -> None:
    viewer_key = build_viewer_key(request, user)
    existing = db.query(VideoView).filter(VideoView.video_id == video.id, VideoView.viewer_key == viewer_key).first()
    if existing:
        return
    row = VideoView(video_id=video.id, user_id=user.id if user else None, viewer_key=viewer_key)
    db.add(row)
    db.flush()
    video.views_count = count_unique_views(db, video.id)


def _safe_ratio(num: float, den: float) -> float:
    return num / den if den > 0 else 0.0


def _normalize(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    return max(0.0, min(1.0, (value - low) / (high - low)))


def get_user_genre_affinity(db: Session, user_id: int) -> dict[str, float]:
    rows = (
        db.query(Video.genre, func.avg(VideoWatchProgress.max_progress_pct))
        .join(Video, Video.id == VideoWatchProgress.video_id)
        .filter(VideoWatchProgress.user_id == user_id)
        .group_by(Video.genre)
        .all()
    )
    return {genre: float(avg_progress or 0.0) / 100.0 for genre, avg_progress in rows}


def rank_videos(
    db: Session,
    videos: list[Video],
    user: User | None,
    subscribed_channel_ids: set[int],
    genre_affinity: dict[str, float],
    limit: int,
    surface: str,
    ab_bucket: str,
) -> list[Video]:
    if not videos:
        return []
    video_ids = [item.id for item in videos]

    impressions_rows = (
        db.query(VideoImpression.video_id, func.count(VideoImpression.id))
        .filter(VideoImpression.video_id.in_(video_ids))
        .group_by(VideoImpression.video_id)
        .all()
    )
    views_rows = (
        db.query(VideoView.video_id, func.count(VideoView.id))
        .filter(VideoView.video_id.in_(video_ids))
        .group_by(VideoView.video_id)
        .all()
    )
    likes_rows = (
        db.query(VideoLike.video_id, func.count(VideoLike.id))
        .filter(VideoLike.video_id.in_(video_ids))
        .group_by(VideoLike.video_id)
        .all()
    )
    comments_rows = (
        db.query(Comment.video_id, func.count(Comment.id))
        .filter(Comment.video_id.in_(video_ids))
        .group_by(Comment.video_id)
        .all()
    )
    retention_rows = (
        db.query(
            VideoWatchProgress.video_id,
            func.avg(VideoWatchProgress.max_progress_pct),
            func.avg(VideoWatchProgress.max_seconds_watched),
        )
        .filter(VideoWatchProgress.video_id.in_(video_ids))
        .group_by(VideoWatchProgress.video_id)
        .all()
    )
    impressions = {video_id: count for video_id, count in impressions_rows}
    views = {video_id: count for video_id, count in views_rows}
    likes = {video_id: count for video_id, count in likes_rows}
    comments = {video_id: count for video_id, count in comments_rows}
    retention = {video_id: (float(avg_pct or 0.0), float(avg_seconds or 0.0)) for video_id, avg_pct, avg_seconds in retention_rows}

    scored: list[tuple[Video, float]] = []
    now = datetime.now(timezone.utc)
    for video in videos:
        v_impressions = float(impressions.get(video.id, 0))
        v_views = float(views.get(video.id, 0))
        v_likes = float(likes.get(video.id, 0))
        v_comments = float(comments.get(video.id, 0))
        avg_watch_pct, avg_watch_seconds = retention.get(video.id, (0.0, 0.0))

        ctr_smoothed = _safe_ratio(v_views + 3.0, v_impressions + 30.0)
        watch_time_per_impression = _safe_ratio(avg_watch_seconds * v_views, max(1.0, v_impressions))
        likes_rate = _safe_ratio(v_likes, max(1.0, v_views))
        comments_rate = _safe_ratio(v_comments, max(1.0, v_views))
        engagement_rate = 0.6 * likes_rate + 0.4 * comments_rate

        created_at = video.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        age_hours = (now - created_at).total_seconds() / 3600.0
        freshness_boost = math.exp(-max(0.0, age_hours) / 72.0)

        trust_score = 1.0
        if v_impressions >= 20 and ctr_smoothed > 0.6 and avg_watch_pct < 15:
            trust_score = 0.75

        personalization_bonus = 0.0
        if user:
            if video.channel_id in subscribed_channel_ids:
                personalization_bonus += 0.15
            personalization_bonus += 0.15 * genre_affinity.get(video.genre, 0.0)

        exploration_boost = 0.08 if v_impressions < 20 else 0.0

        weights = {
            "home": (0.30, 0.35, 0.20, 0.10, 0.05),
            "watch_next": (0.25, 0.40, 0.20, 0.10, 0.05),
            "trending": (0.40, 0.20, 0.15, 0.15, 0.10),
            "subscriptions": (0.20, 0.30, 0.20, 0.10, 0.20),
        }.get(surface, (0.30, 0.35, 0.20, 0.10, 0.05))
        if ab_bucket == "B":
            # Variant B favors retention slightly more.
            weights = (weights[0] - 0.03, weights[1] + 0.05, weights[2], weights[3], weights[4] - 0.02)

        score = (
            weights[0] * _normalize(ctr_smoothed, 0.0, 0.35)
            + weights[1] * _normalize(avg_watch_pct, 0.0, 70.0)
            + weights[2] * _normalize(watch_time_per_impression, 0.0, 300.0)
            + weights[3] * _normalize(engagement_rate, 0.0, 0.15)
            + weights[4] * _normalize(freshness_boost, 0.0, 1.0)
            + personalization_bonus
            + exploration_boost
        ) * trust_score

        scored.append((video, score))

    scored.sort(key=lambda item: item[1], reverse=True)

    # Diversity: no more than 2 videos per channel in the top window.
    result: list[Video] = []
    by_channel: dict[int, int] = {}
    for video, _ in scored:
        current = by_channel.get(video.channel_id, 0)
        if current >= 2:
            continue
        by_channel[video.channel_id] = current + 1
        result.append(video)
        if len(result) >= limit + 6:
            break
    return result


def parse_range_header(range_header: str | None, file_size: int) -> tuple[int, int]:
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1
    raw = range_header.replace("bytes=", "")
    start_s, end_s = raw.split("-", 1)
    start = int(start_s) if start_s else 0
    end = int(end_s) if end_s else file_size - 1
    if start >= file_size or end >= file_size or start > end:
        raise HTTPException(status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail="Invalid range")
    return start, end


def quality_variant_path(original_path: Path, quality: str) -> Path:
    return original_path.with_name(f"{original_path.stem}_{quality}{original_path.suffix}")


def available_qualities(original_path: Path) -> list[str]:
    items = ["source"]
    for quality in ("1080p", "720p", "480p", "360p"):
        if quality_variant_path(original_path, quality).exists():
            items.append(quality)
    return items


def maybe_generate_renditions(original_path: Path) -> None:
    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        return
    renditions = [
        ("720p", 1280, 720),
        ("480p", 854, 480),
        ("360p", 640, 360),
    ]
    for label, width, height in renditions:
        target = quality_variant_path(original_path, label)
        if target.exists():
            continue
        command = [
            ffmpeg_bin,
            "-y",
            "-i",
            str(original_path),
            "-vf",
            f"scale='min({width},iw)':'min({height},ih)':force_original_aspect_ratio=decrease",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "24",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            str(target),
        ]
        proc = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if proc.returncode != 0 and target.exists():
            target.unlink(missing_ok=True)


def maybe_generate_hls(original_path: Path, output_dir: Path) -> str | None:
    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    master_path = output_dir / "master.m3u8"
    command = [
        ffmpeg_bin,
        "-y",
        "-i",
        str(original_path),
        "-preset",
        "veryfast",
        "-g",
        "48",
        "-sc_threshold",
        "0",
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-f",
        "hls",
        "-hls_time",
        "4",
        "-hls_playlist_type",
        "vod",
        "-hls_segment_filename",
        str(output_dir / "v_%03d.ts"),
        str(master_path),
    ]
    proc = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    if proc.returncode != 0:
        return None
    return str(master_path.relative_to(MEDIA_DIR)).replace("\\", "/")


def get_s3_client():
    if not boto3 or STORAGE_BACKEND != "s3":
        return None
    if not S3_BUCKET or not S3_ACCESS_KEY or not S3_SECRET_KEY:
        return None
    config = None
    if BotoConfig:
        config = BotoConfig(
            retries={"max_attempts": S3_MAX_RETRIES, "mode": "standard"},
            connect_timeout=S3_CONNECT_TIMEOUT_SEC,
            read_timeout=S3_READ_TIMEOUT_SEC,
        )
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL or None,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=config,
    )


def storage_upload_bytes(key: str, data: bytes, content_type: str) -> str:
    if STORAGE_BACKEND == "s3":
        client = get_s3_client()
        if not client:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="S3 is not configured")
        client.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)
        return f"s3://{S3_BUCKET}/{key}"
    path = MEDIA_DIR / key
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file_obj:
        file_obj.write(data)
    return key.replace("\\", "/")


def storage_signed_url(path: str) -> str | None:
    if not path.startswith("s3://"):
        return None
    client = get_s3_client()
    if not client:
        return None
    _, _, rest = path.partition("s3://")
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        return None
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=3600,
    )


def storage_public_url(path: str) -> str:
    if path.startswith("s3://"):
        _, _, rest = path.partition("s3://")
        bucket, _, key = rest.partition("/")
        if not bucket or not key:
            return ""
        if CDN_BASE_URL:
            return f"{CDN_BASE_URL}/{key}"
        signed = storage_signed_url(path)
        return signed or ""
    if MEDIA_REDIRECT_BASE_URL and path:
        return f"{MEDIA_REDIRECT_BASE_URL}/{path.lstrip('/')}"
    return f"/media/{path}" if path else ""


def create_notification(db: Session, user_id: int, kind: str, message: str, payload: dict | None = None) -> None:
    db.add(
        Notification(
            user_id=user_id,
            type=kind,
            message=message,
            payload_json=payload or {},
        )
    )


def update_processing_job(db: Session, video_id: int, status_value: str, output: dict | None = None, error: str = "") -> None:
    job = db.query(ProcessingJob).filter(ProcessingJob.video_id == video_id).order_by(desc(ProcessingJob.created_at)).first()
    if not job:
        job = ProcessingJob(video_id=video_id, status=status_value, error_message=error, output_json=output or {})
        db.add(job)
    else:
        job.status = status_value
        job.error_message = error
        if output is not None:
            job.output_json = output
        job.updated_at = datetime.now(timezone.utc)


def queue_impression_event(event_data: dict) -> bool:
    with analytics_lock:
        impression_events_buffer.append(event_data)
        return len(impression_events_buffer) >= ANALYTICS_BATCH_SIZE


def queue_progress_event(event_data: dict) -> bool:
    with analytics_lock:
        progress_events_buffer.append(event_data)
        return len(progress_events_buffer) >= ANALYTICS_BATCH_SIZE


def flush_analytics_buffers(db: Session | None = None) -> None:
    own_db = False
    if db is None:
        db = SessionLocal()
        own_db = True
    try:
        with analytics_lock:
            pending_impressions = impression_events_buffer[:]
            pending_progress = progress_events_buffer[:]
            impression_events_buffer.clear()
            progress_events_buffer.clear()

        merged_impressions: dict[tuple[int, str], dict] = {}
        for item in pending_impressions:
            key = (item["video_id"], item["viewer_key"])
            if key not in merged_impressions:
                merged_impressions[key] = item

        merged_progress: dict[tuple[int, str], dict] = {}
        for item in pending_progress:
            key = (item["video_id"], item["viewer_key"])
            existing = merged_progress.get(key)
            if not existing:
                merged_progress[key] = item.copy()
                continue
            existing["progress_pct"] = max(existing["progress_pct"], item["progress_pct"])
            existing["seconds_watched"] = max(existing["seconds_watched"], item["seconds_watched"])
            existing["duration_seconds"] = max(existing["duration_seconds"], item["duration_seconds"])
            if not existing.get("user_id") and item.get("user_id"):
                existing["user_id"] = item.get("user_id")

        for item in merged_impressions.values():
            exists = (
                db.query(VideoImpression)
                .filter(VideoImpression.video_id == item["video_id"], VideoImpression.viewer_key == item["viewer_key"])
                .first()
            )
            if exists:
                continue
            db.add(
                VideoImpression(
                    video_id=item["video_id"],
                    user_id=item.get("user_id"),
                    viewer_key=item["viewer_key"],
                    source=item.get("source", "feed")[:30],
                )
            )

        for item in merged_progress.values():
            row = (
                db.query(VideoWatchProgress)
                .filter(VideoWatchProgress.video_id == item["video_id"], VideoWatchProgress.viewer_key == item["viewer_key"])
                .first()
            )
            if not row:
                db.add(
                    VideoWatchProgress(
                        video_id=item["video_id"],
                        user_id=item.get("user_id"),
                        viewer_key=item["viewer_key"],
                        max_progress_pct=item["progress_pct"],
                        max_seconds_watched=item["seconds_watched"],
                        duration_seconds=item["duration_seconds"],
                        updated_at=datetime.now(timezone.utc),
                    )
                )
            else:
                row.max_progress_pct = max(row.max_progress_pct, item["progress_pct"])
                row.max_seconds_watched = max(row.max_seconds_watched, item["seconds_watched"])
                row.duration_seconds = max(row.duration_seconds, item["duration_seconds"])
                row.updated_at = datetime.now(timezone.utc)
        db.commit()
    except IntegrityError:
        db.rollback()
        # Rare race fallback: apply idempotently row-by-row.
        for item in merged_impressions.values():
            try:
                exists = (
                    db.query(VideoImpression)
                    .filter(VideoImpression.video_id == item["video_id"], VideoImpression.viewer_key == item["viewer_key"])
                    .first()
                )
                if not exists:
                    db.add(
                        VideoImpression(
                            video_id=item["video_id"],
                            user_id=item.get("user_id"),
                            viewer_key=item["viewer_key"],
                            source=item.get("source", "feed")[:30],
                        )
                    )
                db.commit()
            except IntegrityError:
                db.rollback()
        for item in merged_progress.values():
            try:
                row = (
                    db.query(VideoWatchProgress)
                    .filter(VideoWatchProgress.video_id == item["video_id"], VideoWatchProgress.viewer_key == item["viewer_key"])
                    .first()
                )
                if not row:
                    db.add(
                        VideoWatchProgress(
                            video_id=item["video_id"],
                            user_id=item.get("user_id"),
                            viewer_key=item["viewer_key"],
                            max_progress_pct=item["progress_pct"],
                            max_seconds_watched=item["seconds_watched"],
                            duration_seconds=item["duration_seconds"],
                            updated_at=datetime.now(timezone.utc),
                        )
                    )
                else:
                    row.max_progress_pct = max(row.max_progress_pct, item["progress_pct"])
                    row.max_seconds_watched = max(row.max_seconds_watched, item["seconds_watched"])
                    row.duration_seconds = max(row.duration_seconds, item["duration_seconds"])
                    row.updated_at = datetime.now(timezone.utc)
                db.commit()
            except IntegrityError:
                db.rollback()
                row = (
                    db.query(VideoWatchProgress)
                    .filter(VideoWatchProgress.video_id == item["video_id"], VideoWatchProgress.viewer_key == item["viewer_key"])
                    .first()
                )
                if row:
                    row.max_progress_pct = max(row.max_progress_pct, item["progress_pct"])
                    row.max_seconds_watched = max(row.max_seconds_watched, item["seconds_watched"])
                    row.duration_seconds = max(row.duration_seconds, item["duration_seconds"])
                    row.updated_at = datetime.now(timezone.utc)
                    db.commit()
    finally:
        if own_db:
            db.close()


def analytics_flusher_loop() -> None:
    while not analytics_stop_event.is_set():
        analytics_stop_event.wait(ANALYTICS_FLUSH_INTERVAL_SEC)
        if analytics_stop_event.is_set():
            break
        try:
            flush_analytics_buffers()
        except Exception:
            logger.exception("analytics flusher iteration failed")


def ensure_video_moderation_columns() -> None:
    inspector = inspect(engine)
    if "videos" not in inspector.get_table_names():
        return
    column_names = {column["name"] for column in inspector.get_columns("videos")}
    with engine.begin() as conn:
        if "moderation_status" not in column_names:
            conn.execute(text("ALTER TABLE videos ADD COLUMN moderation_status VARCHAR(16)"))
            conn.execute(text("UPDATE videos SET moderation_status = 'approved' WHERE moderation_status IS NULL"))
        if "approved_at" not in column_names:
            conn.execute(text("ALTER TABLE videos ADD COLUMN approved_at DATETIME"))


def iter_file_part(path: Path, start: int, end: int, chunk_size: int = 1024 * 1024):
    with path.open("rb") as file_obj:
        file_obj.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            read_len = min(chunk_size, remaining)
            data = file_obj.read(read_len)
            if not data:
                break
            remaining -= len(data)
            yield data


def process_video_pipeline(video_id: int, local_video_path: str, original_rel_path: str) -> None:
    db = SessionLocal()
    try:
        update_processing_job(db, video_id, "processing", output={"stage": "start"})
        db.commit()

        path = Path(local_video_path)
        maybe_generate_renditions(path)
        hls_rel = maybe_generate_hls(path, MEDIA_DIR / "hls" / f"video_{video_id}")
        qualities = available_qualities(path)

        output = {
            "stage": "ready",
            "qualities": qualities,
            "hls_master": hls_rel or "",
            "source": original_rel_path,
        }
        update_processing_job(db, video_id, "ready", output=output, error="")
        db.commit()
    except Exception as err:  # pragma: no cover
        update_processing_job(db, video_id, "failed", output={"stage": "failed"}, error=str(err))
        db.commit()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    ensure_video_moderation_columns()
    analytics_stop_event.clear()
    flusher = threading.Thread(target=analytics_flusher_loop, name="analytics-flusher", daemon=True)
    flusher.start()
    yield
    analytics_stop_event.set()
    flush_analytics_buffers()


app = FastAPI(title="HOI4 Tube API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(math.ceil(q * len(ordered)) - 1)))
    return ordered[idx]


@app.middleware("http")
async def request_metrics_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    request.state.request_id = request_id
    started = time.perf_counter()
    try:
        response = await call_next(request)
        status_code = response.status_code
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with metrics_lock:
            metrics_state["requests_total"] += 1
            if status_code >= 500:
                metrics_state["errors_total"] += 1
            request_durations_ms.append(elapsed_ms)
        logger.info(
            "request completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status": status_code,
                "latency_ms": round(elapsed_ms, 2),
            },
        )
        response.headers["x-request-id"] = request_id
        response.headers["x-content-type-options"] = "nosniff"
        response.headers["x-frame-options"] = "DENY"
        response.headers["referrer-policy"] = "strict-origin-when-cross-origin"
        response.headers["content-security-policy"] = "default-src 'self'; img-src 'self' data: https:; media-src 'self' https: blob:; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' https: http:; frame-ancestors 'none';"
        return response
    except Exception:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with metrics_lock:
            metrics_state["requests_total"] += 1
            metrics_state["errors_total"] += 1
            request_durations_ms.append(elapsed_ms)
        logger.exception("request failed", extra={"request_id": request_id, "path": request.url.path})
        raise


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    flush_analytics_buffers()
    uptime_sec = max(1.0, time.time() - app_started_at)
    with analytics_lock:
        queue_lag = len(impression_events_buffer) + len(progress_events_buffer)
    with metrics_lock:
        durations = list(request_durations_ms)
        requests_total = metrics_state["requests_total"]
        errors_total = metrics_state["errors_total"]
        db_query_count = metrics_state["db_query_count"]
        db_query_time_ms = metrics_state["db_query_time_ms"]
    p95 = percentile(durations, 0.95)
    error_rate = (errors_total / requests_total) if requests_total else 0.0
    rps = requests_total / uptime_sec
    avg_db_query_ms = (db_query_time_ms / db_query_count) if db_query_count else 0.0
    body = (
        f"api_requests_total {requests_total}\n"
        f"api_errors_total {errors_total}\n"
        f"api_error_rate {error_rate:.6f}\n"
        f"api_rps {rps:.3f}\n"
        f"api_latency_p95_ms {p95:.2f}\n"
        f"db_query_count_total {db_query_count}\n"
        f"db_query_time_total_ms {db_query_time_ms:.2f}\n"
        f"db_query_avg_ms {avg_db_query_ms:.2f}\n"
        f"analytics_queue_lag {queue_lag}\n"
    )
    return PlainTextResponse(content=body)


@app.post("/auth/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register(payload: UserRegisterIn, request: Request, db: Session = Depends(get_db)) -> UserOut:
    check_rate_limit(request, key="register", max_requests=20, window_seconds=60)
    safe_username = sanitize_text_input(payload.username, max_len=50)
    if db.query(User).filter((User.username == safe_username) | (User.email == payload.email)).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")
    is_first_user = db.query(func.count(User.id)).scalar() == 0
    user = User(
        username=safe_username,
        email=payload.email,
        password_hash=hash_password(payload.password),
        is_admin=is_first_user,
        can_upload=is_first_user,
    )
    db.add(user)
    db.flush()
    channel = Channel(owner_id=user.id, name=safe_username, description="My HOI4 channel")
    db.add(channel)
    db.commit()
    db.refresh(user)
    return UserOut.model_validate(user)


@app.post("/auth/login", response_model=TokenOut)
def login(payload: UserLoginIn, request: Request, db: Session = Depends(get_db)) -> TokenOut:
    check_rate_limit(request, key="login", max_requests=30, window_seconds=60)
    # Built-in local admin shortcut:
    # login: admin / password: admin
    if payload.email == "admin" and payload.password == "admin":
        user = db.query(User).filter(User.username == "admin").first()
        if not user:
            user = User(
                username="admin",
                email="admin@moorluckhosting.com",
                password_hash=hash_password("admin"),
                is_admin=True,
                can_upload=True,
            )
            db.add(user)
            db.flush()
            db.add(Channel(owner_id=user.id, name="admin", description="Admin channel"))
            db.commit()
            db.refresh(user)
        else:
            updated = False
            if not user.is_admin:
                user.is_admin = True
                user.can_upload = True
                updated = True
            if user.email.endswith("@local.test"):
                user.email = f"admin+{user.id}@moorluckhosting.com"
                updated = True
            if updated:
                db.commit()
                db.refresh(user)
        return TokenOut(access_token=create_access_token(user.id))

    user = db.query(User).filter((User.email == payload.email) | (User.username == payload.email)).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    return TokenOut(access_token=create_access_token(user.id))


@app.get("/auth/me", response_model=UserOut)
def me(user: User = Depends(get_current_user)) -> UserOut:
    return UserOut.model_validate(user)


@app.patch("/admin/users/{user_id}/upload-access", response_model=UserOut)
def set_upload_access(
    user_id: int,
    payload: ToggleUploadIn,
    _: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> UserOut:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    user.can_upload = payload.can_upload
    db.commit()
    db.refresh(user)
    return UserOut.model_validate(user)


@app.get("/channels/{channel_id}", response_model=ChannelOut)
def get_channel(channel_id: int, db: Session = Depends(get_db)) -> ChannelOut:
    channel = db.get(Channel, channel_id)
    if not channel:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Channel not found")
    subscribers_count = db.query(func.count(Subscription.id)).filter(Subscription.channel_id == channel.id).scalar() or 0
    return ChannelOut(
        id=channel.id,
        owner_id=channel.owner_id,
        name=channel.name,
        description=channel.description,
        avatar_url=channel.avatar_url,
        subscribers_count=subscribers_count,
    )


@app.post("/channels/{channel_id}/subscribe", status_code=status.HTTP_204_NO_CONTENT)
def subscribe(channel_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    channel = db.get(Channel, channel_id)
    if not channel:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Channel not found")
    if channel.owner_id == user.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot subscribe to own channel")
    existing = db.query(Subscription).filter_by(channel_id=channel_id, subscriber_id=user.id).first()
    if not existing:
        db.add(Subscription(channel_id=channel_id, subscriber_id=user.id))
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.delete("/channels/{channel_id}/subscribe", status_code=status.HTTP_204_NO_CONTENT)
def unsubscribe(channel_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    sub = db.query(Subscription).filter_by(channel_id=channel_id, subscriber_id=user.id).first()
    if sub:
        db.delete(sub)
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/channels/{channel_id}/subscription-status", response_model=SubscriptionStatusOut)
def subscription_status(channel_id: int, user: User | None = Depends(get_optional_user), db: Session = Depends(get_db)) -> SubscriptionStatusOut:
    if not user:
        return SubscriptionStatusOut(is_subscribed=False)
    exists = db.query(Subscription.id).filter(Subscription.channel_id == channel_id, Subscription.subscriber_id == user.id).first()
    return SubscriptionStatusOut(is_subscribed=bool(exists))


@app.get("/videos", response_model=VideoListOut)
def list_videos(
    cursor: int | None = Query(default=None),
    limit: int = Query(default=12, ge=1, le=50),
    genre: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    q: str | None = Query(default=None),
    surface: str = Query(default="home"),
    request: Request = None,
    user: User | None = Depends(get_optional_user),
    db: Session = Depends(get_db),
) -> VideoListOut:
    flush_analytics_buffers(db)
    blocked_video_subquery = db.query(BlockedVideo.video_id)
    query = db.query(Video).filter(Video.id.notin_(blocked_video_subquery))
    if genre:
        query = query.filter(Video.genre == genre)
    if owner_id:
        query = query.filter(Video.owner_id == owner_id)
    if owner_id:
        if not user or (user.id != owner_id and not user.is_admin):
            query = query.filter(Video.moderation_status == "approved")
    else:
        query = query.filter(Video.moderation_status == "approved")
    if q:
        clean_q = sanitize_text_input(q, max_len=120)
        if clean_q:
            like = build_safe_ilike_pattern(clean_q)
            query = query.filter((Video.title.ilike(like, escape="\\")) | (Video.description.ilike(like, escape="\\")))
    if cursor:
        query = query.filter(Video.id < cursor)

    # Candidate generation.
    candidate_limit = min(500, max(120, limit * 12))
    candidates = query.order_by(desc(Video.created_at)).limit(candidate_limit).all()
    subscribed_channel_ids: set[int] = set()
    genre_affinity: dict[str, float] = {}
    if user:
        subscribed_rows = db.query(Subscription.channel_id).filter(Subscription.subscriber_id == user.id).all()
        subscribed_channel_ids = {channel_id for (channel_id,) in subscribed_rows}
        genre_affinity = get_user_genre_affinity(db, user.id)
    viewer_key = build_viewer_key(request, user) if request else "anon:feed"
    ab_bucket = get_ab_bucket(user, viewer_key, feature_name=f"ranking_{surface}")

    ranked = rank_videos(
        db=db,
        videos=candidates,
        user=user,
        subscribed_channel_ids=subscribed_channel_ids,
        genre_affinity=genre_affinity,
        limit=limit,
        surface=surface,
        ab_bucket=ab_bucket,
    )
    has_more = len(ranked) > limit
    selected = ranked[:limit]
    next_cursor = min((item.id for item in selected), default=None) if has_more else None
    return VideoListOut(items=[video_to_out(db, item) for item in selected], next_cursor=next_cursor)


@app.get("/search/suggestions", response_model=SearchSuggestionsOut)
def search_suggestions(q: str = Query(default="", min_length=1), db: Session = Depends(get_db)) -> SearchSuggestionsOut:
    needle = sanitize_text_input(q, max_len=120)
    if not needle:
        return SearchSuggestionsOut(items=[])
    pattern = build_safe_ilike_pattern(needle)
    title_rows = (
        db.query(Video.title)
        .filter(Video.moderation_status == "approved")
        .filter(Video.title.ilike(pattern, escape="\\"))
        .order_by(desc(Video.created_at))
        .limit(6)
        .all()
    )
    channel_rows = db.query(Channel.name).filter(Channel.name.ilike(pattern, escape="\\")).order_by(desc(Channel.created_at)).limit(4).all()
    items: list[str] = []
    for (title,) in title_rows:
        if title and title not in items:
            items.append(title)
    for (name,) in channel_rows:
        if name and name not in items:
            items.append(name)
    return SearchSuggestionsOut(items=items[:10])


@app.get("/me/watch-later", response_model=list[VideoOut])
def list_watch_later(user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[VideoOut]:
    blocked_video_subquery = db.query(BlockedVideo.video_id)
    rows = (
        db.query(Video)
        .join(WatchLater, WatchLater.video_id == Video.id)
        .filter(WatchLater.user_id == user.id)
        .filter(Video.id.notin_(blocked_video_subquery))
        .filter(Video.moderation_status == "approved")
        .order_by(desc(WatchLater.created_at))
        .all()
    )
    return [video_to_out(db, item) for item in rows]


@app.post("/videos/{video_id}/watch-later", status_code=status.HTTP_204_NO_CONTENT)
def add_watch_later(video_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    exists = db.query(WatchLater).filter(WatchLater.user_id == user.id, WatchLater.video_id == video_id).first()
    if not exists:
        db.add(WatchLater(user_id=user.id, video_id=video_id))
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.delete("/videos/{video_id}/watch-later", status_code=status.HTTP_204_NO_CONTENT)
def remove_watch_later(video_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    row = db.query(WatchLater).filter(WatchLater.user_id == user.id, WatchLater.video_id == video_id).first()
    if row:
        db.delete(row)
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/me/continue-watching", response_model=list[ContinueWatchingItemOut])
def continue_watching(user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[ContinueWatchingItemOut]:
    flush_analytics_buffers(db)
    blocked_video_subquery = db.query(BlockedVideo.video_id)
    rows = (
        db.query(VideoWatchProgress, Video)
        .join(Video, Video.id == VideoWatchProgress.video_id)
        .filter(VideoWatchProgress.user_id == user.id)
        .filter(Video.id.notin_(blocked_video_subquery))
        .filter(Video.moderation_status == "approved")
        .filter(VideoWatchProgress.max_progress_pct > 2)
        .filter(VideoWatchProgress.max_progress_pct < 98)
        .order_by(desc(VideoWatchProgress.updated_at))
        .limit(20)
        .all()
    )
    return [
        ContinueWatchingItemOut(
            video=video_to_out(db, video),
            progress_pct=round(progress.max_progress_pct, 2),
            seconds_watched=round(progress.max_seconds_watched, 2),
        )
        for progress, video in rows
    ]


@app.get("/videos/{video_id}", response_model=VideoOut)
def get_video(
    video_id: int,
    request: Request,
    user: User | None = Depends(get_optional_user),
    db: Session = Depends(get_db),
) -> VideoOut:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if db.query(BlockedVideo).filter(BlockedVideo.video_id == video_id).first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    apply_unique_view(db, request, video, user)
    db.commit()
    db.refresh(video)
    return video_to_out(db, video)


@app.post("/videos/upload", response_model=VideoOut, status_code=status.HTTP_201_CREATED)
def upload_video(
    title: str = Form(...),
    description: str = Form(default=""),
    genre: str = Form(default="hoi4_letsplay"),
    video_file: UploadFile = File(...),
    thumbnail: UploadFile | None = File(default=None),
    background_tasks: BackgroundTasks = None,
    user: User = Depends(require_uploader),
    db: Session = Depends(get_db),
) -> VideoOut:
    if not video_file.content_type or not video_file.content_type.startswith("video/"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File must be a video")
    content_length_raw = (video_file.headers or {}).get("content-length")
    if content_length_raw:
        try:
            if int(content_length_raw) > MAX_VIDEO_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    detail=f"Video file is too large. Max size is {MAX_VIDEO_BYTES // (1024 * 1024)} MB",
                )
        except ValueError:
            pass

    channel = db.query(Channel).filter(Channel.owner_id == user.id).first()
    if not channel:
        channel = Channel(owner_id=user.id, name=user.username)
        db.add(channel)
        db.flush()

    video_ext = Path(video_file.filename or "video.mp4").suffix or ".mp4"
    video_name = f"{int(datetime.now(timezone.utc).timestamp())}_{user.id}{video_ext}"
    rel_video_path = Path("videos") / video_name
    video_bytes = read_upload_limited(video_file, MAX_VIDEO_BYTES)
    stored_video_path = storage_upload_bytes(str(rel_video_path).replace("\\", "/"), video_bytes, video_file.content_type or "video/mp4")
    abs_video_path = MEDIA_DIR / rel_video_path

    rel_thumb_path = ""
    if thumbnail:
        thumb_ext = Path(thumbnail.filename or "thumb.jpg").suffix or ".jpg"
        thumb_name = f"{int(datetime.now(timezone.utc).timestamp())}_{user.id}{thumb_ext}"
        rel_thumb_path = str(Path("thumbs") / thumb_name)
        thumb_bytes = thumbnail.file.read()
        rel_thumb_path = storage_upload_bytes(rel_thumb_path.replace("\\", "/"), thumb_bytes, thumbnail.content_type or "image/jpeg")

    safe_title = sanitize_text_input(title, max_len=255)
    safe_description = sanitize_text_input(description, max_len=5000, allow_newlines=True)
    safe_genre = sanitize_text_input(genre, max_len=64)
    if not safe_title:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Title is required")

    moderation_status = "approved" if user.is_admin else "pending"
    video = Video(
        owner_id=user.id,
        channel_id=channel.id,
        title=safe_title,
        description=safe_description,
        genre=safe_genre or "hoi4_letsplay",
        file_path=stored_video_path,
        thumbnail_path=rel_thumb_path,
        moderation_status=moderation_status,
        approved_at=datetime.now(timezone.utc) if moderation_status == "approved" else None,
    )
    db.add(video)
    db.flush()
    update_processing_job(db, video.id, "processing", output={"stage": "queued"}, error="")
    db.commit()
    db.refresh(video)

    if STORAGE_BACKEND == "local" and abs_video_path.exists():
        if USE_CELERY:
            try:
                from tasks import process_video_task

                process_video_task.delay(video.id, str(abs_video_path), str(rel_video_path).replace("\\", "/"))
            except Exception:
                if background_tasks:
                    background_tasks.add_task(process_video_pipeline, video.id, str(abs_video_path), str(rel_video_path).replace("\\", "/"))
                else:
                    process_video_pipeline(video.id, str(abs_video_path), str(rel_video_path).replace("\\", "/"))
        elif background_tasks:
            background_tasks.add_task(process_video_pipeline, video.id, str(abs_video_path), str(rel_video_path).replace("\\", "/"))
        else:
            process_video_pipeline(video.id, str(abs_video_path), str(rel_video_path).replace("\\", "/"))
    else:
        update_processing_job(
            db,
            video.id,
            "ready",
            output={"stage": "ready", "qualities": ["source"], "hls_master": "", "source": video.file_path},
            error="",
        )
        db.commit()

    if moderation_status == "approved":
        subscribers = db.query(Subscription.subscriber_id).filter(Subscription.channel_id == channel.id).all()
        for (subscriber_id,) in subscribers:
            if subscriber_id == user.id:
                continue
            create_notification(
                db,
                subscriber_id,
                "new_video",
                f"Новый ролик на канале {channel.name}: {safe_title}",
                payload={"video_id": video.id, "channel_id": channel.id},
            )
        db.commit()
    return video_to_out(db, video)


@app.get("/videos/{video_id}/qualities", response_model=VideoQualitiesOut)
def get_video_qualities(video_id: int, user: User | None = Depends(get_optional_user), db: Session = Depends(get_db)) -> VideoQualitiesOut:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    if video.file_path.startswith("s3://"):
        return VideoQualitiesOut(items=["source"])
    abs_path = MEDIA_DIR / video.file_path
    job = db.query(ProcessingJob).filter(ProcessingJob.video_id == video.id).order_by(desc(ProcessingJob.created_at)).first()
    if job and isinstance(job.output_json, dict) and job.output_json.get("qualities"):
        return VideoQualitiesOut(items=job.output_json.get("qualities", ["source"]))
    if abs_path.exists():
        return VideoQualitiesOut(items=available_qualities(abs_path))
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video file not found")


@app.delete("/videos/{video_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_video(video_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not (user.is_admin or video.owner_id == user.id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    abs_video_path = MEDIA_DIR / video.file_path
    abs_thumb_path = MEDIA_DIR / video.thumbnail_path if video.thumbnail_path else None

    # Explicit cleanup for sqlite/dev mode even if FK cascades are unavailable.
    db.query(VideoLike).filter(VideoLike.video_id == video.id).delete()
    db.query(Comment).filter(Comment.video_id == video.id).delete()
    db.query(VideoView).filter(VideoView.video_id == video.id).delete()
    db.query(VideoImpression).filter(VideoImpression.video_id == video.id).delete()
    db.query(VideoWatchProgress).filter(VideoWatchProgress.video_id == video.id).delete()
    db.query(ProcessingJob).filter(ProcessingJob.video_id == video.id).delete()
    db.query(WatchLater).filter(WatchLater.video_id == video.id).delete()
    db.query(BlockedVideo).filter(BlockedVideo.video_id == video.id).delete()
    db.query(VideoReport).filter(VideoReport.video_id == video.id).delete()
    db.delete(video)
    db.commit()

    if abs_video_path.exists() and video.file_path.startswith("s3://") is False:
        abs_video_path.unlink(missing_ok=True)
    if abs_thumb_path and abs_thumb_path.exists():
        abs_thumb_path.unlink(missing_ok=True)
    hls_dir = MEDIA_DIR / "hls" / f"video_{video.id}"
    if hls_dir.exists():
        shutil.rmtree(hls_dir, ignore_errors=True)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/videos/{video_id}/stream")
def stream_video(
    video_id: int,
    quality: str | None = Query(default=None),
    range: str | None = Header(default=None),
    user: User | None = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if db.query(BlockedVideo).filter(BlockedVideo.video_id == video_id).first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    if video.file_path.startswith("s3://"):
        storage_url = storage_public_url(video.file_path) or storage_signed_url(video.file_path)
        if not storage_url:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Storage URL unavailable")
        return RedirectResponse(url=storage_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)
    abs_path = MEDIA_DIR / video.file_path
    if quality and quality != "source":
        candidate = quality_variant_path(abs_path, quality)
        if candidate.exists():
            abs_path = candidate
    if not abs_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video file not found")
    if MEDIA_REDIRECT_BASE_URL:
        media_rel = str(abs_path.relative_to(MEDIA_DIR)).replace("\\", "/")
        return RedirectResponse(url=f"{MEDIA_REDIRECT_BASE_URL}/{media_rel}", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    size = abs_path.stat().st_size
    start, end = parse_range_header(range, size)
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{size}",
        "Content-Length": str(end - start + 1),
    }
    return StreamingResponse(
        iter_file_part(abs_path, start, end),
        media_type="video/mp4",
        status_code=status.HTTP_206_PARTIAL_CONTENT if range else status.HTTP_200_OK,
        headers=headers,
    )


@app.get("/videos/{video_id}/hls")
def get_hls_master(video_id: int, user: User | None = Depends(get_optional_user), db: Session = Depends(get_db)) -> dict[str, str]:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    job = db.query(ProcessingJob).filter(ProcessingJob.video_id == video.id).order_by(desc(ProcessingJob.created_at)).first()
    if not job or job.status != "ready":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Video is still processing")
    output = job.output_json or {}
    hls_rel = output.get("hls_master", "")
    if not hls_rel:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="HLS not available")
    return {"hls_url": storage_public_url(hls_rel)}


@app.get("/videos/{video_id}/processing-status", response_model=ProcessingJobOut)
def get_processing_status(video_id: int, db: Session = Depends(get_db)) -> ProcessingJobOut:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    job = db.query(ProcessingJob).filter(ProcessingJob.video_id == video_id).order_by(desc(ProcessingJob.created_at)).first()
    if not job:
        return ProcessingJobOut(video_id=video_id, status="ready", error_message="", output={})
    return ProcessingJobOut(
        video_id=video_id,
        status=job.status,
        error_message=job.error_message or "",
        output=job.output_json or {},
    )


@app.post("/videos/{video_id}/like", status_code=status.HTTP_204_NO_CONTENT)
def like_video(video_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    existing = db.query(VideoLike).filter_by(video_id=video_id, user_id=user.id).first()
    if not existing:
        db.add(VideoLike(video_id=video_id, user_id=user.id))
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.delete("/videos/{video_id}/like", status_code=status.HTTP_204_NO_CONTENT)
def unlike_video(video_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    like = db.query(VideoLike).filter_by(video_id=video_id, user_id=user.id).first()
    if like:
        db.delete(like)
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/videos/{video_id}/like-status", response_model=LikeStatusOut)
def like_status(video_id: int, user: User | None = Depends(get_optional_user), db: Session = Depends(get_db)) -> LikeStatusOut:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    likes_count = db.query(func.count(VideoLike.id)).filter(VideoLike.video_id == video_id).scalar() or 0
    if not user:
        return LikeStatusOut(is_liked=False, likes_count=likes_count)
    is_liked = db.query(VideoLike.id).filter(VideoLike.video_id == video_id, VideoLike.user_id == user.id).first() is not None
    return LikeStatusOut(is_liked=is_liked, likes_count=likes_count)


@app.get("/videos/{video_id}/comments", response_model=list[CommentOut])
def list_comments(video_id: int, user: User | None = Depends(get_optional_user), db: Session = Depends(get_db)) -> list[CommentOut]:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    blocked_subq = db.query(BlockedComment.comment_id)
    rows = (
        db.query(Comment, User.username)
        .join(User, Comment.user_id == User.id)
        .filter(Comment.video_id == video_id)
        .filter(Comment.id.notin_(blocked_subq))
        .order_by(Comment.created_at.asc())
        .all()
    )
    return [
        CommentOut(
            id=comment.id,
            user_id=comment.user_id,
            username=username,
            video_id=comment.video_id,
            text=comment.text,
            created_at=comment.created_at,
        )
        for comment, username in rows
    ]


@app.post("/videos/{video_id}/comments", response_model=CommentOut, status_code=status.HTTP_201_CREATED)
def add_comment(
    video_id: int,
    payload: CommentIn,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> CommentOut:
    check_rate_limit(request, key=f"comment:user:{user.id}", max_requests=15, window_seconds=60)
    safe_text = sanitize_text_input(payload.text, max_len=2000, allow_newlines=True)
    if not safe_text:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Comment is empty")
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    comment = Comment(video_id=video_id, user_id=user.id, text=safe_text)
    db.add(comment)
    if video.owner_id != user.id:
        create_notification(
            db,
            video.owner_id,
            "new_comment",
            f"Новый комментарий к видео '{video.title}'",
            payload={"video_id": video.id, "comment_text": safe_text[:120]},
        )
    db.commit()
    db.refresh(comment)
    return CommentOut(
        id=comment.id,
        user_id=comment.user_id,
        username=user.username,
        video_id=comment.video_id,
        text=comment.text,
        created_at=comment.created_at,
    )


@app.post("/analytics/impressions", status_code=status.HTTP_204_NO_CONTENT)
def track_impression(
    payload: ImpressionIn,
    request: Request,
    user: User | None = Depends(get_optional_user),
    db: Session = Depends(get_db),
) -> Response:
    video = db.get(Video, payload.video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    viewer_key = build_viewer_key(request, user)
    should_flush = queue_impression_event(
        {
            "video_id": payload.video_id,
            "user_id": user.id if user else None,
            "viewer_key": viewer_key,
            "source": payload.source[:30],
        }
    )
    if should_flush:
        flush_analytics_buffers(db)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/analytics/watch-progress", status_code=status.HTTP_204_NO_CONTENT)
def track_watch_progress(
    payload: WatchProgressIn,
    request: Request,
    user: User | None = Depends(get_optional_user),
    db: Session = Depends(get_db),
) -> Response:
    video = db.get(Video, payload.video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    progress_pct = max(0.0, min(100.0, payload.progress_pct))
    seconds = max(0.0, payload.seconds_watched)
    duration = max(0.0, payload.duration_seconds)
    viewer_key = build_viewer_key(request, user)
    should_flush = queue_progress_event(
        {
            "video_id": payload.video_id,
            "user_id": user.id if user else None,
            "viewer_key": viewer_key,
            "progress_pct": progress_pct,
            "seconds_watched": seconds,
            "duration_seconds": duration,
        }
    )
    if should_flush:
        flush_analytics_buffers(db)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/analytics/batch", status_code=status.HTTP_204_NO_CONTENT)
def track_analytics_batch(
    payload: AnalyticsBatchIn,
    request: Request,
    user: User | None = Depends(get_optional_user),
    db: Session = Depends(get_db),
) -> Response:
    viewer_key = build_viewer_key(request, user)
    for item in payload.impressions[:500]:
        if not db.get(Video, item.video_id):
            continue
        should_flush = queue_impression_event(
            {
                "video_id": item.video_id,
                "user_id": user.id if user else None,
                "viewer_key": viewer_key,
                "source": item.source[:30],
            }
        )
        if should_flush:
            flush_analytics_buffers(db)
    for item in payload.watch_progress[:500]:
        if not db.get(Video, item.video_id):
            continue
        should_flush = queue_progress_event(
            {
                "video_id": item.video_id,
                "user_id": user.id if user else None,
                "viewer_key": viewer_key,
                "progress_pct": max(0.0, min(100.0, item.progress_pct)),
                "seconds_watched": max(0.0, item.seconds_watched),
                "duration_seconds": max(0.0, item.duration_seconds),
            }
        )
        if should_flush:
            flush_analytics_buffers(db)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/studio/videos/{video_id}/analytics", response_model=VideoAnalyticsOut)
def get_video_analytics(video_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> VideoAnalyticsOut:
    flush_analytics_buffers(db)
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not (user.is_admin or video.owner_id == user.id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    impressions = db.query(func.count(VideoImpression.id)).filter(VideoImpression.video_id == video_id).scalar() or 0
    unique_views = db.query(func.count(VideoView.id)).filter(VideoView.video_id == video_id).scalar() or 0
    avg_watch = db.query(func.avg(VideoWatchProgress.max_progress_pct)).filter(VideoWatchProgress.video_id == video_id).scalar() or 0.0
    ctr = (unique_views / impressions * 100.0) if impressions > 0 else 0.0

    progress_values = db.query(VideoWatchProgress.max_progress_pct).filter(VideoWatchProgress.video_id == video_id).all()
    values = [val for (val,) in progress_values]
    retention = [
        RetentionPoint(bucket="0-25%", viewers=sum(1 for val in values if 0 <= val < 25)),
        RetentionPoint(bucket="25-50%", viewers=sum(1 for val in values if 25 <= val < 50)),
        RetentionPoint(bucket="50-75%", viewers=sum(1 for val in values if 50 <= val < 75)),
        RetentionPoint(bucket="75-100%", viewers=sum(1 for val in values if 75 <= val <= 100)),
    ]
    return VideoAnalyticsOut(
        video_id=video_id,
        impressions=impressions,
        unique_views=unique_views,
        ctr_percent=round(ctr, 2),
        avg_watch_percent=round(float(avg_watch), 2),
        retention=retention,
    )


@app.post("/reports/video/{video_id}", status_code=status.HTTP_201_CREATED)
def report_video(
    video_id: int,
    payload: ReportIn,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    check_rate_limit(request, key=f"report-video:user:{user.id}", max_requests=20, window_seconds=60)
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    if not can_access_video(video, user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video is pending moderation")
    safe_reason = sanitize_text_input(payload.reason, max_len=255, allow_newlines=True)
    if not safe_reason:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Reason is required")
    report = VideoReport(reporter_id=user.id, video_id=video_id, reason=safe_reason, status="open")
    db.add(report)
    db.commit()
    return {"status": "reported"}


@app.post("/reports/comment/{comment_id}", status_code=status.HTTP_201_CREATED)
def report_comment(
    comment_id: int,
    payload: ReportIn,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    check_rate_limit(request, key=f"report-comment:user:{user.id}", max_requests=30, window_seconds=60)
    if not db.get(Comment, comment_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Comment not found")
    safe_reason = sanitize_text_input(payload.reason, max_len=255, allow_newlines=True)
    if not safe_reason:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Reason is required")
    report = CommentReport(reporter_id=user.id, comment_id=comment_id, reason=safe_reason, status="open")
    db.add(report)
    db.commit()
    return {"status": "reported"}


@app.get("/moderation/reports", response_model=list[ModerationReportOut])
def moderation_reports(status_filter: str = Query(default="open"), _: User = Depends(require_admin), db: Session = Depends(get_db)) -> list[ModerationReportOut]:
    video_rows = db.query(VideoReport).filter(VideoReport.status == status_filter).order_by(desc(VideoReport.created_at)).all()
    comment_rows = db.query(CommentReport).filter(CommentReport.status == status_filter).order_by(desc(CommentReport.created_at)).all()
    out: list[ModerationReportOut] = []
    for row in video_rows:
        out.append(
            ModerationReportOut(
                kind="video",
                report_id=row.id,
                target_id=row.video_id,
                reason=row.reason,
                status=row.status,
                created_at=row.created_at,
            )
        )
    for row in comment_rows:
        out.append(
            ModerationReportOut(
                kind="comment",
                report_id=row.id,
                target_id=row.comment_id,
                reason=row.reason,
                status=row.status,
                created_at=row.created_at,
            )
        )
    out.sort(key=lambda item: item.created_at, reverse=True)
    return out


@app.get("/moderation/videos/pending", response_model=list[VideoOut])
def moderation_pending_videos(_: User = Depends(require_admin), db: Session = Depends(get_db)) -> list[VideoOut]:
    rows = db.query(Video).filter(Video.moderation_status == "pending").order_by(desc(Video.created_at)).limit(200).all()
    return [video_to_out(db, row) for row in rows]


@app.patch("/moderation/videos/{video_id}", response_model=VideoOut)
def moderation_video_action(
    video_id: int,
    payload: VideoModerationIn,
    _: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> VideoOut:
    video = db.get(Video, video_id)
    if not video:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Video not found")
    action = payload.action.strip().lower()
    if action not in ("approve", "reject"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown action")
    if action == "approve":
        was_approved = video.moderation_status == "approved"
        video.moderation_status = "approved"
        video.approved_at = datetime.now(timezone.utc)
        if not was_approved:
            channel = db.get(Channel, video.channel_id)
            channel_name = channel.name if channel else ""
            subscribers = db.query(Subscription.subscriber_id).filter(Subscription.channel_id == video.channel_id).all()
            for (subscriber_id,) in subscribers:
                if subscriber_id == video.owner_id:
                    continue
                create_notification(
                    db,
                    subscriber_id,
                    "new_video",
                    f"Новый ролик на канале {channel_name}: {video.title}",
                    payload={"video_id": video.id, "channel_id": video.channel_id},
                )
    else:
        video.moderation_status = "rejected"
        video.approved_at = None
    db.commit()
    db.refresh(video)
    return video_to_out(db, video)


@app.patch("/moderation/reports/{kind}/{report_id}", status_code=status.HTTP_200_OK)
def moderation_action(
    kind: str,
    report_id: int,
    payload: ModerationActionIn,
    _: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    action = payload.action.strip().lower()
    if kind == "video":
        report = db.get(VideoReport, report_id)
        if not report:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found")
        if action == "dismiss":
            report.status = "dismissed"
            db.commit()
            return {"status": "ok"}
        if action == "hide":
            exists = db.query(BlockedVideo).filter(BlockedVideo.video_id == report.video_id).first()
            if not exists:
                db.add(BlockedVideo(video_id=report.video_id, reason=report.reason))
        elif action == "ban":
            exists = db.query(BlockedVideo).filter(BlockedVideo.video_id == report.video_id).first()
            if not exists:
                db.add(BlockedVideo(video_id=report.video_id, reason=f"ban:{report.reason}"))
        report.status = "resolved"
        db.commit()
        return {"status": "ok"}
    if kind == "comment":
        report = db.get(CommentReport, report_id)
        if not report:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found")
        if action == "dismiss":
            report.status = "dismissed"
            db.commit()
            return {"status": "ok"}
        if action in ("hide", "ban"):
            exists = db.query(BlockedComment).filter(BlockedComment.comment_id == report.comment_id).first()
            if not exists:
                db.add(BlockedComment(comment_id=report.comment_id, reason=report.reason))
        report.status = "resolved"
        db.commit()
        return {"status": "ok"}
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown kind")


@app.get("/me/notifications", response_model=list[NotificationOut])
def my_notifications(unread_only: bool = Query(default=False), user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[NotificationOut]:
    query = db.query(Notification).filter(Notification.user_id == user.id)
    if unread_only:
        query = query.filter(Notification.is_read.is_(False))
    rows = query.order_by(desc(Notification.created_at)).limit(100).all()
    return [
        NotificationOut(
            id=row.id,
            type=row.type,
            message=row.message,
            is_read=row.is_read,
            created_at=row.created_at,
            payload=row.payload_json or {},
        )
        for row in rows
    ]


@app.post("/me/notifications/{notification_id}/read", status_code=status.HTTP_204_NO_CONTENT)
def mark_notification_read(notification_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Response:
    row = db.query(Notification).filter(Notification.id == notification_id, Notification.user_id == user.id).first()
    if row:
        row.is_read = True
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/studio/dashboard", response_model=StudioDashboardOut)
def studio_dashboard(days: int = Query(default=14, ge=3, le=90), user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> StudioDashboardOut:
    flush_analytics_buffers(db)
    owned_video_ids = [video_id for (video_id,) in db.query(Video.id).filter(Video.owner_id == user.id).all()]
    if not owned_video_ids:
        return StudioDashboardOut(
            daily_views=[],
            daily_watch_time_minutes=[],
            traffic_sources=[],
            watch_time_curve=[],
            top_videos=[],
        )

    daily_views_rows = (
        db.query(func.date(VideoView.created_at), func.count(VideoView.id))
        .filter(VideoView.video_id.in_(owned_video_ids))
        .group_by(func.date(VideoView.created_at))
        .order_by(desc(func.date(VideoView.created_at)))
        .limit(days)
        .all()
    )
    daily_watch_rows = (
        db.query(func.date(VideoWatchProgress.updated_at), func.sum(VideoWatchProgress.max_seconds_watched))
        .filter(VideoWatchProgress.video_id.in_(owned_video_ids))
        .group_by(func.date(VideoWatchProgress.updated_at))
        .order_by(desc(func.date(VideoWatchProgress.updated_at)))
        .limit(days)
        .all()
    )
    source_rows = (
        db.query(VideoImpression.source, func.count(VideoImpression.id))
        .filter(VideoImpression.video_id.in_(owned_video_ids))
        .group_by(VideoImpression.source)
        .order_by(desc(func.count(VideoImpression.id)))
        .limit(8)
        .all()
    )
    curve_rows = (
        db.query(VideoWatchProgress.max_progress_pct, func.count(VideoWatchProgress.id))
        .filter(VideoWatchProgress.video_id.in_(owned_video_ids))
        .group_by(VideoWatchProgress.max_progress_pct)
        .order_by(VideoWatchProgress.max_progress_pct.asc())
        .all()
    )
    top_rows = (
        db.query(
            Video.id,
            Video.title,
            func.count(VideoView.id).label("views"),
            func.avg(VideoWatchProgress.max_progress_pct).label("avg_watch"),
        )
        .outerjoin(VideoView, VideoView.video_id == Video.id)
        .outerjoin(VideoWatchProgress, VideoWatchProgress.video_id == Video.id)
        .filter(Video.owner_id == user.id)
        .group_by(Video.id, Video.title)
        .order_by(desc("views"))
        .limit(10)
        .all()
    )

    return StudioDashboardOut(
        daily_views=[DailyPointOut(day=str(day), value=float(value or 0)) for day, value in daily_views_rows][::-1],
        daily_watch_time_minutes=[
            DailyPointOut(day=str(day), value=round(float(value or 0) / 60.0, 2)) for day, value in daily_watch_rows
        ][::-1],
        traffic_sources=[TrafficSourceOut(source=str(source), impressions=int(value or 0)) for source, value in source_rows],
        watch_time_curve=[DailyPointOut(day=str(int(progress)), value=float(value or 0)) for progress, value in curve_rows],
        top_videos=[
            TopVideoOut(video_id=video_id, title=title, views=int(views or 0), avg_watch_percent=round(float(avg_watch or 0), 2))
            for video_id, title, views, avg_watch in top_rows
        ],
    )
