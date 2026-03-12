from __future__ import annotations

import os

from celery import Celery

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
REDIS_SOCKET_TIMEOUT_SEC = int(os.getenv("REDIS_SOCKET_TIMEOUT_SEC", "5"))
REDIS_SOCKET_CONNECT_TIMEOUT_SEC = int(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT_SEC", "5"))
REDIS_RETRY_ON_TIMEOUT = os.getenv("REDIS_RETRY_ON_TIMEOUT", "1") == "1"

celery = Celery("moorluck_hosting", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
celery.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "socket_timeout": REDIS_SOCKET_TIMEOUT_SEC,
        "socket_connect_timeout": REDIS_SOCKET_CONNECT_TIMEOUT_SEC,
        "retry_on_timeout": REDIS_RETRY_ON_TIMEOUT,
    },
    redis_backend_health_check_interval=30,
)
