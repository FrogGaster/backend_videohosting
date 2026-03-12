from __future__ import annotations

from celery_app import celery
from main import process_video_pipeline


@celery.task(name="video.process")
def process_video_task(video_id: int, local_video_path: str, original_rel_path: str) -> None:
    process_video_pipeline(video_id=video_id, local_video_path=local_video_path, original_rel_path=original_rel_path)
