import os
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import main

_tmp_media = tempfile.TemporaryDirectory()
_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)


def setup_module() -> None:
    media_root = Path(_tmp_media.name)
    (media_root / "videos").mkdir(parents=True, exist_ok=True)
    (media_root / "thumbs").mkdir(parents=True, exist_ok=True)
    main.MEDIA_DIR = media_root
    test_db_url = f"sqlite:///{Path(_tmp_db.name).as_posix()}"
    main.DATABASE_URL = test_db_url
    main.engine = create_engine(test_db_url, future=True, connect_args={"check_same_thread": False})
    main.SessionLocal = sessionmaker(bind=main.engine, autoflush=False, autocommit=False, expire_on_commit=False)
    main.Base.metadata.drop_all(bind=main.engine)
    main.Base.metadata.create_all(bind=main.engine)


def teardown_module() -> None:
    _tmp_media.cleanup()
    _tmp_db.close()
    try:
        os.unlink(_tmp_db.name)
    except OSError:
        pass


def auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_auth_and_permissions_and_social_flow() -> None:
    client = TestClient(main.app)

    admin_register = client.post(
        "/auth/register",
        json={"username": "admin", "email": "admin@example.com", "password": "secret123"},
    )
    assert admin_register.status_code == 201
    assert admin_register.json()["is_admin"] is True

    user_register = client.post(
        "/auth/register",
        json={"username": "user", "email": "user@example.com", "password": "secret123"},
    )
    assert user_register.status_code == 201
    assert user_register.json()["can_upload"] is False

    admin_login = client.post("/auth/login", json={"email": "admin@example.com", "password": "secret123"})
    user_login = client.post("/auth/login", json={"email": "user@example.com", "password": "secret123"})
    admin_token = admin_login.json()["access_token"]
    user_token = user_login.json()["access_token"]

    upload_ok = client.post(
        "/videos/upload",
        headers=auth_header(user_token),
        data={"title": "Denied upload", "description": "test", "genre": "hoi4_letsplay"},
        files={"video_file": ("x.mp4", b"fake-data", "video/mp4")},
    )
    assert upload_ok.status_code == 201
    assert upload_ok.json()["moderation_status"] == "pending"

    access_granted = client.patch(
        f"/admin/users/{user_register.json()['id']}/upload-access",
        headers=auth_header(admin_token),
        json={"can_upload": True},
    )
    assert access_granted.status_code == 200
    assert access_granted.json()["can_upload"] is True

    upload_ok = client.post(
        "/videos/upload",
        headers=auth_header(user_token),
        data={"title": "HOI4 test", "description": "desc", "genre": "hoi4_letsplay"},
        files={"video_file": ("real.mp4", b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 100, "video/mp4")},
    )
    assert upload_ok.status_code == 201
    assert upload_ok.json()["moderation_status"] == "pending"
    video_id = upload_ok.json()["id"]
    channel_id = upload_ok.json()["channel_id"]

    approve = client.patch(f"/moderation/videos/{video_id}", headers=auth_header(admin_token), json={"action": "approve"})
    assert approve.status_code == 200
    assert approve.json()["moderation_status"] == "approved"

    like = client.post(f"/videos/{video_id}/like", headers=auth_header(admin_token))
    assert like.status_code == 204

    comment = client.post(
        f"/videos/{video_id}/comments",
        headers=auth_header(admin_token),
        json={"text": "Нормальный летсплей"},
    )
    assert comment.status_code == 201

    subscribe = client.post(f"/channels/{channel_id}/subscribe", headers=auth_header(admin_token))
    assert subscribe.status_code == 204

    comments = client.get(f"/videos/{video_id}/comments")
    assert comments.status_code == 200
    assert len(comments.json()) == 1


def test_range_streaming_endpoint() -> None:
    client = TestClient(main.app)
    login = client.post("/auth/login", json={"email": "user@example.com", "password": "secret123"})
    token = login.json()["access_token"]

    upload = client.post(
        "/videos/upload",
        headers=auth_header(token),
        data={"title": "Stream me", "description": "stream", "genre": "hoi4_letsplay"},
        files={"video_file": ("stream.mp4", b"A" * 2048, "video/mp4")},
    )
    assert upload.status_code == 201
    video_id = upload.json()["id"]

    full = client.get(f"/videos/{video_id}/stream", headers=auth_header(token))
    assert full.status_code in (200, 206)
    assert full.headers.get("accept-ranges") == "bytes"

    partial = client.get(f"/videos/{video_id}/stream", headers={**auth_header(token), "Range": "bytes=0-99"})
    assert partial.status_code == 206
    assert partial.headers["content-range"].startswith("bytes 0-99/")
    assert len(partial.content) == 100

    qualities = client.get(f"/videos/{video_id}/qualities", headers=auth_header(token))
    assert qualities.status_code == 200
    assert "source" in qualities.json()["items"]

    # Simulate ready transcoded quality to test quality switch.
    db = main.SessionLocal()
    video_row = db.get(main.Video, video_id)
    abs_original = main.MEDIA_DIR / video_row.file_path
    fake_360 = abs_original.with_name(f"{abs_original.stem}_360p{abs_original.suffix}")
    fake_360.write_bytes(b"B" * 321)
    db.close()

    q_stream = client.get(f"/videos/{video_id}/stream?quality=360p", headers=auth_header(token))
    assert q_stream.status_code in (200, 206)
    assert len(q_stream.content) == 321


def test_unique_views_and_studio_analytics() -> None:
    client = TestClient(main.app)
    admin_login = client.post("/auth/login", json={"email": "admin@example.com", "password": "secret123"})
    admin_token = admin_login.json()["access_token"]
    user_login = client.post("/auth/login", json={"email": "user@example.com", "password": "secret123"})
    user_token = user_login.json()["access_token"]

    upload = client.post(
        "/videos/upload",
        headers=auth_header(user_token),
        data={"title": "Analytics test", "description": "retention", "genre": "hoi4_letsplay"},
        files={"video_file": ("analytics.mp4", b"A" * 2048, "video/mp4")},
    )
    assert upload.status_code == 201
    video_id = upload.json()["id"]
    approve = client.patch(f"/moderation/videos/{video_id}", headers=auth_header(admin_token), json={"action": "approve"})
    assert approve.status_code == 200

    first_view = client.get(f"/videos/{video_id}", headers=auth_header(admin_token))
    second_view = client.get(f"/videos/{video_id}", headers=auth_header(admin_token))
    assert first_view.status_code == 200
    assert second_view.status_code == 200
    assert second_view.json()["views_count"] == 1

    anon_view = client.get(f"/videos/{video_id}")
    assert anon_view.status_code == 200
    assert anon_view.json()["views_count"] == 2

    track_impression = client.post(
        "/analytics/impressions",
        headers=auth_header(admin_token),
        json={"video_id": video_id, "source": "feed"},
    )
    assert track_impression.status_code == 204

    track_watch = client.post(
        "/analytics/watch-progress",
        headers=auth_header(admin_token),
        json={"video_id": video_id, "seconds_watched": 60, "duration_seconds": 120, "progress_pct": 50},
    )
    assert track_watch.status_code == 204

    analytics = client.get(f"/studio/videos/{video_id}/analytics", headers=auth_header(user_token))
    assert analytics.status_code == 200
    body = analytics.json()
    assert body["impressions"] == 1
    assert body["unique_views"] >= 2
    assert body["ctr_percent"] >= 100.0


def test_builtin_admin_login_admin_admin() -> None:
    client = TestClient(main.app)
    response = client.post("/auth/login", json={"email": "admin", "password": "admin"})
    assert response.status_code == 200
    token = response.json()["access_token"]

    me = client.get("/auth/me", headers=auth_header(token))
    assert me.status_code == 200
    assert me.json()["is_admin"] is True


def test_video_delete_by_owner() -> None:
    client = TestClient(main.app)
    user_login = client.post("/auth/login", json={"email": "user@example.com", "password": "secret123"})
    user_token = user_login.json()["access_token"]

    upload = client.post(
        "/videos/upload",
        headers=auth_header(user_token),
        data={"title": "Delete me", "description": "to remove", "genre": "hoi4_letsplay"},
        files={"video_file": ("delete.mp4", b"A" * 1000, "video/mp4")},
    )
    assert upload.status_code == 201
    video_id = upload.json()["id"]

    delete = client.delete(f"/videos/{video_id}", headers=auth_header(user_token))
    assert delete.status_code == 204

    video = client.get(f"/videos/{video_id}")
    assert video.status_code == 404


def test_ranked_feed_and_search_query() -> None:
    client = TestClient(main.app)
    token = client.post("/auth/login", json={"email": "admin", "password": "admin"}).json()["access_token"]

    # Create searchable video.
    uploaded = client.post(
        "/videos/upload",
        headers=auth_header(token),
        data={"title": "HOI4 Germany Guide", "description": "rank me", "genre": "hoi4_letsplay"},
        files={"video_file": ("guide.mp4", b"A" * 2048, "video/mp4")},
    )
    assert uploaded.status_code == 201
    assert uploaded.json()["moderation_status"] == "approved"

    feed = client.get("/videos", headers=auth_header(token))
    assert feed.status_code == 200
    assert isinstance(feed.json()["items"], list)

    search = client.get("/videos?q=Germany", headers=auth_header(token))
    assert search.status_code == 200
    assert any("Germany" in item["title"] for item in search.json()["items"])

    suggestions = client.get("/search/suggestions?q=Ger")
    assert suggestions.status_code == 200
    assert any("Germany" in item for item in suggestions.json()["items"])


def test_watch_later_and_continue_watching() -> None:
    client = TestClient(main.app)
    user_token = client.post("/auth/login", json={"email": "user@example.com", "password": "secret123"}).json()["access_token"]
    admin_token = client.post("/auth/login", json={"email": "admin", "password": "admin"}).json()["access_token"]

    upload = client.post(
        "/videos/upload",
        headers=auth_header(admin_token),
        data={"title": "Continue me", "description": "continue block", "genre": "hoi4_letsplay"},
        files={"video_file": ("continue.mp4", b"A" * 4096, "video/mp4")},
    )
    assert upload.status_code == 201
    video_id = upload.json()["id"]

    add_later = client.post(f"/videos/{video_id}/watch-later", headers=auth_header(user_token))
    assert add_later.status_code == 204

    watch_later = client.get("/me/watch-later", headers=auth_header(user_token))
    assert watch_later.status_code == 200
    assert any(item["id"] == video_id for item in watch_later.json())

    progress = client.post(
        "/analytics/watch-progress",
        headers=auth_header(user_token),
        json={"video_id": video_id, "seconds_watched": 42, "duration_seconds": 120, "progress_pct": 35},
    )
    assert progress.status_code == 204

    continue_rows = client.get("/me/continue-watching", headers=auth_header(user_token))
    assert continue_rows.status_code == 200
    assert any(item["video"]["id"] == video_id for item in continue_rows.json())


def test_pending_video_hidden_and_size_limit() -> None:
    client = TestClient(main.app)
    user_token = client.post("/auth/login", json={"email": "user@example.com", "password": "secret123"}).json()["access_token"]
    admin_token = client.post("/auth/login", json={"email": "admin", "password": "admin"}).json()["access_token"]

    upload = client.post(
        "/videos/upload",
        headers=auth_header(user_token),
        data={"title": "Pending only", "description": "pending", "genre": "hoi4_letsplay"},
        files={"video_file": ("pending.mp4", b"A" * 1024, "video/mp4")},
    )
    assert upload.status_code == 201
    video_id = upload.json()["id"]
    assert upload.json()["moderation_status"] == "pending"

    anon_open = client.get(f"/videos/{video_id}")
    assert anon_open.status_code == 404
    owner_open = client.get(f"/videos/{video_id}", headers=auth_header(user_token))
    assert owner_open.status_code == 200

    approve = client.patch(f"/moderation/videos/{video_id}", headers=auth_header(admin_token), json={"action": "approve"})
    assert approve.status_code == 200
    anon_after = client.get(f"/videos/{video_id}")
    assert anon_after.status_code == 200

    original_limit = main.MAX_VIDEO_BYTES
    main.MAX_VIDEO_BYTES = 10
    try:
        too_big = client.post(
            "/videos/upload",
            headers=auth_header(user_token),
            data={"title": "Too big", "description": "x", "genre": "hoi4_letsplay"},
            files={"video_file": ("big.mp4", b"A" * 128, "video/mp4")},
        )
        assert too_big.status_code == 413
    finally:
        main.MAX_VIDEO_BYTES = original_limit


def test_notifications_moderation_and_dashboard() -> None:
    client = TestClient(main.app)
    admin_token = client.post("/auth/login", json={"email": "admin", "password": "admin"}).json()["access_token"]
    user_token = client.post("/auth/login", json={"email": "user@example.com", "password": "secret123"}).json()["access_token"]

    first_upload = client.post(
        "/videos/upload",
        headers=auth_header(admin_token),
        data={"title": "Notify base", "description": "x", "genre": "hoi4_letsplay"},
        files={"video_file": ("notify0.mp4", b"A" * 2048, "video/mp4")},
    )
    assert first_upload.status_code == 201
    channel_id = first_upload.json()["channel_id"]
    client.post(f"/channels/{channel_id}/subscribe", headers=auth_header(user_token))

    upload = client.post(
        "/videos/upload",
        headers=auth_header(admin_token),
        data={"title": "Notify and report", "description": "x", "genre": "hoi4_letsplay"},
        files={"video_file": ("notify1.mp4", b"A" * 2048, "video/mp4")},
    )
    assert upload.status_code == 201
    video_id = upload.json()["id"]

    notifications = client.get("/me/notifications", headers=auth_header(user_token))
    assert notifications.status_code == 200

    report = client.post(f"/reports/video/{video_id}", headers=auth_header(user_token), json={"reason": "spam"})
    assert report.status_code == 201

    queue = client.get("/moderation/reports", headers=auth_header(admin_token))
    assert queue.status_code == 200
    if queue.json():
        first = queue.json()[0]
        action = client.patch(
            f"/moderation/reports/{first['kind']}/{first['report_id']}",
            headers=auth_header(admin_token),
            json={"action": "dismiss"},
        )
        assert action.status_code == 200

    dashboard = client.get("/studio/dashboard", headers=auth_header(admin_token))
    assert dashboard.status_code == 200
