import json
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from car_media_manager.web import create_app
from car_media_manager.web import status_sse_event


class EmptyDatabase:
    async def get_stats(self) -> dict[str, int]:
        return {
            "total_files": 0,
            "pending_files": 0,
            "uploaded_files": 0,
            "total_bytes": 0,
            "pending_bytes": 0,
        }

    async def list_recent(self, *, limit: int = 50) -> list[object]:
        return []

    async def list_pending_upload(self) -> list[object]:
        return []

    async def list_active_multipart_progress(self) -> list[dict[str, object]]:
        return []

    async def list_active_copies(self) -> list[object]:
        return []


class EmptyRegistry:
    async def discover_all(self) -> list[object]:
        return []


def test_status_sse_event_formats_named_event() -> None:
    event = status_sse_event({"stats": {"total_files": 1}})

    assert event.startswith("event: status\n")
    assert event.endswith("\n\n")
    payload = event.split("data: ", 1)[1]
    assert json.loads(payload)["stats"]["total_files"] == 1


def test_status_endpoint_returns_dashboard_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("car_media_manager.web.upload.has_internet", _internet)
    app = create_app(
        settings=SimpleNamespace(
            storage_dir=tmp_path,
            s3_bucket_name="bucket",
            s3_access_key_id="key",
            s3_secret_access_key="secret",
            ingest_free_space_reserve_bytes=1,
        ),
        database=EmptyDatabase(),
        s3_client=object(),
        registry=EmptyRegistry(),
    )

    response = TestClient(app).get("/api/status")

    assert response.status_code == 200
    body = response.json()
    assert body["stats"]["total_files"] == 0
    assert body["pending_upload"]["files"] == 0
    assert body["active_uploads"] == []


async def _internet() -> bool:
    return True
