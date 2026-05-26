from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from car_media_manager import db
from car_media_manager import upload
from car_media_manager.cameras.base import CameraVendor
from car_media_manager.cameras.base import MediaFileInfo
from car_media_manager.status import build_status


class FakeCamera:
    vendor = CameraVendor.DJI
    display_name = "DJI Osmo 360"
    capabilities = ["USB"]
    supports_pairing = False
    supports_remote_control = False
    is_paired = False

    @property
    def camera_id(self) -> str:
        return "usb"

    async def list_media(self) -> list[MediaFileInfo]:
        return [
            MediaFileInfo(name="already.mov", size=100, path="/already.mov"),
            MediaFileInfo(name="new.mov", size=200, path="/new.mov"),
        ]


class FakeRegistry:
    async def discover_all(self) -> list[FakeCamera]:
        return [FakeCamera()]


class FakeDatabase:
    def __init__(self, active_copy_path: Path) -> None:
        now = datetime(2026, 5, 26, tzinfo=timezone.utc)
        self.ingested = db.MediaFile(
            id=1,
            vendor="dji",
            original_filename="already.mov",
            local_path="/tmp/already.mov",
            file_size=100,
            created_at=now,
            ingested_at=now,
            uploaded_at=None,
        )
        self.active_copy = db.MediaFile(
            id=2,
            vendor="dji",
            original_filename="copying.mov",
            local_path=str(active_copy_path),
            file_size=400,
            created_at=now,
            ingested_at=None,
            uploaded_at=None,
        )
        self.pending_upload = db.MediaFile(
            id=3,
            vendor="dji",
            original_filename="pending.mov",
            local_path="/tmp/pending.mov",
            file_size=800,
            created_at=now,
            ingested_at=now,
            uploaded_at=None,
        )

    async def get_stats(self) -> dict[str, int]:
        return {
            "total_files": 3,
            "pending_files": 3,
            "uploaded_files": 0,
            "total_bytes": 1300,
            "pending_bytes": 1300,
        }

    async def list_recent(self, *, limit: int = 50) -> list[db.MediaFile]:
        if limit > 50:
            return [self.ingested]
        return [self.pending_upload, self.active_copy, self.ingested]

    async def list_pending_upload(self) -> list[db.MediaFile]:
        return [self.ingested, self.pending_upload]

    async def list_active_multipart_progress(self) -> list[dict[str, object]]:
        return [
            {
                "media_file_id": 3,
                "multipart_upload_id": 1,
                "vendor": "dji",
                "original_filename": "pending.mov",
                "file_size": 800,
                "part_size": 400,
                "started_at": "2026-05-26T00:00:00+00:00",
                "completed_parts": 1,
                "last_part_at": "2026-05-26T00:01:00+00:00",
                "bytes_uploaded": 200,
                "percent": 25.0,
            }
        ]

    async def list_active_copies(self) -> list[db.MediaFile]:
        return [self.active_copy]


@pytest.mark.asyncio
async def test_build_status_shapes_dashboard_data(tmp_path: Path) -> None:
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    active_copy = storage_dir / "copying.mov"
    active_copy.write_bytes(b"x" * 100)

    status = await build_status(
        settings=SimpleNamespace(storage_dir=storage_dir),
        database=FakeDatabase(active_copy),
        registry=FakeRegistry(),
        has_internet=lambda: _true(),
    )

    assert status["has_internet"] is True
    assert status["cameras"][0]["display_name"] == "DJI Osmo 360"
    assert status["cameras"][0]["remaining_files"] == 1
    assert status["cameras"][0]["remaining_bytes"] == 200
    assert status["pending_upload"]["files"] == 2
    assert status["pending_upload"]["bytes"] == 900
    assert status["camera_remaining"]["files"] == 1
    assert status["camera_remaining"]["bytes"] == 200
    assert status["pipeline"]["on_camera"]["bytes"] == 200
    assert status["pipeline"]["copying"]["bytes"] == 400
    assert status["pipeline"]["queued"]["bytes"] == 900
    assert status["active_copies"][0]["bytes_copied"] == 100
    assert status["active_uploads"][0]["bytes_uploaded"] == 200
    assert status["recent_files"][0]["state"] == "uploading"


async def _true() -> bool:
    return True


class SlowUploadDatabase(FakeDatabase):
    async def list_active_copies(self) -> list[db.MediaFile]:
        return []

    async def list_active_multipart_progress(self) -> list[dict[str, object]]:
        return [
            {
                "media_file_id": 3,
                "multipart_upload_id": 1,
                "vendor": "dji",
                "original_filename": "pending.mov",
                "file_size": 800,
                "part_size": 400,
                "started_at": "2026-05-26T00:00:00+00:00",
                "completed_parts": 0,
                "last_part_at": None,
                "bytes_uploaded": 0,
                "percent": 0.0,
            }
        ]


@pytest.mark.asyncio
async def test_active_upload_without_speed_estimates_instead_of_using_ingest_eta(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    active_copy = storage_dir / "copying.mov"
    active_copy.write_bytes(b"")
    monkeypatch.setattr(
        upload,
        "get_active_upload",
        lambda: {
            "media_file_id": 3,
            "vendor": "dji",
            "original_filename": "pending.mov",
            "file_size": 800,
            "part_number": 1,
            "total_parts": 2,
            "current_part_size": 400,
            "confirmed_bytes": 0,
            "part_elapsed_seconds": 30.0,
        },
    )

    status = await build_status(
        settings=SimpleNamespace(storage_dir=storage_dir),
        database=SlowUploadDatabase(active_copy),
        registry=FakeRegistry(),
        has_internet=lambda: _true(),
    )

    assert status["active_uploads"][0]["part_status"] == "sending part 1/2"
    assert status["speeds"]["upload_display"] == "estimating"
    assert status["etas"]["upload_display"] == "estimating"
    assert status["etas"]["overall_display"] == "estimating"


def test_multipart_chunk_size_uses_s3_minimum() -> None:
    assert upload.MULTIPART_CHUNK_SIZE == 5 * 1024 * 1024
    assert upload.MULTIPART_THRESHOLD == 5 * 1024 * 1024
