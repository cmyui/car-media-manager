from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from car_media_manager import db
from car_media_manager import ingest
from car_media_manager import runtime
from car_media_manager.cameras.base import CameraVendor
from car_media_manager.cameras.base import MediaFileInfo


class FakeCamera:
    vendor = CameraVendor.DJI
    display_name = "DJI Osmo 360"

    async def list_media(self) -> list[MediaFileInfo]:
        return [MediaFileInfo(name="clip.osv", size=1000, path="/clip.osv")]

    async def download_file(
        self,
        file_info: MediaFileInfo,
        dest: Path,
        on_progress=None,
    ) -> bool:
        dest.write_bytes(b"x" * file_info.size)
        if on_progress is not None:
            on_progress(file_info.size)
        return True


class FakeRegistry:
    async def discover_all(self) -> list[FakeCamera]:
        return [FakeCamera()]


class FakeDatabase:
    def __init__(self) -> None:
        self.inserted = 0
        self.marked_ingested = 0
        self.deleted = 0

    async def is_ingested(
        self,
        *,
        vendor: str,
        original_filename: str,
        file_size: int,
    ) -> bool:
        return False

    async def insert_media_file(
        self,
        *,
        vendor: str,
        original_filename: str,
        local_path: str,
        file_size: int,
        created_at: datetime,
    ) -> db.MediaFile:
        self.inserted += 1
        return db.MediaFile(
            id=1,
            vendor=vendor,
            original_filename=original_filename,
            local_path=local_path,
            file_size=file_size,
            created_at=created_at,
            ingested_at=None,
            uploaded_at=None,
        )

    async def mark_ingested(self, file_id: int) -> None:
        self.marked_ingested += 1

    async def list_incomplete_copies(self) -> list[db.MediaFile]:
        return []

    async def delete_media_file(self, file_id: int) -> None:
        self.deleted += 1


@pytest.mark.asyncio
async def test_ingest_blocks_before_insert_when_storage_is_low(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = FakeDatabase()
    monkeypatch.setattr(
        ingest.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=500),
    )

    result = await ingest.run_ingest_cycle(
        database=database,
        storage_dir=tmp_path,
        registry=FakeRegistry(),
        free_space_reserve_bytes=100,
    )

    assert result == 0
    assert database.inserted == 0
    assert database.marked_ingested == 0
    assert runtime.get_message()["code"] == "low_storage"
    runtime.clear_message()


@pytest.mark.asyncio
async def test_ingest_copies_when_storage_has_file_size_plus_reserve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = FakeDatabase()
    monkeypatch.setattr(
        ingest.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=2000),
    )

    result = await ingest.run_ingest_cycle(
        database=database,
        storage_dir=tmp_path,
        registry=FakeRegistry(),
        free_space_reserve_bytes=100,
    )

    assert result == 1
    assert database.inserted == 1
    assert database.marked_ingested == 1
    assert database.deleted == 0
    assert runtime.get_message() is None
