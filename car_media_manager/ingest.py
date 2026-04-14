import asyncio
import logging
from datetime import datetime
from datetime import timezone
from pathlib import Path

from car_media_manager import db
from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import MediaFileInfo
from car_media_manager.cameras.base import discover_cameras

log = logging.getLogger(__name__)

_ingest_lock = asyncio.Lock()


async def ingest_file(
    *,
    database: db.Database,
    camera: Camera,
    file_info: MediaFileInfo,
    storage_dir: Path,
) -> db.MediaFile | None:
    if await database.is_ingested(
        source=camera.source_name,
        original_filename=file_info.name,
        file_size=file_info.size,
    ):
        return None

    dest_dir = storage_dir / camera.source_name / datetime.now().strftime("%Y-%m-%d")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / file_info.name

    if dest_path.exists():
        stem = Path(file_info.name).stem
        suffix = Path(file_info.name).suffix
        counter = 1
        while dest_path.exists():
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    record = await database.insert_media_file(
        source=camera.source_name,
        original_filename=file_info.name,
        local_path=str(dest_path),
        file_size=file_info.size,
        created_at=datetime.now(tz=timezone.utc),
    )

    log.info("Ingesting %s -> %s (%d bytes)", file_info.name, dest_path, file_info.size)
    ok = await camera.download_file(file_info, dest_path)
    if not ok:
        log.error("Failed to download %s", file_info.name)
        if dest_path.exists():
            dest_path.unlink()
        await database.delete_media_file(record.id)
        return None

    await database.mark_ingested(record.id)
    return record


async def _cleanup_partial_copies(database: db.Database) -> None:
    stale = await database.list_incomplete_copies()
    for mf in stale:
        partial = Path(mf.local_path)
        if partial.exists():
            partial.unlink()
            log.info("Cleaned up partial copy: %s", partial)
        await database.delete_media_file(mf.id)


async def run_ingest_cycle(
    *,
    database: db.Database,
    storage_dir: Path,
    **_kwargs: object,
) -> int:
    if _ingest_lock.locked():
        log.debug("Ingest cycle already in progress, skipping")
        return 0

    async with _ingest_lock:
        await _cleanup_partial_copies(database)
        cameras = await discover_cameras()

        if not cameras:
            log.debug("No cameras detected")
            return 0

        ingested = 0
        for camera in cameras:
            log.info("%s detected at %s", camera.display_name, camera)

            files = await camera.list_media()
            log.info("Found %d files from %s", len(files), camera.source_name)

            for file_info in files:
                result = await ingest_file(
                    database=database,
                    camera=camera,
                    file_info=file_info,
                    storage_dir=storage_dir,
                )
                if result:
                    ingested += 1

        return ingested
