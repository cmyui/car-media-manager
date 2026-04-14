import asyncio
import logging
import shutil
from datetime import datetime
from datetime import timezone
from pathlib import Path

from car_media_manager import cameras
from car_media_manager import db
from car_media_manager import gio

log = logging.getLogger(__name__)

_ingest_lock = asyncio.Lock()


def discover_cameras() -> list[tuple[Path, cameras.Camera]]:
    results: list[tuple[Path, cameras.Camera]] = []
    for mount_path in gio.discover_mtp_mounts():
        camera = cameras.identify_camera(mount_path)
        if camera is None:
            continue
        results.append((mount_path, camera))
    return results


async def ingest_file(
    *,
    database: db.Database,
    camera: cameras.Camera,
    file_path: Path,
    storage_dir: Path,
) -> db.MediaFile | None:
    file_size = file_path.stat().st_size
    original_filename = file_path.name

    if await database.is_ingested(
        source=camera.source_name,
        original_filename=original_filename,
        file_size=file_size,
    ):
        return None

    dest_dir = storage_dir / camera.source_name / datetime.now().strftime("%Y-%m-%d")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / original_filename

    if dest_path.exists():
        stem = file_path.stem
        suffix = file_path.suffix
        counter = 1
        while dest_path.exists():
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    log.info("Ingesting %s -> %s (%d bytes)", original_filename, dest_path, file_size)

    record = await database.insert_media_file(
        source=camera.source_name,
        original_filename=original_filename,
        local_path=str(dest_path),
        file_size=file_size,
        created_at=datetime.now(tz=timezone.utc),
    )

    await asyncio.to_thread(shutil.copy2, file_path, dest_path)
    await database.mark_ingested(record.id)

    return record


async def _try_ingest(
    *,
    database: db.Database,
    storage_dir: Path,
) -> int:
    discovered = discover_cameras()

    if not discovered:
        log.debug("No cameras detected")
        return 0

    ingested = 0
    for mount_path, camera in discovered:
        if camera is cameras.GENERIC:
            log.warning("Unknown camera at %s", mount_path)
        else:
            log.info("%s detected at %s", camera.display_name, mount_path)

        files = camera.scan(mount_path)
        log.info("Found %d files from %s", len(files), camera.source_name)

        for file_path in files:
            result = await ingest_file(
                database=database,
                camera=camera,
                file_path=file_path,
                storage_dir=storage_dir,
            )
            if result:
                ingested += 1

    return ingested


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
        try:
            return await _try_ingest(database=database, storage_dir=storage_dir)
        except OSError:
            log.warning("MTP error during ingest, attempting session reset")
            for mount_path in gio.discover_mtp_mounts():
                await gio.reset_mtp_session(mount_path)
            try:
                return await _try_ingest(database=database, storage_dir=storage_dir)
            except OSError:
                log.exception("Ingest failed after MTP reset")
                return 0
