import asyncio
import logging
from datetime import datetime
from datetime import timezone
from pathlib import Path

from car_media_manager import cameras
from car_media_manager import db
from car_media_manager import gio

log = logging.getLogger(__name__)

_ingest_lock = asyncio.Lock()


async def _discover_cameras() -> list[tuple[str, cameras.Camera]]:
    mounts = await gio.discover_mtp_mounts()
    results: list[tuple[str, cameras.Camera]] = []
    for mount in mounts:
        camera = cameras.identify_camera_from_uri(mount.uri)
        media_root = await gio.find_media_root(mount.uri, camera.media_dir)
        if media_root is None:
            continue
        results.append((media_root, camera))
    return results


async def ingest_file(
    *,
    database: db.Database,
    camera: cameras.Camera,
    file_info: gio.GioFileInfo,
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

    log.info("Ingesting %s -> %s (%d bytes)", file_info.name, dest_path, file_info.size)
    if not await gio.copy_file(file_info.uri, dest_path):
        log.error("Failed to copy %s", file_info.name)
        return None

    return await database.insert_media_file(
        source=camera.source_name,
        original_filename=file_info.name,
        local_path=str(dest_path),
        file_size=file_info.size,
        created_at=datetime.now(tz=timezone.utc),
    )


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
        ingested = 0

        discovered = await _discover_cameras()

        if not discovered:
            log.debug("No cameras detected")
            return 0

        for media_root_uri, camera in discovered:
            if camera is cameras.GENERIC:
                log.warning("Unknown camera at %s", media_root_uri)
            else:
                log.info("%s detected at %s", camera.display_name, media_root_uri)

            files = await gio.scan_media_files(media_root_uri, camera.media_extensions)
            log.info("Found %d files from %s", len(files), camera.source_name)

            for f in files:
                result = await ingest_file(
                    database=database,
                    camera=camera,
                    file_info=f,
                    storage_dir=storage_dir,
                )
                if result:
                    ingested += 1

        return ingested
