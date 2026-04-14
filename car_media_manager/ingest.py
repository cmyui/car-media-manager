import asyncio
import logging
import platform
import shutil
from datetime import datetime
from datetime import timezone
from pathlib import Path

from car_media_manager import cameras
from car_media_manager import db

log = logging.getLogger(__name__)

_ingest_lock = asyncio.Lock()


def _default_volumes_root() -> Path:
    system = platform.system()
    if system == "Darwin":
        return Path("/Volumes")
    if system == "Linux":
        gvfs = Path("/run/user/1000/gvfs")
        if gvfs.is_dir():
            return gvfs
        return Path("/media") / "pi"
    raise RuntimeError(f"Unsupported platform: {system}")


def list_mounted_volumes(volumes_root: Path | None = None) -> list[Path]:
    root = volumes_root if volumes_root is not None else _default_volumes_root()
    if not root.is_dir():
        return []
    return [p for p in root.iterdir() if p.is_dir()]


def find_camera_mounts(volumes_root: Path | None = None) -> list[tuple[Path, cameras.Camera]]:
    mounts: list[tuple[Path, cameras.Camera]] = []
    for volume in list_mounted_volumes(volumes_root):
        camera = cameras.identify_camera(volume)
        if camera is None:
            continue
        mounts.append((volume, camera))
    return mounts


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

    log.info("Ingesting %s -> %s (%d bytes)", file_path, dest_path, file_size)
    await asyncio.to_thread(shutil.copy2, file_path, dest_path)

    created_at = datetime.fromtimestamp(
        file_path.stat().st_mtime,
        tz=timezone.utc,
    )

    return await database.insert_media_file(
        source=camera.source_name,
        original_filename=original_filename,
        local_path=str(dest_path),
        file_size=file_size,
        created_at=created_at,
    )


async def run_ingest_cycle(
    *,
    database: db.Database,
    storage_dir: Path,
    volumes_root: Path | None = None,
) -> int:
    if _ingest_lock.locked():
        log.debug("Ingest cycle already in progress, skipping")
        return 0

    async with _ingest_lock:
        ingested = 0

        all_mounts = find_camera_mounts(volumes_root)

        if not all_mounts:
            log.debug("No cameras detected")
            return 0

        for mount_path, camera in all_mounts:
            if camera is cameras.GENERIC:
                log.warning(
                    "Unknown camera at %s, falling back to generic scan",
                    mount_path,
                )
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
