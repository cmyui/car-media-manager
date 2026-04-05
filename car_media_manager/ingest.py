import logging
import platform
import shutil
from datetime import datetime
from datetime import timezone
from pathlib import Path

from car_media_manager import db

log = logging.getLogger(__name__)

GOPRO_MEDIA_DIR = "DCIM"
GOPRO_EXTENSIONS = {".mp4", ".jpg", ".thm", ".lrv"}

INSTA360_MEDIA_DIR = "DCIM"
INSTA360_EXTENSIONS = {".mp4", ".insv", ".insp", ".jpg", ".lrv"}


def _get_volumes_root() -> Path:
    system = platform.system()
    if system == "Darwin":
        return Path("/Volumes")
    if system == "Linux":
        return Path("/media") / "pi"
    raise RuntimeError(f"Unsupported platform: {system}")


def find_camera_volume(volume_name: str) -> Path | None:
    volume_path = _get_volumes_root() / volume_name
    if volume_path.is_dir():
        return volume_path
    return None


def _scan_media_files(
    volume_path: Path,
    media_dir: str,
    extensions: set[str],
) -> list[Path]:
    dcim = volume_path / media_dir
    if not dcim.is_dir():
        return []
    files: list[Path] = []
    for path in dcim.rglob("*"):
        if path.is_file() and path.suffix.lower() in extensions:
            files.append(path)
    return sorted(files)


def scan_gopro(volume_path: Path) -> list[Path]:
    return _scan_media_files(volume_path, GOPRO_MEDIA_DIR, GOPRO_EXTENSIONS)


def scan_insta360(volume_path: Path) -> list[Path]:
    return _scan_media_files(volume_path, INSTA360_MEDIA_DIR, INSTA360_EXTENSIONS)


def ingest_file(
    *,
    database: db.Database,
    source: str,
    file_path: Path,
    storage_dir: Path,
) -> db.MediaFile | None:
    file_size = file_path.stat().st_size
    original_filename = file_path.name

    if database.is_ingested(
        source=source,
        original_filename=original_filename,
        file_size=file_size,
    ):
        return None

    dest_dir = storage_dir / source / datetime.now().strftime("%Y-%m-%d")
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
    shutil.copy2(file_path, dest_path)

    created_at = datetime.fromtimestamp(
        file_path.stat().st_mtime,
        tz=timezone.utc,
    )

    return database.insert_media_file(
        source=source,
        original_filename=original_filename,
        local_path=str(dest_path),
        file_size=file_size,
        created_at=created_at,
    )


def run_ingest_cycle(
    *,
    database: db.Database,
    storage_dir: Path,
    gopro_volume_name: str,
    insta360_volume_name: str,
) -> int:
    ingested = 0

    sources: list[tuple[str, str, list[Path]]] = []

    gopro_vol = find_camera_volume(gopro_volume_name)
    if gopro_vol:
        log.info("GoPro detected at %s", gopro_vol)
        sources.append(("gopro", gopro_volume_name, scan_gopro(gopro_vol)))

    insta360_vol = find_camera_volume(insta360_volume_name)
    if insta360_vol:
        log.info("Insta360 detected at %s", insta360_vol)
        sources.append(("insta360", insta360_volume_name, scan_insta360(insta360_vol)))

    for source, _vol_name, files in sources:
        log.info("Found %d files from %s", len(files), source)
        for file_path in files:
            result = ingest_file(
                database=database,
                source=source,
                file_path=file_path,
                storage_dir=storage_dir,
            )
            if result:
                ingested += 1

    if not sources:
        log.debug("No cameras detected")

    return ingested
