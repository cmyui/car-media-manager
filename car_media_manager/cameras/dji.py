from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import MediaFileInfo
from car_media_manager.speed import ProgressCallback

log = logging.getLogger(__name__)

DJI_MOUNT_PATH = Path("/media/pi/Osmo360")
MEDIA_DIR = "DCIM"
MEDIA_EXTENSIONS = frozenset({".osv", ".lrf", ".mp4", ".jpg", ".dng"})

COPY_BUFFER_SIZE = 8 * 1024 * 1024


class DJIOsmoCamera(Camera):
    source_name = "dji"
    display_name = "DJI Osmo 360"

    def __init__(self, mount_path: Path) -> None:
        self.mount_path = mount_path

    def __repr__(self) -> str:
        return f"DJIOsmoCamera({self.mount_path})"

    @classmethod
    async def discover(cls) -> list[Camera]:
        if DJI_MOUNT_PATH.is_dir() and (DJI_MOUNT_PATH / MEDIA_DIR).is_dir():
            log.info("DJI Osmo 360 found at %s", DJI_MOUNT_PATH)
            return [cls(DJI_MOUNT_PATH)]
        return []

    async def stop_recording(self) -> bool:
        raise NotImplementedError("DJI BLE R SDK not yet implemented")

    async def start_recording(self) -> bool:
        raise NotImplementedError("DJI BLE R SDK not yet implemented")

    async def list_media(self) -> list[MediaFileInfo]:
        media_root = self.mount_path / MEDIA_DIR
        if not media_root.is_dir():
            return []

        files: list[MediaFileInfo] = []
        for path in media_root.rglob("*"):
            if not path.is_file():
                continue
            if path.name.startswith("."):
                continue
            if path.suffix.lower() not in MEDIA_EXTENSIONS:
                continue
            try:
                size = path.stat().st_size
            except OSError:
                continue
            files.append(
                MediaFileInfo(
                    name=path.name,
                    size=size,
                    path=str(path.relative_to(self.mount_path)),
                )
            )
        return sorted(files, key=lambda f: f.name)

    async def download_file(
        self,
        file_info: MediaFileInfo,
        dest: Path,
        on_progress: ProgressCallback | None = None,
    ) -> bool:
        src = self.mount_path / file_info.path
        if not src.is_file():
            log.error("Source file missing: %s", src)
            return False

        try:
            if on_progress is None:
                await asyncio.to_thread(shutil.copy2, src, dest)
            else:
                await asyncio.to_thread(
                    _copy_with_progress, src, dest, on_progress,
                )
            return True
        except OSError:
            log.exception("Failed to copy %s", src)
            if dest.exists():
                dest.unlink()
            return False


def _copy_with_progress(
    src: Path,
    dest: Path,
    on_progress: ProgressCallback,
) -> None:
    with open(src, "rb") as src_f, open(dest, "wb") as dest_f:
        while True:
            chunk = src_f.read(COPY_BUFFER_SIZE)
            if not chunk:
                break
            dest_f.write(chunk)
            on_progress(len(chunk))
    shutil.copystat(src, dest)
