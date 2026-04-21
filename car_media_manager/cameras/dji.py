from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import MediaFileInfo
from car_media_manager.cameras.dji_ble import pairing_store
from car_media_manager.cameras.dji_ble.client import DJIBLESession
from car_media_manager.cameras.dji_ble.client import PairingInfo
from car_media_manager.cameras.dji_ble.client import scan_for_cameras
from car_media_manager.speed import ProgressCallback

log = logging.getLogger(__name__)

DJI_MOUNT_PATH = Path("/media/pi/Osmo360")
MEDIA_DIR = "DCIM"
MEDIA_EXTENSIONS = frozenset({".osv", ".lrf", ".mp4", ".jpg", ".dng"})

COPY_BUFFER_SIZE = 8 * 1024 * 1024


class DJIOsmoCamera(Camera):
    source_name = "dji"
    display_name = "DJI Osmo 360"

    # Set at app startup before registry.discover_all() is called
    storage_dir: Path | None = None

    def __init__(
        self,
        *,
        mount_path: Path | None = None,
        pairing: PairingInfo | None = None,
    ) -> None:
        self.mount_path = mount_path
        self.pairing = pairing

    def __repr__(self) -> str:
        parts: list[str] = []
        if self.mount_path:
            parts.append(f"usb={self.mount_path}")
        if self.pairing:
            parts.append(f"ble={self.pairing.address}")
        return f"DJIOsmoCamera({', '.join(parts) or 'not connected'})"

    @classmethod
    async def discover(cls) -> list[Camera]:
        mount = (
            DJI_MOUNT_PATH
            if (DJI_MOUNT_PATH.is_dir() and (DJI_MOUNT_PATH / MEDIA_DIR).is_dir())
            else None
        )
        pairing = pairing_store.load(cls.storage_dir) if cls.storage_dir else None

        if mount is None and pairing is None:
            return []

        log.info(
            "DJI Osmo 360 detected: usb=%s ble=%s", mount, pairing and pairing.address
        )
        return [cls(mount_path=mount, pairing=pairing)]

    async def stop_recording(self) -> bool:
        return await self._record_control(start=False)

    async def start_recording(self) -> bool:
        return await self._record_control(start=True)

    async def _record_control(self, *, start: bool) -> bool:
        if self.pairing is None:
            log.error("DJI camera not paired; cannot control recording")
            return False
        try:
            async with DJIBLESession(self.pairing.address) as session:
                await session.reconnect()
                if start:
                    await session.start_recording(device_id=self.pairing.device_id)
                else:
                    await session.stop_recording(device_id=self.pairing.device_id)
            return True
        except Exception:
            log.exception("BLE record control failed (start=%s)", start)
            return False

    async def list_media(self) -> list[MediaFileInfo]:
        if self.mount_path is None:
            return []
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
        if self.mount_path is None:
            log.error("DJI camera not USB-connected; cannot download files")
            return False
        src = self.mount_path / file_info.path
        if not src.is_file():
            log.error("Source file missing: %s", src)
            return False

        try:
            if on_progress is None:
                await asyncio.to_thread(shutil.copy2, src, dest)
            else:
                await asyncio.to_thread(
                    _copy_with_progress,
                    src,
                    dest,
                    on_progress,
                )
            return True
        except OSError:
            log.exception("Failed to copy %s", src)
            if dest.exists():
                dest.unlink()
            return False


async def pair_new_camera(storage_dir: Path) -> PairingInfo:
    """One-time pairing flow — camera will show a confirmation popup.

    The user must physically tap 'accept' on the camera screen within ~30s.
    """
    cameras = await scan_for_cameras()
    if not cameras:
        raise RuntimeError("No DJI cameras found via BLE scan")
    if len(cameras) > 1:
        log.warning("Multiple DJI cameras found; using first: %s", cameras)
    camera = cameras[0]
    log.info("Pairing with %s (%s)", camera.name, camera.address)

    async with DJIBLESession(camera.address) as session:
        info = await session.pair(wait_for_user=True)

    pairing_store.save(storage_dir, info)
    return info


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
