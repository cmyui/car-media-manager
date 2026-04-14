from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

import httpx

from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import MediaFileInfo
from car_media_manager.speed import ProgressCallback

log = logging.getLogger(__name__)

GOPRO_USB_VENDOR_ID = "2672"
GOPRO_USB_BASE_URL = "http://172.20.170.51:8080"
GOPRO_WIFI_BASE_URL = "http://10.5.5.9:8080"

MEDIA_EXTENSIONS = frozenset({".mp4", ".jpg", ".thm", ".lrv", ".360"})

API_TIMEOUT = httpx.Timeout(connect=5, read=10, write=10, pool=5)
DOWNLOAD_TIMEOUT = httpx.Timeout(connect=5, read=120, write=10, pool=5)
DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024

USB_DEVICE_RE = re.compile(
    rf"ID {GOPRO_USB_VENDOR_ID}:\w+\s+GoPro",
    re.IGNORECASE,
)


def _gopro_usb_connected() -> bool:
    try:
        lsusb = Path("/dev/bus/usb")
        if not lsusb.exists():
            return False
        # Check /sys/bus/usb for vendor ID
        for device in Path("/sys/bus/usb/devices").iterdir():
            vendor_path = device / "idVendor"
            if vendor_path.exists() and vendor_path.read_text().strip() == GOPRO_USB_VENDOR_ID:
                return True
    except OSError:
        pass
    return False


class GoProCamera(Camera):
    source_name = "gopro"
    display_name = "GoPro"

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self._client = httpx.AsyncClient(base_url=base_url, timeout=API_TIMEOUT)

    def __repr__(self) -> str:
        return f"GoProCamera({self.base_url})"

    @classmethod
    async def discover(cls) -> list[Camera]:
        cameras: list[Camera] = []

        if _gopro_usb_connected():
            cameras.append(cls(GOPRO_USB_BASE_URL))
            log.info("GoPro found via USB")

        # TODO: WiFi discovery via nmcli scan for GoPro SSID
        # For now, only USB is supported

        return cameras

    async def stop_recording(self) -> bool:
        try:
            resp = await self._client.get("/gopro/camera/shutter/stop")
            return resp.status_code == 200
        except httpx.HTTPError:
            log.exception("Failed to stop recording")
            return False

    async def start_recording(self) -> bool:
        try:
            resp = await self._client.get("/gopro/camera/shutter/start")
            return resp.status_code == 200
        except httpx.HTTPError:
            log.exception("Failed to start recording")
            return False

    async def list_media(self) -> list[MediaFileInfo]:
        try:
            resp = await self._client.get("/gopro/media/list")
            if resp.status_code != 200:
                return []
            data = resp.json()
        except httpx.HTTPError:
            log.exception("Failed to list media")
            return []

        files: list[MediaFileInfo] = []
        for directory in data.get("media", []):
            dir_name = directory.get("d", "")
            for f in directory.get("fs", []):
                name = f.get("n", "")
                suffix = Path(name).suffix.lower()
                if suffix not in MEDIA_EXTENSIONS:
                    continue
                files.append(
                    MediaFileInfo(
                        name=name,
                        size=int(f.get("s", 0)),
                        path=f"{dir_name}/{name}",
                    )
                )
        return sorted(files, key=lambda f: f.name)

    async def download_file(
        self,
        file_info: MediaFileInfo,
        dest: Path,
        on_progress: ProgressCallback | None = None,
    ) -> bool:
        url = f"/videos/DCIM/{file_info.path}"
        try:
            async with self._client.stream(
                "GET",
                url,
                timeout=DOWNLOAD_TIMEOUT,
            ) as resp:
                if resp.status_code != 200:
                    log.error(
                        "Download failed for %s: HTTP %d",
                        file_info.name, resp.status_code,
                    )
                    return False
                with open(dest, "wb") as f:
                    async for chunk in resp.aiter_bytes(DOWNLOAD_CHUNK_SIZE):
                        f.write(chunk)
                        if on_progress:
                            on_progress(len(chunk))
            return True
        except httpx.HTTPError:
            log.exception("Download failed for %s", file_info.name)
            if dest.exists():
                dest.unlink()
            return False
