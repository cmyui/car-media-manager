from __future__ import annotations

import logging
import re
from pathlib import Path

import httpx

from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import CameraVendor
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


def _discover_gopro_usb_serials() -> list[str]:
    serials: list[str] = []
    try:
        for device in Path("/sys/bus/usb/devices").iterdir():
            vendor_path = device / "idVendor"
            serial_path = device / "serial"
            if not vendor_path.exists() or not serial_path.exists():
                continue
            if vendor_path.read_text().strip() != GOPRO_USB_VENDOR_ID:
                continue
            serials.append(serial_path.read_text().strip())
    except OSError:
        pass
    return serials


class GoProCamera(Camera):
    vendor = CameraVendor.GOPRO
    display_name = "GoPro"

    def __init__(self, base_url: str, *, transport: str, serial: str) -> None:
        self.base_url = base_url
        self._transport = transport
        self._serial = serial
        self._client = httpx.AsyncClient(base_url=base_url, timeout=API_TIMEOUT)

    def __repr__(self) -> str:
        return f"GoProCamera({self._transport}={self._serial} @ {self.base_url})"

    @property
    def camera_id(self) -> str:
        return self._serial

    @property
    def capabilities(self) -> list[str]:
        return [self._transport.upper()]

    @property
    def supports_remote_control(self) -> bool:
        return True

    @classmethod
    async def discover(cls, *, storage_dir: Path) -> list[Camera]:
        cameras: list[Camera] = []

        # USB: one camera per GoPro serial. The hardcoded base URL only reaches
        # the first camera's USB network interface — additional GoPros would
        # need per-serial subnet discovery (Open GoPro: 172.2X.1YZ.51).
        serials = _discover_gopro_usb_serials()
        for serial in serials[:1]:
            cameras.append(cls(GOPRO_USB_BASE_URL, transport="usb", serial=serial))
            log.info("GoPro found via USB (serial=%s)", serial)
        if len(serials) > 1:
            log.warning(
                "%d GoPro USB devices detected; only the first is reachable at %s",
                len(serials),
                GOPRO_USB_BASE_URL,
            )

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
                        file_info.name,
                        resp.status_code,
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
