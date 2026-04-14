from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import aiohttp

from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import MediaFileInfo
from car_media_manager.cameras.base import register_camera_type

log = logging.getLogger(__name__)

GOPRO_USB_SUBNET = "172.20.170"
GOPRO_USB_IP = "172.20.170.51"
GOPRO_WIFI_IP = "10.5.5.9"
GOPRO_PORT = 8080
MEDIA_EXTENSIONS = frozenset({".mp4", ".jpg", ".thm", ".lrv", ".360"})

CONNECT_TIMEOUT = aiohttp.ClientTimeout(total=5)
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=3600, sock_read=120)


@register_camera_type
class GoProCamera(Camera):
    source_name = "gopro"
    display_name = "GoPro"

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def __repr__(self) -> str:
        return f"GoProCamera({self.base_url})"

    @classmethod
    async def discover(cls) -> list[Camera]:
        cameras: list[Camera] = []
        for ip in (GOPRO_USB_IP, GOPRO_WIFI_IP):
            url = f"http://{ip}:{GOPRO_PORT}"
            try:
                async with aiohttp.ClientSession(timeout=CONNECT_TIMEOUT) as session:
                    async with session.get(f"{url}/gopro/camera/state") as resp:
                        if resp.status == 200:
                            cameras.append(cls(url))
                            log.info("GoPro found at %s", url)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                continue
        return cameras

    async def stop_recording(self) -> bool:
        try:
            async with aiohttp.ClientSession(timeout=CONNECT_TIMEOUT) as session:
                async with session.get(
                    f"{self.base_url}/gopro/camera/shutter/stop",
                ) as resp:
                    return resp.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError):
            log.exception("Failed to stop recording")
            return False

    async def start_recording(self) -> bool:
        try:
            async with aiohttp.ClientSession(timeout=CONNECT_TIMEOUT) as session:
                async with session.get(
                    f"{self.base_url}/gopro/camera/shutter/start",
                ) as resp:
                    return resp.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError):
            log.exception("Failed to start recording")
            return False

    async def list_media(self) -> list[MediaFileInfo]:
        try:
            async with aiohttp.ClientSession(timeout=CONNECT_TIMEOUT) as session:
                async with session.get(
                    f"{self.base_url}/gopro/media/list",
                ) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
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

    async def download_file(self, file_info: MediaFileInfo, dest: Path) -> bool:
        url = f"{self.base_url}/videos/DCIM/{file_info.path}"
        try:
            async with aiohttp.ClientSession(timeout=DOWNLOAD_TIMEOUT) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        log.error(
                            "Download failed for %s: HTTP %d",
                            file_info.name, resp.status,
                        )
                        return False
                    with open(dest, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8 * 1024 * 1024):
                            f.write(chunk)
            return True
        except (aiohttp.ClientError, asyncio.TimeoutError):
            log.exception("Download failed for %s", file_info.name)
            if dest.exists():
                dest.unlink()
            return False
