from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import httpx

from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import MediaFileInfo

log = logging.getLogger(__name__)

GOPRO_USB_IP = "172.20.170.51"
GOPRO_WIFI_IP = "10.5.5.9"
GOPRO_PORT = 8080
MEDIA_EXTENSIONS = frozenset({".mp4", ".jpg", ".thm", ".lrv", ".360"})

DISCOVER_TIMEOUT = httpx.Timeout(connect=1.5, read=3, write=3, pool=3)
CONNECT_TIMEOUT = httpx.Timeout(connect=5, read=10, write=10, pool=5)
DOWNLOAD_TIMEOUT = httpx.Timeout(connect=5, read=120, write=10, pool=5)

DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024


class GoProCamera(Camera):
    source_name = "gopro"
    display_name = "GoPro"

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self._client = httpx.AsyncClient(base_url=base_url, timeout=CONNECT_TIMEOUT)

    def __repr__(self) -> str:
        return f"GoProCamera({self.base_url})"

    @classmethod
    async def _probe(cls, ip: str) -> Camera | None:
        url = f"http://{ip}:{GOPRO_PORT}"
        try:
            async with httpx.AsyncClient(timeout=DISCOVER_TIMEOUT) as probe:
                resp = await probe.get(f"{url}/gopro/camera/state")
                if resp.status_code == 200:
                    log.info("GoPro found at %s", url)
                    return cls(url)
        except httpx.HTTPError:
            pass
        return None

    @classmethod
    async def discover(cls) -> list[Camera]:
        results = await asyncio.gather(
            *[cls._probe(ip) for ip in (GOPRO_USB_IP, GOPRO_WIFI_IP)],
        )
        return [r for r in results if r is not None]

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

    async def download_file(self, file_info: MediaFileInfo, dest: Path) -> bool:
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
            return True
        except httpx.HTTPError:
            log.exception("Download failed for %s", file_info.name)
            if dest.exists():
                dest.unlink()
            return False
