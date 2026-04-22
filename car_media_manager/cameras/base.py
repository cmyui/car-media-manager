from __future__ import annotations

import abc
import logging
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any
from typing import ClassVar

from car_media_manager.speed import ProgressCallback

log = logging.getLogger(__name__)


class CameraVendor(StrEnum):
    DJI = "dji"
    GOPRO = "gopro"


@dataclass(frozen=True, slots=True)
class MediaFileInfo:
    name: str
    size: int
    path: str


class Camera(abc.ABC):
    vendor: ClassVar[CameraVendor]
    display_name: ClassVar[str]

    @property
    @abc.abstractmethod
    def camera_id(self) -> str:
        """Stable identifier unique within (vendor, camera_id)."""

    @property
    def capabilities(self) -> list[str]:
        return []

    @property
    def supports_pairing(self) -> bool:
        return False

    @property
    def is_paired(self) -> bool:
        return False

    @property
    def supports_remote_control(self) -> bool:
        return False

    async def pair(self, storage_dir: Path) -> dict[str, Any]:
        raise NotImplementedError

    async def unpair(self, storage_dir: Path) -> None:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    async def discover(cls, *, storage_dir: Path) -> list[Camera]:
        ...

    @abc.abstractmethod
    async def stop_recording(self) -> bool:
        ...

    @abc.abstractmethod
    async def start_recording(self) -> bool:
        ...

    @abc.abstractmethod
    async def list_media(self) -> list[MediaFileInfo]:
        ...

    @abc.abstractmethod
    async def download_file(
        self,
        file_info: MediaFileInfo,
        dest: Path,
        on_progress: ProgressCallback | None = None,
    ) -> bool:
        ...


class CameraRegistry:
    def __init__(self, *, storage_dir: Path) -> None:
        self._storage_dir = storage_dir
        self._types: list[type[Camera]] = []

    def register(self, cls: type[Camera]) -> None:
        self._types.append(cls)

    async def discover_all(self) -> list[Camera]:
        cameras: list[Camera] = []
        for cls in self._types:
            try:
                found = await cls.discover(storage_dir=self._storage_dir)
                cameras.extend(found)
            except Exception:
                log.exception("Discovery failed for %s", cls.__name__)
        return cameras

    async def find(self, vendor: CameraVendor, camera_id: str) -> Camera | None:
        for cam in await self.discover_all():
            if cam.vendor == vendor and cam.camera_id == camera_id:
                return cam
        return None
