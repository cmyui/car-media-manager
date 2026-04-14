from __future__ import annotations

import abc
import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MediaFileInfo:
    name: str
    size: int
    path: str


class Camera(abc.ABC):
    source_name: str
    display_name: str

    @classmethod
    @abc.abstractmethod
    async def discover(cls) -> list[Camera]:
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
    async def download_file(self, file_info: MediaFileInfo, dest: Path) -> bool:
        ...


_CAMERA_TYPES: list[type[Camera]] = []


def register_camera_type(cls: type[Camera]) -> type[Camera]:
    _CAMERA_TYPES.append(cls)
    return cls


async def discover_cameras() -> list[Camera]:
    cameras: list[Camera] = []
    for cls in _CAMERA_TYPES:
        try:
            found = await cls.discover()
            cameras.extend(found)
        except Exception:
            log.exception("Discovery failed for %s", cls.__name__)
    return cameras
