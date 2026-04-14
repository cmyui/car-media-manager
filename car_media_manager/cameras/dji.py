from __future__ import annotations

import logging
from pathlib import Path

from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import MediaFileInfo
from car_media_manager.cameras.base import register_camera_type

log = logging.getLogger(__name__)


@register_camera_type
class DJIOsmoCamera(Camera):
    """Stub — BLE control + USB file access. Needs hardware to implement."""

    source_name = "dji"
    display_name = "DJI Osmo"

    @classmethod
    async def discover(cls) -> list[Camera]:
        # TODO: scan lsusb for DJI vendor ID, check BLE advertisements
        return []

    async def stop_recording(self) -> bool:
        # TODO: BLE DJI R SDK command CmdSet=0x1D, CmdID=0x03, record_ctrl=1
        raise NotImplementedError

    async def start_recording(self) -> bool:
        # TODO: BLE DJI R SDK command CmdSet=0x1D, CmdID=0x03, record_ctrl=0
        raise NotImplementedError

    async def list_media(self) -> list[MediaFileInfo]:
        # TODO: USB mass storage or MTP file listing
        raise NotImplementedError

    async def download_file(self, file_info: MediaFileInfo, dest: Path) -> bool:
        # TODO: USB file copy
        raise NotImplementedError
