"""Persistent storage for DJI BLE pairing state."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from car_media_manager.cameras.dji_ble.client import PairingInfo

log = logging.getLogger(__name__)

PAIRING_FILENAME = "dji_pairing.json"


def _path_for(storage_dir: Path) -> Path:
    return storage_dir / PAIRING_FILENAME


def load(storage_dir: Path) -> PairingInfo | None:
    path = _path_for(storage_dir)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text())
        return PairingInfo(
            address=data["address"],
            slot_number=int(data["slot_number"]),
            device_id=int(data["device_id"]),
        )
    except (OSError, ValueError, KeyError):
        log.exception("Failed to load pairing info")
        return None


def save(storage_dir: Path, info: PairingInfo) -> None:
    path = _path_for(storage_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "address": info.address,
        "slot_number": info.slot_number,
        "device_id": info.device_id,
    }))
    log.info("Saved DJI pairing to %s", path)


def clear(storage_dir: Path) -> None:
    path = _path_for(storage_dir)
    if path.is_file():
        path.unlink()
        log.info("Cleared DJI pairing")
