"""Persistent storage for DJI BLE pairing state.

Pairings are stored one-per-camera under <storage_dir>/dji_pairings/<mac>.json,
where <mac> is the BLE address with colons stripped.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from car_media_manager.cameras.dji_ble.client import PairingInfo

log = logging.getLogger(__name__)

PAIRINGS_DIRNAME = "dji_pairings"


def _dir_for(storage_dir: Path) -> Path:
    return storage_dir / PAIRINGS_DIRNAME


def _path_for(storage_dir: Path, address: str) -> Path:
    slug = address.replace(":", "").lower()
    return _dir_for(storage_dir) / f"{slug}.json"


def load_all(storage_dir: Path) -> dict[str, PairingInfo]:
    pairings: dict[str, PairingInfo] = {}
    pairings_dir = _dir_for(storage_dir)
    if not pairings_dir.is_dir():
        return pairings
    for path in pairings_dir.glob("*.json"):
        try:
            data = json.loads(path.read_text())
            info = PairingInfo(
                address=data["address"],
                slot_number=int(data["slot_number"]),
                device_id=int(data["device_id"]),
            )
        except (OSError, ValueError, KeyError):
            log.exception("Failed to load pairing %s", path)
            continue
        pairings[info.address] = info
    return pairings


def save(storage_dir: Path, info: PairingInfo) -> None:
    path = _path_for(storage_dir, info.address)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "address": info.address,
        "slot_number": info.slot_number,
        "device_id": info.device_id,
    }))
    log.info("Saved DJI pairing for %s", info.address)


def clear(storage_dir: Path, address: str) -> None:
    path = _path_for(storage_dir, address)
    if path.is_file():
        path.unlink()
        log.info("Cleared DJI pairing for %s", address)
