from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Any
from typing import Awaitable
from typing import Callable

from car_media_manager import db
from car_media_manager import runtime
from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import CameraRegistry
from car_media_manager.settings import Settings
from car_media_manager.speed import ingest_tracker
from car_media_manager.speed import upload_tracker

InternetCheck = Callable[[], Awaitable[bool]]


def format_size(num_bytes: int | float) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def format_eta(seconds: float | None) -> str:
    if seconds is None or seconds <= 0:
        return "-"
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def format_speed(bps: float) -> str:
    if bps <= 0:
        return "-"
    return f"{format_size(bps)}/s"


def _camera_view(cam: Camera) -> dict[str, Any]:
    return {
        "vendor": cam.vendor,
        "camera_id": cam.camera_id,
        "display_name": cam.display_name,
        "detail": repr(cam),
        "capabilities": cam.capabilities,
        "supports_pairing": cam.supports_pairing,
        "supports_remote_control": cam.supports_remote_control,
        "is_paired": cam.is_paired,
    }


def _datetime_display(value: Any) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M")


def _media_file_view(media_file: db.MediaFile, uploading_file_ids: set[int]) -> dict[str, Any]:
    if media_file.uploaded_at is not None:
        state = "uploaded"
        status_label = "Uploaded"
    elif media_file.ingested_at is None:
        state = "copying"
        status_label = "Copying..."
    elif media_file.id in uploading_file_ids:
        state = "uploading"
        status_label = "Uploading..."
    else:
        state = "pending"
        status_label = "Awaiting Upload"

    return {
        "id": media_file.id,
        "vendor": media_file.vendor,
        "original_filename": media_file.original_filename,
        "file_size": media_file.file_size,
        "file_size_display": format_size(media_file.file_size),
        "ingested_at": media_file.ingested_at.isoformat() if media_file.ingested_at else None,
        "ingested_at_display": _datetime_display(media_file.ingested_at),
        "uploaded_at": media_file.uploaded_at.isoformat() if media_file.uploaded_at else None,
        "uploaded_at_display": _datetime_display(media_file.uploaded_at),
        "state": state,
        "status_label": status_label,
    }


def _safe_stat_size(path: str) -> int:
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0


async def build_status(
    *,
    settings: Settings,
    database: db.Database,
    registry: CameraRegistry,
    has_internet: InternetCheck,
) -> dict[str, Any]:
    stats, recent_files, pending_upload, found, has_internet_now, disk, active_uploads, active_copies = await asyncio.gather(
        database.get_stats(),
        database.list_recent(limit=50),
        database.list_pending_upload(),
        registry.discover_all(),
        has_internet(),
        asyncio.to_thread(shutil.disk_usage, settings.storage_dir),
        database.list_active_multipart_progress(),
        database.list_active_copies(),
    )

    detected_cameras = [_camera_view(c) for c in found]
    ingest_speed = ingest_tracker.bytes_per_second()
    upload_speed = upload_tracker.bytes_per_second()
    uploading_file_ids = {u["media_file_id"] for u in active_uploads}

    ingested_names: set[str] = set()
    for mf in await database.list_recent(limit=10000):
        ingested_names.add(f"{mf.vendor}:{mf.original_filename}")

    camera_remaining_bytes = 0
    camera_remaining_files = 0
    for cam in found:
        try:
            media = await cam.list_media()
        except Exception:
            continue
        for file_info in media:
            key = f"{cam.vendor}:{file_info.name}"
            if key not in ingested_names:
                camera_remaining_bytes += file_info.size
                camera_remaining_files += 1

    copy_progress: list[dict[str, Any]] = []
    ingest_remaining = camera_remaining_bytes
    for mf in active_copies:
        bytes_copied = _safe_stat_size(mf.local_path)
        remaining = max(mf.file_size - bytes_copied, 0)
        ingest_remaining += remaining
        eta_seconds = ingest_tracker.eta_seconds(remaining)
        copy_progress.append(
            {
                "media_file_id": mf.id,
                "vendor": mf.vendor,
                "original_filename": mf.original_filename,
                "file_size": mf.file_size,
                "file_size_display": format_size(mf.file_size),
                "bytes_copied": bytes_copied,
                "bytes_copied_display": format_size(bytes_copied),
                "percent": (bytes_copied / mf.file_size * 100) if mf.file_size else 0,
                "speed": ingest_speed,
                "speed_display": format_speed(ingest_speed),
                "eta_seconds": eta_seconds,
                "eta_display": format_eta(eta_seconds),
            }
        )

    upload_progress: list[dict[str, Any]] = []
    for upload_row in active_uploads:
        remaining = max(upload_row["file_size"] - upload_row["bytes_uploaded"], 0)
        eta_seconds = upload_tracker.eta_seconds(remaining)
        upload_progress.append(
            {
                **upload_row,
                "file_size_display": format_size(upload_row["file_size"]),
                "bytes_uploaded_display": format_size(upload_row["bytes_uploaded"]),
                "speed": upload_speed,
                "speed_display": format_speed(upload_speed),
                "eta_seconds": eta_seconds,
                "eta_display": format_eta(eta_seconds),
            }
        )

    pending_upload_bytes = sum(mf.file_size for mf in pending_upload)
    pending_upload_files = len(pending_upload)
    upload_eta_seconds = upload_tracker.eta_seconds(pending_upload_bytes)
    ingest_eta_seconds = ingest_tracker.eta_seconds(ingest_remaining)

    if ingest_eta_seconds is not None and upload_eta_seconds is not None:
        overall_eta_seconds = max(ingest_eta_seconds, upload_eta_seconds)
    elif ingest_eta_seconds is not None:
        overall_eta_seconds = ingest_eta_seconds
    elif upload_eta_seconds is not None:
        overall_eta_seconds = upload_eta_seconds
    else:
        overall_eta_seconds = None

    return {
        "cameras": detected_cameras,
        "has_internet": has_internet_now,
        "disk": {
            "free": disk.free,
            "total": disk.total,
            "free_display": format_size(disk.free),
            "total_display": format_size(disk.total),
        },
        "stats": stats,
        "pending_upload": {
            "files": pending_upload_files,
            "bytes": pending_upload_bytes,
            "bytes_display": format_size(pending_upload_bytes),
        },
        "camera_remaining": {
            "files": camera_remaining_files,
            "bytes": camera_remaining_bytes,
            "bytes_display": format_size(camera_remaining_bytes),
        },
        "active_copies": copy_progress,
        "active_uploads": upload_progress,
        "recent_files": [
            _media_file_view(media_file, uploading_file_ids) for media_file in recent_files
        ],
        "uploading_file_ids": list(uploading_file_ids),
        "speeds": {
            "ingest_bps": ingest_speed,
            "upload_bps": upload_speed,
            "ingest_display": format_speed(ingest_speed),
            "upload_display": format_speed(upload_speed),
        },
        "etas": {
            "ingest_seconds": ingest_eta_seconds,
            "upload_seconds": upload_eta_seconds,
            "overall_seconds": overall_eta_seconds,
            "ingest_display": format_eta(ingest_eta_seconds),
            "upload_display": format_eta(upload_eta_seconds),
            "overall_display": format_eta(overall_eta_seconds),
        },
        "last_message": runtime.get_message(),
        "display": {
            "total_uploaded_bytes": format_size(stats["total_bytes"] - stats["pending_bytes"]),
        },
    }
