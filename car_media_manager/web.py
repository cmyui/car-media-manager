import asyncio
import shutil
from pathlib import Path
from typing import Any

import jinja2
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from types_aiobotocore_s3 import S3Client

from car_media_manager import db
from car_media_manager import ingest
from car_media_manager import upload
from car_media_manager.cameras.base import Camera
from car_media_manager.cameras.base import CameraRegistry
from car_media_manager.cameras.base import CameraVendor
from car_media_manager.settings import Settings
from car_media_manager.speed import ingest_tracker
from car_media_manager.speed import upload_tracker

TEMPLATES_DIR = Path(__file__).parent / "templates"


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


def create_app(
    *,
    settings: Settings,
    database: db.Database,
    s3_client: S3Client,
    registry: CameraRegistry,
) -> FastAPI:
    app = FastAPI(title="Car Media Manager")
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=True,
    )
    env.filters["format_size"] = format_size

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request) -> HTMLResponse:
        stats = await database.get_stats()
        recent_files = await database.list_recent(limit=50)
        found = await registry.discover_all()
        detected_cameras = [_camera_view(c) for c in found]
        has_internet_now = await upload.has_internet()
        disk = await asyncio.to_thread(shutil.disk_usage, settings.storage_dir)
        active_uploads = await database.list_active_multipart_progress()
        active_copies = await database.list_active_copies()

        ingest_speed = ingest_tracker.bytes_per_second()
        upload_speed = upload_tracker.bytes_per_second()

        # Files remaining on cameras
        camera_remaining_bytes = 0
        ingested_names: set[str] = set()
        for mf in await database.list_recent(limit=10000):
            ingested_names.add(f"{mf.vendor}:{mf.original_filename}")
        for cam in found:
            try:
                media = await cam.list_media()
                for f in media:
                    key = f"{cam.vendor}:{f.name}"
                    if key not in ingested_names:
                        camera_remaining_bytes += f.size
            except Exception:
                pass

        # Compute ETAs
        ingest_remaining = camera_remaining_bytes
        for mf in active_copies:
            dest = Path(mf.local_path)
            done = dest.stat().st_size if dest.exists() else 0
            ingest_remaining += mf.file_size - done

        upload_remaining = stats["pending_bytes"]
        ingest_eta = ingest_tracker.eta_seconds(ingest_remaining)
        upload_eta = upload_tracker.eta_seconds(upload_remaining)

        if ingest_eta is not None and upload_eta is not None:
            global_eta = max(ingest_eta, upload_eta)
        elif ingest_eta is not None:
            global_eta = ingest_eta
        elif upload_eta is not None:
            global_eta = upload_eta
        else:
            global_eta = None

        copy_progress: list[dict[str, object]] = []
        for mf in active_copies:
            dest = Path(mf.local_path)
            bytes_copied = dest.stat().st_size if dest.exists() else 0
            remaining = mf.file_size - bytes_copied
            copy_progress.append(
                {
                    "vendor": mf.vendor,
                    "original_filename": mf.original_filename,
                    "file_size": mf.file_size,
                    "bytes_copied": bytes_copied,
                    "percent": (bytes_copied / mf.file_size * 100) if mf.file_size else 0,
                    "speed": format_speed(ingest_speed),
                    "eta": format_eta(ingest_tracker.eta_seconds(remaining)),
                }
            )

        upload_progress: list[dict[str, object]] = []
        for u in active_uploads:
            remaining = u["file_size"] - u["bytes_uploaded"]
            u["speed"] = format_speed(upload_speed)
            u["eta"] = format_eta(upload_tracker.eta_seconds(remaining))
            upload_progress.append(u)

        uploading_file_ids = {u["media_file_id"] for u in active_uploads}

        template = env.get_template("dashboard.html")
        html = template.render(
            stats=stats,
            recent_files=recent_files,
            detected_cameras=detected_cameras,
            active_uploads=upload_progress,
            active_copies=copy_progress,
            uploading_file_ids=uploading_file_ids,
            has_internet=has_internet_now,
            storage_free_value=format_size(disk.free),
            storage_total_value=format_size(disk.total),
            pending_size_display=format_size(stats["pending_bytes"]),
            ingest_speed=format_speed(ingest_speed),
            upload_speed=format_speed(upload_speed),
            global_eta=format_eta(global_eta),
            camera_remaining=format_size(camera_remaining_bytes),
            total_uploaded_bytes=format_size(stats["total_bytes"] - stats["pending_bytes"]),
        )
        return HTMLResponse(html)

    @app.post("/api/ingest")
    async def api_ingest() -> dict[str, str]:
        asyncio.create_task(ingest.run_ingest_cycle(
            database=database,
            storage_dir=settings.storage_dir,
            registry=registry,
        ))
        return {"status": "started"}

    @app.post("/api/upload")
    async def api_upload() -> dict[str, str]:
        asyncio.create_task(upload.run_upload_cycle(
            database=database,
            s3_client=s3_client,
            bucket=settings.s3_bucket_name,
            s3_prefix=settings.s3_prefix,
        ))
        return {"status": "started"}

    @app.get("/api/stats")
    async def api_stats() -> dict[str, int]:
        return await database.get_stats()

    @app.get("/api/progress")
    async def api_progress() -> list[dict[str, Any]]:
        return await database.list_active_multipart_progress()

    @app.get("/api/cameras")
    async def api_cameras() -> list[dict[str, Any]]:
        return [_camera_view(c) for c in await registry.discover_all()]

    @app.post("/api/cameras/{vendor}/{camera_id}/pair")
    async def api_camera_pair(vendor: CameraVendor, camera_id: str) -> dict[str, Any]:
        cam = await registry.find(vendor, camera_id)
        if cam is None:
            raise HTTPException(status_code=404, detail="Camera not found")
        if not cam.supports_pairing:
            raise HTTPException(status_code=400, detail="Camera does not support pairing")
        try:
            return await cam.pair(settings.storage_dir)
        except Exception as e:
            return {"status": "error", "error": str(e)}

    @app.post("/api/cameras/{vendor}/{camera_id}/unpair")
    async def api_camera_unpair(vendor: CameraVendor, camera_id: str) -> dict[str, str]:
        cam = await registry.find(vendor, camera_id)
        if cam is None:
            raise HTTPException(status_code=404, detail="Camera not found")
        if not cam.supports_pairing:
            raise HTTPException(status_code=400, detail="Camera does not support pairing")
        await cam.unpair(settings.storage_dir)
        return {"status": "unpaired"}

    @app.post("/api/cameras/{vendor}/{camera_id}/start_recording")
    async def api_camera_start(vendor: CameraVendor, camera_id: str) -> dict[str, str]:
        cam = await registry.find(vendor, camera_id)
        if cam is None:
            raise HTTPException(status_code=404, detail="Camera not found")
        if not cam.supports_remote_control:
            raise HTTPException(status_code=400, detail="Camera does not support remote control")
        ok = await cam.start_recording()
        return {"status": "started" if ok else "failed"}

    @app.post("/api/cameras/{vendor}/{camera_id}/stop_recording")
    async def api_camera_stop(vendor: CameraVendor, camera_id: str) -> dict[str, str]:
        cam = await registry.find(vendor, camera_id)
        if cam is None:
            raise HTTPException(status_code=404, detail="Camera not found")
        if not cam.supports_remote_control:
            raise HTTPException(status_code=400, detail="Camera does not support remote control")
        ok = await cam.stop_recording()
        return {"status": "stopped" if ok else "failed"}

    return app
