import asyncio
import shutil
from pathlib import Path
from typing import Any

import jinja2
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import HTMLResponse
from types_aiobotocore_s3 import S3Client

from car_media_manager import db
from car_media_manager import ingest
from car_media_manager import upload
from car_media_manager.cameras.base import discover_cameras
from car_media_manager.settings import Settings

TEMPLATES_DIR = Path(__file__).parent / "templates"


def format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


async def _detected_cameras() -> list[dict[str, str]]:
    cameras = await discover_cameras()
    return [
        {
            "source": cam.source_name,
            "display_name": cam.display_name,
            "detail": repr(cam),
        }
        for cam in cameras
    ]


def create_app(
    *,
    settings: Settings,
    database: db.Database,
    s3_client: S3Client,
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
        detected_cameras = await _detected_cameras()
        has_internet_now = await upload.has_internet()
        disk = await asyncio.to_thread(shutil.disk_usage, settings.storage_dir)
        active_uploads = await database.list_active_multipart_progress()
        active_copies = await database.list_active_copies()

        copy_progress: list[dict[str, object]] = []
        for mf in active_copies:
            dest = Path(mf.local_path)
            bytes_copied = dest.stat().st_size if dest.exists() else 0
            copy_progress.append(
                {
                    "source": mf.source,
                    "original_filename": mf.original_filename,
                    "file_size": mf.file_size,
                    "bytes_copied": bytes_copied,
                    "percent": (bytes_copied / mf.file_size * 100) if mf.file_size else 0,
                }
            )

        template = env.get_template("dashboard.html")
        html = template.render(
            stats=stats,
            recent_files=recent_files,
            detected_cameras=detected_cameras,
            active_uploads=active_uploads,
            active_copies=copy_progress,
            has_internet=has_internet_now,
            storage_used_value=format_size(stats["total_bytes"]),
            storage_total_value=format_size(disk.total),
            pending_size_display=format_size(stats["pending_bytes"]),
        )
        return HTMLResponse(html)

    @app.post("/api/ingest")
    async def api_ingest() -> dict[str, int]:
        ingested = await ingest.run_ingest_cycle(
            database=database,
            storage_dir=settings.storage_dir,
        )
        return {"ingested": ingested}

    @app.post("/api/upload")
    async def api_upload() -> dict[str, int]:
        uploaded = await upload.run_upload_cycle(
            database=database,
            s3_client=s3_client,
            bucket=settings.s3_bucket_name,
            s3_prefix=settings.s3_prefix,
        )
        return {"uploaded": uploaded}

    @app.get("/api/stats")
    async def api_stats() -> dict[str, int]:
        return await database.get_stats()

    @app.get("/api/progress")
    async def api_progress() -> list[dict[str, Any]]:
        return await database.list_active_multipart_progress()

    return app
