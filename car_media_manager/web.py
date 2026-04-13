import asyncio
import shutil
from pathlib import Path

import jinja2
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import HTMLResponse
from mypy_boto3_s3 import S3Client

from car_media_manager import db
from car_media_manager import ingest
from car_media_manager import upload
from car_media_manager.settings import Settings

TEMPLATES_DIR = Path(__file__).parent / "templates"


def format_size(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024  # type: ignore[assignment]
    return f"{num_bytes:.1f} PB"


def _detected_cameras(settings: Settings) -> list[dict[str, str]]:
    detected: list[dict[str, str]] = []
    for mount_path, camera in ingest.find_camera_mounts(settings.volumes_root):
        detected.append(
            {
                "source": camera.source_name,
                "display_name": camera.display_name,
                "mount": str(mount_path),
            }
        )
    for mount_path, camera in ingest.find_mtp_cameras():
        detected.append(
            {
                "source": camera.source_name,
                "display_name": f"{camera.display_name} (USB)",
                "mount": str(mount_path),
            }
        )
    return detected


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
        stats = database.get_stats()
        recent_files = database.list_recent(limit=50)
        detected_cameras = await asyncio.to_thread(_detected_cameras, settings)
        has_internet_now = await asyncio.to_thread(upload.has_internet)
        disk = shutil.disk_usage(settings.storage_dir)

        template = env.get_template("dashboard.html")
        html = template.render(
            stats=stats,
            recent_files=recent_files,
            detected_cameras=detected_cameras,
            has_internet=has_internet_now,
            storage_used_value=format_size(stats["total_bytes"]),
            storage_total_value=format_size(disk.total),
            pending_size_display=format_size(stats["pending_bytes"]),
        )
        return HTMLResponse(html)

    @app.post("/api/ingest")
    async def api_ingest() -> dict[str, int]:
        ingested = await asyncio.to_thread(
            ingest.run_ingest_cycle,
            database=database,
            storage_dir=settings.storage_dir,
            volumes_root=settings.volumes_root,
        )
        return {"ingested": ingested}

    @app.post("/api/upload")
    async def api_upload() -> dict[str, int]:
        uploaded = await asyncio.to_thread(
            upload.run_upload_cycle,
            database=database,
            s3_client=s3_client,
            bucket=settings.s3_bucket_name,
            s3_prefix=settings.s3_prefix,
        )
        return {"uploaded": uploaded}

    @app.get("/api/stats")
    async def api_stats() -> dict[str, int]:
        return database.get_stats()

    return app
