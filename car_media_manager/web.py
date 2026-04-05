import asyncio
from pathlib import Path

import jinja2
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import HTMLResponse

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


def create_app(*, settings: Settings, database: db.Database) -> FastAPI:
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
        gopro_connected = ingest.find_camera_volume(settings.gopro_volume_name) is not None
        insta360_connected = ingest.find_camera_volume(settings.insta360_volume_name) is not None
        has_internet_now = await asyncio.to_thread(upload.has_internet)

        template = env.get_template("dashboard.html")
        html = template.render(
            stats=stats,
            recent_files=recent_files,
            gopro_connected=gopro_connected,
            insta360_connected=insta360_connected,
            has_internet=has_internet_now,
            total_size_display=format_size(stats["total_bytes"]),
            pending_size_display=format_size(stats["pending_bytes"]),
        )
        return HTMLResponse(html)

    @app.post("/api/ingest")
    async def api_ingest() -> dict[str, int]:
        ingested = await asyncio.to_thread(
            ingest.run_ingest_cycle,
            database=database,
            storage_dir=settings.storage_dir,
            gopro_volume_name=settings.gopro_volume_name,
            insta360_volume_name=settings.insta360_volume_name,
        )
        return {"ingested": ingested}

    @app.post("/api/upload")
    async def api_upload() -> dict[str, int]:
        uploaded = await asyncio.to_thread(
            upload.run_upload_cycle,
            database=database,
            rclone_remote=settings.rclone_remote,
        )
        return {"uploaded": uploaded}

    @app.get("/api/stats")
    async def api_stats() -> dict[str, int]:
        return database.get_stats()

    return app
