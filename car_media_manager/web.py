import asyncio
from pathlib import Path
from typing import Any

import jinja2
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from types_aiobotocore_s3 import S3Client

from car_media_manager import actions
from car_media_manager import db
from car_media_manager import ingest
from car_media_manager import runtime
from car_media_manager import status as status_builder
from car_media_manager import upload
from car_media_manager.cameras.base import CameraRegistry
from car_media_manager.cameras.base import CameraVendor
from car_media_manager.settings import Settings

TEMPLATES_DIR = Path(__file__).parent / "templates"


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
    env.filters["format_size"] = status_builder.format_size

    async def get_status() -> dict[str, Any]:
        return await status_builder.build_status(
            settings=settings,
            database=database,
            registry=registry,
            has_internet=upload.has_internet,
        )

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> HTMLResponse:
        current = await get_status()
        template = env.get_template("dashboard.html")
        html = template.render(
            status=current,
            stats=current["stats"],
            recent_files=current["recent_files"],
            detected_cameras=current["cameras"],
            active_uploads=current["active_uploads"],
            active_copies=current["active_copies"],
            has_internet=current["has_internet"],
            storage_free_value=current["disk"]["free_display"],
            storage_total_value=current["disk"]["total_display"],
            pending_size_display=current["pending_upload"]["bytes_display"],
            ingest_speed=current["speeds"]["ingest_display"],
            upload_speed=current["speeds"]["upload_display"],
            global_eta=current["etas"]["overall_display"],
            camera_remaining=current["camera_remaining"]["bytes_display"],
            total_uploaded_bytes=current["display"]["total_uploaded_bytes"],
        )
        return HTMLResponse(html)

    @app.post("/api/ingest")
    async def api_ingest() -> JSONResponse:
        current = await get_status()
        decision = actions.decide_ingest_action(
            status=current,
            is_running=ingest.is_running(),
        )
        if decision.status != "accepted":
            runtime.set_message(
                level="info" if decision.status == "noop" else "warning",
                code=decision.status,
                message=decision.message,
            )
            return JSONResponse(
                status_code=decision.http_status,
                content=decision.as_dict(),
            )
        asyncio.create_task(ingest.run_ingest_cycle(
            database=database,
            storage_dir=settings.storage_dir,
            registry=registry,
            free_space_reserve_bytes=settings.ingest_free_space_reserve_bytes,
        ))
        runtime.clear_message()
        return JSONResponse(status_code=decision.http_status, content=decision.as_dict())

    @app.post("/api/upload")
    async def api_upload() -> JSONResponse:
        current = await get_status()
        decision = actions.decide_upload_action(
            settings=settings,
            status=current,
            is_running=upload.is_running(),
        )
        if decision.status != "accepted":
            runtime.set_message(
                level="info" if decision.status == "noop" else "warning",
                code=decision.status,
                message=decision.message,
            )
            return JSONResponse(
                status_code=decision.http_status,
                content=decision.as_dict(),
            )
        asyncio.create_task(upload.run_upload_cycle(
            database=database,
            s3_client=s3_client,
            bucket=settings.s3_bucket_name,
            s3_prefix=settings.s3_prefix,
        ))
        runtime.clear_message()
        return JSONResponse(status_code=decision.http_status, content=decision.as_dict())

    @app.get("/api/status")
    async def api_status() -> dict[str, Any]:
        return await get_status()

    @app.get("/api/stats")
    async def api_stats() -> dict[str, int]:
        return await database.get_stats()

    @app.get("/api/progress")
    async def api_progress() -> list[dict[str, Any]]:
        return await database.list_active_multipart_progress()

    @app.get("/api/cameras")
    async def api_cameras() -> list[dict[str, Any]]:
        current = await get_status()
        return current["cameras"]

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
