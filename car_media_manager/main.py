import asyncio
import logging

import uvicorn
from types_aiobotocore_s3 import S3Client

from car_media_manager import ingest
from car_media_manager import upload
from car_media_manager.cameras import gopro as _gopro_reg  # noqa: F401 (triggers registration)
from car_media_manager.cameras import dji as _dji_reg  # noqa: F401 (triggers registration)
from car_media_manager.db import Database
from car_media_manager.settings import Settings
from car_media_manager.web import create_app

log = logging.getLogger("car_media_manager")


async def ingest_loop(*, settings: Settings, database: Database) -> None:
    while True:
        try:
            ingested = await ingest.run_ingest_cycle(
                database=database,
                storage_dir=settings.storage_dir,
            )
            if ingested:
                log.info("Ingest cycle: %d new files", ingested)
        except Exception:
            log.exception("Ingest cycle failed")
        await asyncio.sleep(settings.ingest_interval_seconds)


async def upload_loop(
    *,
    settings: Settings,
    database: Database,
    s3_client: S3Client,
) -> None:
    while True:
        try:
            uploaded = await upload.run_upload_cycle(
                database=database,
                s3_client=s3_client,
                bucket=settings.s3_bucket_name,
                s3_prefix=settings.s3_prefix,
            )
            if uploaded:
                log.info("Upload cycle: %d files uploaded", uploaded)
        except Exception:
            log.exception("Upload cycle failed")
        await asyncio.sleep(settings.upload_interval_seconds)


async def run() -> None:
    settings = Settings(_env_file=".env")  # type: ignore[call-arg]
    settings.storage_dir.mkdir(parents=True, exist_ok=True)

    database = Database(settings.db_path)
    await database.connect()

    log.info("Starting Car Media Manager on port %d", settings.web_port)
    log.info("Storage: %s", settings.storage_dir.resolve())
    log.info("Database: %s", settings.db_path.resolve())

    async with upload.s3_client_context(settings) as s3_client:
        app = create_app(settings=settings, database=database, s3_client=s3_client)

        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=settings.web_port,
            log_level="warning",
        )
        server = uvicorn.Server(config)

        ingest_task = asyncio.create_task(
            ingest_loop(settings=settings, database=database),
        )
        upload_task = asyncio.create_task(
            upload_loop(settings=settings, database=database, s3_client=s3_client),
        )

        try:
            await server.serve()
        finally:
            ingest_task.cancel()
            upload_task.cancel()
            await database.disconnect()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
