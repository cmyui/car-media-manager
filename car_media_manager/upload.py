import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aioboto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from types_aiobotocore_s3 import S3Client

from car_media_manager import db
from car_media_manager.settings import Settings

log = logging.getLogger(__name__)

CONNECTIVITY_CHECK_HOST = "1.1.1.1"
CONNECTIVITY_CHECK_TIMEOUT_SECONDS = 3

MULTIPART_CHUNK_SIZE = 4 * 1024 * 1024
MULTIPART_THRESHOLD = 8 * 1024 * 1024
MAX_CONCURRENT_PARTS = 4

_upload_lock = asyncio.Lock()
_upload_transfer_config = TransferConfig(
    multipart_threshold=MULTIPART_THRESHOLD,
    multipart_chunksize=MULTIPART_CHUNK_SIZE,
    max_concurrency=MAX_CONCURRENT_PARTS,
    use_threads=True,
)


async def has_internet() -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ping",
            "-c",
            "1",
            "-W",
            str(CONNECTIVITY_CHECK_TIMEOUT_SECONDS),
            CONNECTIVITY_CHECK_HOST,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            returncode = await asyncio.wait_for(
                proc.wait(),
                timeout=CONNECTIVITY_CHECK_TIMEOUT_SECONDS + 2,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return False
        return returncode == 0
    except FileNotFoundError:
        return False


@asynccontextmanager
async def s3_client_context(settings: Settings) -> AsyncIterator[S3Client]:
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region_name,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        config=BotoConfig(
            retries={"max_attempts": 5, "mode": "adaptive"},
            connect_timeout=10,
            read_timeout=60,
        ),
    ) as client:
        yield client


async def put_file(
    *,
    s3_client: S3Client,
    bucket: str,
    local_path: str,
    s3_key: str,
) -> bool:
    try:
        await s3_client.upload_file(
            local_path,
            bucket,
            s3_key,
            Config=_upload_transfer_config,
        )
        return True
    except (BotoCoreError, ClientError):
        log.exception("S3 upload failed for %s", local_path)
        return False


async def run_upload_cycle(
    *,
    database: db.Database,
    s3_client: S3Client,
    bucket: str,
    s3_prefix: str,
) -> int:
    if _upload_lock.locked():
        log.debug("Upload cycle already in progress, skipping")
        return 0

    async with _upload_lock:
        if not await has_internet():
            log.debug("No internet connectivity")
            return 0

        pending = await database.list_pending_upload()
        if not pending:
            log.debug("No files pending upload")
            return 0

        log.info("%d files pending upload", len(pending))
        uploaded = 0

        for media_file in pending:
            local = Path(media_file.local_path)
            s3_key = f"{s3_prefix}/{media_file.source}/{local.parent.name}/{local.name}"

            log.info(
                "Uploading %s -> s3://%s/%s (%d bytes)",
                local.name, bucket, s3_key, media_file.file_size,
            )
            if await put_file(
                s3_client=s3_client,
                bucket=bucket,
                local_path=media_file.local_path,
                s3_key=s3_key,
            ):
                await database.mark_uploaded(media_file.id)
                uploaded += 1
                log.info("Uploaded %s", media_file.original_filename)
            else:
                log.warning(
                    "Failed to upload %s, will retry next cycle",
                    media_file.original_filename,
                )
                break

        return uploaded
