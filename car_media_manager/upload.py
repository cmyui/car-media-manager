import asyncio
import logging
import math
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aioboto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from types_aiobotocore_s3 import S3Client

from car_media_manager import db
from car_media_manager.settings import Settings

log = logging.getLogger(__name__)

CONNECTIVITY_CHECK_HOST = "1.1.1.1"
CONNECTIVITY_CHECK_TIMEOUT_SECONDS = 3

MULTIPART_CHUNK_SIZE = 8 * 1024 * 1024
MULTIPART_THRESHOLD = 8 * 1024 * 1024

_upload_lock = asyncio.Lock()


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


async def _put_object_single(
    *,
    s3_client: S3Client,
    bucket: str,
    s3_key: str,
    local_path: str,
) -> None:
    with open(local_path, "rb") as f:
        body = f.read()
    await s3_client.put_object(Bucket=bucket, Key=s3_key, Body=body)


async def _list_all_s3_parts(
    *,
    s3_client: S3Client,
    bucket: str,
    s3_key: str,
    s3_upload_id: str,
) -> list[db.MultipartPart]:
    parts: list[db.MultipartPart] = []
    marker: int | None = None
    while True:
        kwargs: dict = {
            "Bucket": bucket,
            "Key": s3_key,
            "UploadId": s3_upload_id,
        }
        if marker is not None:
            kwargs["PartNumberMarker"] = marker
        resp = await s3_client.list_parts(**kwargs)
        for part in resp.get("Parts", []):
            parts.append(
                db.MultipartPart(
                    part_number=part["PartNumber"],
                    etag=part["ETag"],
                    size=part["Size"],
                )
            )
        if not resp.get("IsTruncated"):
            break
        marker = resp.get("NextPartNumberMarker")
        if marker is None:
            break
    return parts


async def _abort_upload(
    *,
    s3_client: S3Client,
    bucket: str,
    s3_key: str,
    s3_upload_id: str,
) -> None:
    try:
        await s3_client.abort_multipart_upload(
            Bucket=bucket, Key=s3_key, UploadId=s3_upload_id,
        )
    except (BotoCoreError, ClientError):
        log.exception("Failed to abort multipart upload %s", s3_upload_id)


async def upload_file_resumable(
    *,
    s3_client: S3Client,
    database: db.Database,
    media_file: db.MediaFile,
    bucket: str,
    s3_key: str,
) -> bool:
    if media_file.file_size < MULTIPART_THRESHOLD:
        try:
            await _put_object_single(
                s3_client=s3_client,
                bucket=bucket,
                s3_key=s3_key,
                local_path=media_file.local_path,
            )
            return True
        except (BotoCoreError, ClientError):
            log.exception("put_object failed for %s", media_file.original_filename)
            return False

    record = await database.get_multipart_upload(media_file.id)

    if record is not None:
        try:
            s3_parts = await _list_all_s3_parts(
                s3_client=s3_client,
                bucket=record.s3_bucket,
                s3_key=record.s3_key,
                s3_upload_id=record.s3_upload_id,
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchUpload":
                log.warning(
                    "Multipart upload %s expired, restarting",
                    record.s3_upload_id,
                )
                await database.delete_multipart_upload(record.id)
                record = None
            else:
                log.exception(
                    "list_parts failed for %s", media_file.original_filename
                )
                return False
        else:
            await database.replace_parts(record.id, s3_parts)
            if record.part_size != MULTIPART_CHUNK_SIZE:
                log.warning(
                    "Part size changed (%d -> %d), restarting multipart upload",
                    record.part_size,
                    MULTIPART_CHUNK_SIZE,
                )
                await _abort_upload(
                    s3_client=s3_client,
                    bucket=record.s3_bucket,
                    s3_key=record.s3_key,
                    s3_upload_id=record.s3_upload_id,
                )
                await database.delete_multipart_upload(record.id)
                record = None
            elif record.s3_key != s3_key or record.s3_bucket != bucket:
                log.warning(
                    "S3 destination changed, restarting multipart upload",
                )
                await _abort_upload(
                    s3_client=s3_client,
                    bucket=record.s3_bucket,
                    s3_key=record.s3_key,
                    s3_upload_id=record.s3_upload_id,
                )
                await database.delete_multipart_upload(record.id)
                record = None

    if record is None:
        try:
            create_resp = await s3_client.create_multipart_upload(
                Bucket=bucket, Key=s3_key,
            )
        except (BotoCoreError, ClientError):
            log.exception("create_multipart_upload failed")
            return False
        record = await database.create_multipart_upload(
            media_file_id=media_file.id,
            s3_bucket=bucket,
            s3_key=s3_key,
            s3_upload_id=create_resp["UploadId"],
            part_size=MULTIPART_CHUNK_SIZE,
        )
        log.info("Started multipart upload %s for %s", record.s3_upload_id, s3_key)

    completed = await database.list_completed_parts(record.id)
    completed_by_num = {p.part_number: p for p in completed}
    total_parts = math.ceil(media_file.file_size / record.part_size)

    if total_parts > 10000:
        log.error(
            "File %s requires %d parts which exceeds S3's 10000 limit",
            media_file.original_filename, total_parts,
        )
        return False

    log.info(
        "Multipart upload: %d/%d parts already complete (%d bytes)",
        len(completed_by_num),
        total_parts,
        sum(p.size for p in completed),
    )

    try:
        with open(media_file.local_path, "rb") as f:
            for part_num in range(1, total_parts + 1):
                if part_num in completed_by_num:
                    continue

                f.seek((part_num - 1) * record.part_size)
                chunk = f.read(record.part_size)
                if not chunk:
                    log.error(
                        "Unexpected empty read for part %d of %s",
                        part_num, media_file.original_filename,
                    )
                    return False

                part_resp = await s3_client.upload_part(
                    Bucket=record.s3_bucket,
                    Key=record.s3_key,
                    UploadId=record.s3_upload_id,
                    PartNumber=part_num,
                    Body=chunk,
                )
                etag = part_resp["ETag"]
                await database.record_part_uploaded(
                    multipart_upload_id=record.id,
                    part_number=part_num,
                    etag=etag,
                    size=len(chunk),
                )
                completed_by_num[part_num] = db.MultipartPart(
                    part_number=part_num, etag=etag, size=len(chunk),
                )
                log.debug(
                    "Uploaded part %d/%d of %s",
                    part_num, total_parts, media_file.original_filename,
                )
    except FileNotFoundError:
        log.error(
            "Local file %s is gone; aborting multipart upload",
            media_file.local_path,
        )
        await _abort_upload(
            s3_client=s3_client,
            bucket=record.s3_bucket,
            s3_key=record.s3_key,
            s3_upload_id=record.s3_upload_id,
        )
        await database.delete_multipart_upload(record.id)
        return False
    except (BotoCoreError, ClientError):
        log.exception(
            "upload_part failed for %s (will resume next cycle)",
            media_file.original_filename,
        )
        return False

    parts_for_complete = [
        {"PartNumber": num, "ETag": completed_by_num[num].etag}
        for num in sorted(completed_by_num)
    ]
    try:
        await s3_client.complete_multipart_upload(
            Bucket=record.s3_bucket,
            Key=record.s3_key,
            UploadId=record.s3_upload_id,
            MultipartUpload={"Parts": parts_for_complete},
        )
    except (BotoCoreError, ClientError):
        log.exception(
            "complete_multipart_upload failed for %s",
            media_file.original_filename,
        )
        return False

    await database.delete_multipart_upload(record.id)
    return True


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
            if await upload_file_resumable(
                s3_client=s3_client,
                database=database,
                media_file=media_file,
                bucket=bucket,
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
