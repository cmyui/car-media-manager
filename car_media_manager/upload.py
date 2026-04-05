import logging
import subprocess
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from car_media_manager import db
from car_media_manager.settings import Settings

log = logging.getLogger(__name__)

CONNECTIVITY_CHECK_HOST = "1.1.1.1"
CONNECTIVITY_CHECK_TIMEOUT_SECONDS = 3


def has_internet() -> bool:
    try:
        subprocess.run(
            ["ping", "-c", "1", "-W", str(CONNECTIVITY_CHECK_TIMEOUT_SECONDS), CONNECTIVITY_CHECK_HOST],
            capture_output=True,
            timeout=CONNECTIVITY_CHECK_TIMEOUT_SECONDS + 2,
        )
        return True
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        return False


def create_s3_client(settings: Settings) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region_name,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        config=BotoConfig(
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


def upload_file(
    *,
    s3_client: S3Client,
    bucket: str,
    local_path: str,
    s3_key: str,
) -> bool:
    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        return True
    except (BotoCoreError, ClientError):
        log.exception("S3 upload failed for %s", local_path)
        return False


def run_upload_cycle(
    *,
    database: db.Database,
    s3_client: S3Client,
    bucket: str,
    s3_prefix: str,
) -> int:
    if not has_internet():
        log.debug("No internet connectivity")
        return 0

    pending = database.list_pending_upload()
    if not pending:
        log.debug("No files pending upload")
        return 0

    log.info("%d files pending upload", len(pending))
    uploaded = 0

    for media_file in pending:
        local = Path(media_file.local_path)
        s3_key = f"{s3_prefix}/{media_file.source}/{local.parent.name}/{local.name}"

        log.info("Uploading %s -> s3://%s/%s (%d bytes)", local.name, bucket, s3_key, media_file.file_size)
        if upload_file(s3_client=s3_client, bucket=bucket, local_path=media_file.local_path, s3_key=s3_key):
            database.mark_uploaded(media_file.id)
            uploaded += 1
            log.info("Uploaded %s", media_file.original_filename)
        else:
            log.warning("Failed to upload %s, will retry next cycle", media_file.original_filename)
            break

    return uploaded
