import logging
import subprocess

from car_media_manager import db

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


def upload_file(*, rclone_remote: str, local_path: str) -> bool:
    try:
        result = subprocess.run(
            ["rclone", "copy", local_path, rclone_remote, "--progress"],
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            log.error("rclone failed: %s", result.stderr)
            return False
        return True
    except subprocess.TimeoutExpired:
        log.error("rclone timed out uploading %s", local_path)
        return False
    except FileNotFoundError:
        log.error("rclone not found — install it: https://rclone.org/install/")
        return False


def run_upload_cycle(*, database: db.Database, rclone_remote: str) -> int:
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
        log.info("Uploading %s (%d bytes)", media_file.local_path, media_file.file_size)
        if upload_file(rclone_remote=rclone_remote, local_path=media_file.local_path):
            database.mark_uploaded(media_file.id)
            uploaded += 1
            log.info("Uploaded %s", media_file.original_filename)
        else:
            log.warning("Failed to upload %s, will retry next cycle", media_file.original_filename)
            break

    return uploaded
