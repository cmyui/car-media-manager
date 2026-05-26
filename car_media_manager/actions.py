from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from car_media_manager.settings import Settings


@dataclass(frozen=True, slots=True)
class ActionDecision:
    http_status: int
    status: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {
            "status": self.status,
            "message": self.message,
        }


def missing_s3_settings(settings: Settings) -> list[str]:
    required = {
        "CMM_S3_BUCKET_NAME": settings.s3_bucket_name,
        "CMM_S3_ACCESS_KEY_ID": settings.s3_access_key_id,
        "CMM_S3_SECRET_ACCESS_KEY": settings.s3_secret_access_key,
    }
    return [name for name, value in required.items() if not value]


def decide_ingest_action(*, status: dict[str, Any], is_running: bool) -> ActionDecision:
    if is_running:
        return ActionDecision(
            http_status=409,
            status="already_running",
            message="Ingest is already running",
        )
    storage = status.get("storage") or {}
    if storage.get("is_below_reserve"):
        return ActionDecision(
            http_status=400,
            status="blocked",
            message="Waiting for upload to free space",
        )
    if not status["cameras"]:
        return ActionDecision(
            http_status=200,
            status="noop",
            message="No cameras detected",
        )
    return ActionDecision(
        http_status=202,
        status="accepted",
        message="Ingest queued",
    )


def decide_upload_action(
    *,
    settings: Settings,
    status: dict[str, Any],
    is_running: bool,
) -> ActionDecision:
    if is_running:
        return ActionDecision(
            http_status=409,
            status="already_running",
            message="Upload is already running",
        )

    missing = missing_s3_settings(settings)
    if missing:
        return ActionDecision(
            http_status=400,
            status="blocked",
            message=f"Missing S3 settings: {', '.join(missing)}",
        )

    if not status["has_internet"]:
        return ActionDecision(
            http_status=400,
            status="blocked",
            message="No internet connectivity",
        )

    if status["pending_upload"]["files"] <= 0:
        return ActionDecision(
            http_status=200,
            status="noop",
            message="No files pending upload",
        )

    return ActionDecision(
        http_status=202,
        status="accepted",
        message="Upload queued",
    )
