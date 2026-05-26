from types import SimpleNamespace

from car_media_manager.actions import decide_ingest_action
from car_media_manager.actions import decide_upload_action


def _settings(**overrides: str) -> SimpleNamespace:
    values = {
        "s3_bucket_name": "bucket",
        "s3_access_key_id": "key",
        "s3_secret_access_key": "secret",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _status(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "cameras": [{"camera_id": "cam"}],
        "has_internet": True,
        "pending_upload": {"files": 1},
    }
    values.update(overrides)
    return values


def test_ingest_action_reports_already_running() -> None:
    decision = decide_ingest_action(status=_status(), is_running=True)

    assert decision.http_status == 409
    assert decision.status == "already_running"


def test_ingest_action_noops_without_cameras() -> None:
    decision = decide_ingest_action(
        status=_status(cameras=[]),
        is_running=False,
    )

    assert decision.http_status == 200
    assert decision.status == "noop"


def test_ingest_action_blocks_low_storage() -> None:
    decision = decide_ingest_action(
        status=_status(storage={"is_below_reserve": True}),
        is_running=False,
    )

    assert decision.http_status == 400
    assert decision.status == "blocked"


def test_upload_action_blocks_missing_s3_config() -> None:
    decision = decide_upload_action(
        settings=_settings(s3_bucket_name=""),
        status=_status(),
        is_running=False,
    )

    assert decision.http_status == 400
    assert decision.status == "blocked"
    assert "CMM_S3_BUCKET_NAME" in decision.message


def test_upload_action_noops_without_pending_files() -> None:
    decision = decide_upload_action(
        settings=_settings(),
        status=_status(pending_upload={"files": 0}),
        is_running=False,
    )

    assert decision.http_status == 200
    assert decision.status == "noop"


def test_upload_action_accepts_ready_upload() -> None:
    decision = decide_upload_action(
        settings=_settings(),
        status=_status(),
        is_running=False,
    )

    assert decision.http_status == 202
    assert decision.status == "accepted"
