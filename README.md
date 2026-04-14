# Car Media Manager

Auto-sync footage from car-mounted cameras to Wasabi S3.

Built for a multi-camera road trip rig running on a Raspberry Pi 5, powered
from 12V. Currently supports GoPro (via HTTP API over USB/WiFi), with DJI Osmo
360 support in progress.

## How it works

```
Cameras record → Pi pulls files via HTTP API (GoPro) or USB
→ stores on local SSD → uploads to Wasabi S3 when internet is available
```

- **Ingest**: Discovers connected cameras, lists media via camera-specific
  adapters, downloads to local SSD, tracks state in SQLite
- **Upload**: Resumable multipart uploads to S3 with per-part checkpointing.
  Survives restarts — resumes from last completed part
- **Dashboard**: FastAPI web UI showing cameras, copy/upload progress with
  speed and ETA, storage usage, and manual trigger buttons

## Setup

```bash
uv sync
cp .env.example .env
# Fill in your Wasabi S3 credentials in .env
```

## Run

```bash
uv run python -m car_media_manager.main
```

Open `http://localhost:8000` for the dashboard.

## Install as a systemd user service (Raspberry Pi)

```bash
./deploy/install-service.sh
```

This installs and enables the service so it starts on boot and restarts on
failure. Common operations:

```bash
systemctl --user status car-media-manager   # check status
systemctl --user restart car-media-manager  # restart
systemctl --user stop car-media-manager     # stop
journalctl --user -u car-media-manager -f   # tail logs
```

## Configuration

All config via environment variables (prefix `CMM_`), loaded from `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `CMM_STORAGE_DIR` | Local path for ingested files | - |
| `CMM_DB_PATH` | SQLite database path | - |
| `CMM_WEB_PORT` | Dashboard port | `8000` |
| `CMM_S3_ENDPOINT_URL` | S3-compatible endpoint | - |
| `CMM_S3_REGION_NAME` | S3 region | - |
| `CMM_S3_BUCKET_NAME` | S3 bucket | - |
| `CMM_S3_ACCESS_KEY_ID` | S3 access key | - |
| `CMM_S3_SECRET_ACCESS_KEY` | S3 secret key | - |
| `CMM_S3_PREFIX` | Key prefix in bucket | `car-footage` |
| `CMM_INGEST_INTERVAL_SECONDS` | Seconds between ingest cycles | `300` |
| `CMM_UPLOAD_INTERVAL_SECONDS` | Seconds between upload cycles | `60` |

## Camera support

Cameras are pluggable via the `Camera` ABC in `cameras/base.py`. Each adapter
implements discovery, recording control, media listing, and file download.

| Camera | Status | Protocol |
|--------|--------|----------|
| GoPro (Hero 13, Max 2) | Working | HTTP API over USB NCM or WiFi |
| DJI Osmo 360 | Stub | BLE control (DJI R SDK) + USB file access |

## Project structure

```
car_media_manager/
    cameras/
        base.py      — Camera ABC + CameraRegistry
        gopro.py     — GoPro HTTP API adapter
        dji.py       — DJI Osmo stub
    settings.py      — pydantic-settings config
    db.py            — SQLite schema + operations (databases + aiosqlite)
    ingest.py        — Ingest orchestrator with partial copy recovery
    upload.py        — Resumable multipart S3 upload with per-part checkpointing
    speed.py         — Rolling speed tracker for ingest/upload throughput
    web.py           — FastAPI dashboard + API endpoints
    main.py          — Entry point, background loops, camera registration
    templates/
        dashboard.html
    deploy/
        car-media-manager.service  — systemd user service
        install-service.sh
```
