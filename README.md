# Car Media Manager

Auto-sync footage from car-mounted cameras to Wasabi S3.

Built for a multi-camera road trip rig: GoPro Hero 13 (interior), Insta360 X4
(exterior 360), and DJI Mic 2 (audio). Runs on a Raspberry Pi 5 in the car,
powered from 12V.

## How it works

```
Cameras record to SD → Pi pulls files over USB (MTP) at rest stops
→ stores on local SSD → uploads to Wasabi S3 when internet is available
```

- **Ingest**: Detects cameras via USB volume mount or MTP, scans for new media,
  copies to local SSD, tracks state in SQLite
- **Upload**: Checks internet connectivity, uploads pending files to Wasabi S3
  via boto3, marks as uploaded
- **Dashboard**: FastAPI web UI at `http://<pi-ip>:8000` showing sync status,
  upload queue, storage usage, and manual trigger buttons

## Setup

```bash
uv sync
cp .env.example .env
# Fill in your Wasabi S3 credentials in .env
```

For MTP camera support:
```bash
# macOS
brew install go-mtpfs

# Linux (Raspberry Pi)
sudo apt install jmtpfs
```

## Run

```bash
uv run python -m car_media_manager.main
```

Open `http://localhost:8000` for the dashboard.

## Install as a systemd service (Raspberry Pi)

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
| `CMM_GOPRO_VOLUME_NAME` | GoPro USB volume name | `HERO13 BLACK` |
| `CMM_INSTA360_VOLUME_NAME` | Insta360 USB volume name | `Insta360 X4` |

## Project structure

```
car_media_manager/
    settings.py  — pydantic-settings config
    db.py        — SQLite schema + operations
    ingest.py    — Camera detection, file scanning, local copy
    mtp.py       — MTP device detection and FUSE mounting
    upload.py    — Internet check + S3 upload
    web.py       — FastAPI dashboard + API
    main.py      — Entry point, background loops
    templates/
        dashboard.html
```
