#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_FILE="$SCRIPT_DIR/car-media-manager.service"
TARGET="/etc/systemd/system/car-media-manager.service"

sudo cp "$SERVICE_FILE" "$TARGET"
sudo systemctl daemon-reload
sudo systemctl enable car-media-manager.service
sudo systemctl restart car-media-manager.service

echo "Installed. Status:"
sudo systemctl status car-media-manager.service --no-pager
