#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_FILE="$SCRIPT_DIR/car-media-manager.service"
USER_SERVICE_DIR="$HOME/.config/systemd/user"

mkdir -p "$USER_SERVICE_DIR"
cp "$SERVICE_FILE" "$USER_SERVICE_DIR/car-media-manager.service"
systemctl --user daemon-reload
systemctl --user enable car-media-manager.service
systemctl --user restart car-media-manager.service

# Enable lingering so user services start at boot without login
sudo loginctl enable-linger "$USER"

echo "Installed. Status:"
systemctl --user status car-media-manager.service --no-pager
