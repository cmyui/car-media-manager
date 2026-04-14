#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_FILE="$SCRIPT_DIR/car-media-manager.service"
USER_SERVICE_DIR="$HOME/.config/systemd/user"

# Remove old system service if it exists
if systemctl is-active car-media-manager.service &>/dev/null; then
    echo "Removing old system service..."
    sudo systemctl stop car-media-manager.service
    sudo systemctl disable car-media-manager.service
    sudo rm -f /etc/systemd/system/car-media-manager.service
    sudo systemctl daemon-reload
fi

# Install as user service
mkdir -p "$USER_SERVICE_DIR"
cp "$SERVICE_FILE" "$USER_SERVICE_DIR/car-media-manager.service"
systemctl --user daemon-reload
systemctl --user enable car-media-manager.service
systemctl --user restart car-media-manager.service

# Enable lingering so user services start at boot without login
sudo loginctl enable-linger "$USER"

echo "Installed as user service. Status:"
systemctl --user status car-media-manager.service --no-pager
