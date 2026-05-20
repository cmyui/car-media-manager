#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVICE_FILE="$SCRIPT_DIR/car-media-manager.service"
USER_SERVICE_DIR="$HOME/.config/systemd/user"
UV_BIN="${UV_BIN:-$HOME/.local/bin/uv}"
PATH_ENV="$HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

escape_sed_replacement() {
    printf "%s" "$1" | sed "s/[&|]/\\\\&/g"
}

repo_dir_escaped="$(escape_sed_replacement "$REPO_DIR")"
uv_bin_escaped="$(escape_sed_replacement "$UV_BIN")"
path_env_escaped="$(escape_sed_replacement "$PATH_ENV")"

mkdir -p "$USER_SERVICE_DIR"
sed \
    -e "s|^WorkingDirectory=.*|WorkingDirectory=$repo_dir_escaped|" \
    -e "s|^Environment=PATH=.*|Environment=PATH=$path_env_escaped|" \
    -e "s|^ExecStart=.*|ExecStart=$uv_bin_escaped run python -m car_media_manager.main|" \
    "$SERVICE_FILE" > "$USER_SERVICE_DIR/car-media-manager.service"

systemctl --user daemon-reload
systemctl --user enable car-media-manager.service
systemctl --user restart car-media-manager.service

# Enable lingering so user services start at boot without login
sudo loginctl enable-linger "$USER"

echo "Installed. Status:"
systemctl --user status car-media-manager.service --no-pager
