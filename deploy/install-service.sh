#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVICE_FILE="$SCRIPT_DIR/car-media-manager.service"
OSMO_MOUNT_TEMPLATE="$SCRIPT_DIR/media-pi-Osmo360.mount.in"
OSMO_UDEV_RULE="$SCRIPT_DIR/99-car-media-manager-osmo360.rules"
USER_SERVICE_DIR="$HOME/.config/systemd/user"
UV_BIN="${UV_BIN:-$HOME/.local/bin/uv}"
PATH_ENV="$HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

escape_sed_replacement() {
    printf "%s" "$1" | sed "s/[&|]/\\\\&/g"
}

repo_dir_escaped="$(escape_sed_replacement "$REPO_DIR")"
uv_bin_escaped="$(escape_sed_replacement "$UV_BIN")"
path_env_escaped="$(escape_sed_replacement "$PATH_ENV")"

install_osmo_mount() {
    local mount_unit_tmp

    mount_unit_tmp="$(mktemp)"
    sed \
        -e "s|@CMM_UID@|$(id -u)|" \
        -e "s|@CMM_GID@|$(id -g)|" \
        "$OSMO_MOUNT_TEMPLATE" > "$mount_unit_tmp"

    sudo mkdir -p /media/pi/Osmo360
    sudo install -m 0644 "$mount_unit_tmp" /etc/systemd/system/media-pi-Osmo360.mount
    sudo install -m 0644 "$OSMO_UDEV_RULE" /etc/udev/rules.d/99-car-media-manager-osmo360.rules
    rm -f "$mount_unit_tmp"

    sudo systemctl daemon-reload
    sudo udevadm control --reload-rules
    sudo udevadm trigger --subsystem-match=block --action=add

    if [ -e /dev/disk/by-label/Osmo360 ]; then
        sudo systemctl start media-pi-Osmo360.mount || true
    fi
}

install_osmo_mount

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
