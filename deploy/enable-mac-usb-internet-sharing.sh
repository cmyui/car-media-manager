#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "This helper must be run on the Mac connected to the Raspberry Pi over USB." >&2
    exit 1
fi

PI_USB_SERVICE="${PI_USB_SERVICE:-Raspberry Pi USB Gadget}"
MAC_USB_IP="${MAC_USB_IP:-10.12.194.9}"
PI_USB_SUBNET="${PI_USB_SUBNET:-10.12.194.0/28}"
PI_USB_NETMASK="${PI_USB_NETMASK:-255.255.255.240}"
PF_ANCHOR="${PF_ANCHOR:-com.apple/car-media-manager-usb-nat}"

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "Missing required command: $1" >&2
        exit 1
    }
}

service_device() {
    local service="$1"
    networksetup -listallhardwareports | awk -v service="$service" '
        $0 == "Hardware Port: " service { found = 1; next }
        found && /^Device: / { print $2; exit }
    '
}

service_exists() {
    networksetup -listallnetworkservices | sed 's/^\*//' | grep -Fxq "$1"
}

require awk
require ifconfig
require networksetup
require pfctl
require route
require sysctl

if ! service_exists "$PI_USB_SERVICE"; then
    echo "Network service not found: $PI_USB_SERVICE" >&2
    echo "Connect the Raspberry Pi over USB-C and approve the macOS accessory prompt first." >&2
    exit 1
fi

pi_usb_device="$(service_device "$PI_USB_SERVICE")"
if [[ -z "$pi_usb_device" ]]; then
    echo "Could not find a device for network service: $PI_USB_SERVICE" >&2
    exit 1
fi

outbound_device="${OUTBOUND_DEVICE:-$(route -n get default 2>/dev/null | awk '/interface:/ { print $2; exit }')}"
if [[ -z "$outbound_device" ]]; then
    echo "Could not determine the current default-route interface." >&2
    exit 1
fi

if [[ "$outbound_device" == "$pi_usb_device" ]]; then
    echo "The default route is currently using $PI_USB_SERVICE ($pi_usb_device)." >&2
    echo "Move Wi-Fi or another internet service above the Pi USB service in macOS Network settings." >&2
    exit 1
fi

sudo networksetup -setmanual "$PI_USB_SERVICE" "$MAC_USB_IP" "$PI_USB_NETMASK"
sudo sysctl -w net.inet.ip.forwarding=1 >/dev/null

printf 'nat on %s from %s to any -> (%s)\n' \
    "$outbound_device" \
    "$PI_USB_SUBNET" \
    "$outbound_device" \
    | sudo pfctl -a "$PF_ANCHOR" -f -

sudo pfctl -e 2>/dev/null || true

echo "Enabled USB internet sharing for the Raspberry Pi."
echo "Mac USB address: $MAC_USB_IP on $pi_usb_device"
echo "NAT outbound interface: $outbound_device"
echo
echo "On the Pi, the default route should point at $MAC_USB_IP:"
echo "  ip route"
