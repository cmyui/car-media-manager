import logging
import platform
import re
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

MTP_MOUNT_ROOT = Path("/tmp/cmm-mtp")


def _detect_usb_devices() -> list[dict[str, str]]:
    system = platform.system()
    if system == "Darwin":
        return _detect_usb_devices_macos()
    if system == "Linux":
        return _detect_usb_devices_linux()
    return []


def _detect_usb_devices_macos() -> list[dict[str, str]]:
    try:
        result = subprocess.run(
            ["system_profiler", "SPUSBDataType", "-detailLevel", "mini"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        devices: list[dict[str, str]] = []
        current_name = ""
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if stripped.endswith(":") and not stripped.startswith("USB"):
                current_name = stripped.rstrip(":")
            if "Serial Number" in stripped and current_name:
                devices.append({"name": current_name})
                current_name = ""
            elif "Product ID" in stripped and current_name:
                devices.append({"name": current_name})
                current_name = ""
        return devices
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def _detect_usb_devices_linux() -> list[dict[str, str]]:
    try:
        result = subprocess.run(
            ["lsusb"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        devices: list[dict[str, str]] = []
        for line in result.stdout.splitlines():
            parts = line.split(maxsplit=6)
            if len(parts) >= 7:
                devices.append({"name": parts[6]})
        return devices
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def detect_mtp_camera(pattern: re.Pattern[str]) -> bool:
    devices = _detect_usb_devices()
    return any(pattern.search(d["name"]) for d in devices)


def _get_mtp_mount_tool() -> str:
    system = platform.system()
    if system == "Darwin":
        return "go-mtpfs"
    return "jmtpfs"


def mount_mtp_device(mount_name: str) -> Path | None:
    mount_path = MTP_MOUNT_ROOT / mount_name
    mount_path.mkdir(parents=True, exist_ok=True)

    if any(mount_path.iterdir()):
        log.debug("MTP already mounted at %s", mount_path)
        return mount_path

    tool = _get_mtp_mount_tool()
    try:
        result = subprocess.run(
            [tool, str(mount_path)],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            log.warning("MTP mount failed (%s): %s", tool, result.stderr.strip())
            return None
        log.info("MTP device mounted at %s", mount_path)
        return mount_path
    except FileNotFoundError:
        log.error(
            "%s not found. Install it:\n"
            "  macOS: brew install go-mtpfs\n"
            "  Linux: sudo apt install jmtpfs",
            tool,
        )
        return None
    except subprocess.TimeoutExpired:
        log.warning("MTP mount timed out")
        return None


def unmount_mtp_device(mount_name: str) -> None:
    mount_path = MTP_MOUNT_ROOT / mount_name
    if not mount_path.exists():
        return

    system = platform.system()
    unmount_cmd = ["umount", str(mount_path)]
    if system == "Darwin":
        unmount_cmd = ["diskutil", "unmount", str(mount_path)]

    try:
        subprocess.run(unmount_cmd, capture_output=True, timeout=10)
        log.info("Unmounted MTP device at %s", mount_path)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        log.warning("Failed to unmount %s", mount_path)
