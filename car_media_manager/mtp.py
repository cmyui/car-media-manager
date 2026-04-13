import asyncio
import logging
import platform
import re
from pathlib import Path

log = logging.getLogger(__name__)

MTP_MOUNT_ROOT = Path("/tmp/cmm-mtp")


async def _run_cmd(*args: str, timeout: float = 10) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise
    return proc.returncode or 0, stdout.decode(errors="replace"), stderr.decode(errors="replace")


async def _detect_usb_devices() -> list[dict[str, str]]:
    system = platform.system()
    if system == "Darwin":
        return await _detect_usb_devices_macos()
    if system == "Linux":
        return await _detect_usb_devices_linux()
    return []


async def _detect_usb_devices_macos() -> list[dict[str, str]]:
    try:
        _, stdout, _ = await _run_cmd(
            "system_profiler",
            "SPUSBDataType",
            "-detailLevel",
            "mini",
            timeout=10,
        )
    except (asyncio.TimeoutError, FileNotFoundError):
        return []

    devices: list[dict[str, str]] = []
    current_name = ""
    for line in stdout.splitlines():
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


async def _detect_usb_devices_linux() -> list[dict[str, str]]:
    try:
        _, stdout, _ = await _run_cmd("lsusb", timeout=5)
    except (asyncio.TimeoutError, FileNotFoundError):
        return []

    devices: list[dict[str, str]] = []
    for line in stdout.splitlines():
        parts = line.split(maxsplit=6)
        if len(parts) >= 7:
            devices.append({"name": parts[6]})
    return devices


async def detect_mtp_camera(pattern: re.Pattern[str]) -> bool:
    devices = await _detect_usb_devices()
    return any(pattern.search(d["name"]) for d in devices)


def _get_mtp_mount_tool() -> str:
    system = platform.system()
    if system == "Darwin":
        return "go-mtpfs"
    return "jmtpfs"


async def mount_mtp_device(mount_name: str) -> Path | None:
    mount_path = MTP_MOUNT_ROOT / mount_name
    mount_path.mkdir(parents=True, exist_ok=True)

    if any(mount_path.iterdir()):
        log.debug("MTP already mounted at %s", mount_path)
        return mount_path

    tool = _get_mtp_mount_tool()
    try:
        returncode, _, stderr = await _run_cmd(tool, str(mount_path), timeout=15)
    except FileNotFoundError:
        log.error(
            "%s not found. Install it:\n"
            "  macOS: brew install go-mtpfs\n"
            "  Linux: sudo apt install jmtpfs",
            tool,
        )
        return None
    except asyncio.TimeoutError:
        log.warning("MTP mount timed out")
        return None

    if returncode != 0:
        log.warning("MTP mount failed (%s): %s", tool, stderr.strip())
        return None

    log.info("MTP device mounted at %s", mount_path)
    return mount_path


async def unmount_mtp_device(mount_name: str) -> None:
    mount_path = MTP_MOUNT_ROOT / mount_name
    if not mount_path.exists():
        return

    system = platform.system()
    unmount_cmd: tuple[str, ...] = ("umount", str(mount_path))
    if system == "Darwin":
        unmount_cmd = ("diskutil", "unmount", str(mount_path))

    try:
        await _run_cmd(*unmount_cmd, timeout=10)
        log.info("Unmounted MTP device at %s", mount_path)
    except (asyncio.TimeoutError, FileNotFoundError):
        log.warning("Failed to unmount %s", mount_path)
