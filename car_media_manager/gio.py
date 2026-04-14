"""MTP mount discovery and recovery via gvfs."""

import asyncio
import logging
import signal
from pathlib import Path

log = logging.getLogger(__name__)

GVFS_ROOT = Path("/run/user/1000/gvfs")


def discover_mtp_mounts() -> list[Path]:
    if not GVFS_ROOT.is_dir():
        return []
    return [p for p in GVFS_ROOT.iterdir() if p.is_dir() and p.name.startswith("mtp:")]


def mtp_path_to_uri(mount_path: Path) -> str:
    name = mount_path.name
    host = name.removeprefix("mtp:host=")
    return f"mtp://{host}/"


async def _run_gio(*args: str, timeout: float = 15) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        "gio",
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
    return (
        proc.returncode or 0,
        stdout.decode(errors="replace"),
        stderr.decode(errors="replace"),
    )


async def reset_mtp_session(mount_path: Path) -> bool:
    uri = mtp_path_to_uri(mount_path)
    log.warning("Resetting stale MTP session for %s", uri)

    for proc_dir in Path("/proc").iterdir():
        try:
            cmdline = (proc_dir / "cmdline").read_bytes().decode(errors="replace")
            if "gvfsd-mtp" in cmdline:
                pid = int(proc_dir.name)
                log.info("Killing gvfsd-mtp (PID %d)", pid)
                proc_dir.name  # validate it's numeric
                import os
                os.kill(pid, signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError, FileNotFoundError):
            continue

    await asyncio.sleep(2)

    try:
        returncode, _, stderr = await _run_gio("mount", uri, timeout=15)
        if returncode != 0:
            log.error("Failed to remount %s: %s", uri, stderr.strip())
            return False
        log.info("Remounted %s", uri)
        return True
    except asyncio.TimeoutError:
        log.error("Timed out remounting %s", uri)
        return False
