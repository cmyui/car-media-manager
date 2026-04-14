"""Async wrapper around the `gio` CLI for MTP device access."""

import asyncio
import logging
import re
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

MTP_MOUNT_RE = re.compile(r"->\s+(mtp://\S+)")


async def _run_gio(*args: str, timeout: float = 30) -> tuple[int, str, str]:
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


@dataclass(frozen=True, slots=True)
class MtpMount:
    uri: str
    name: str


@dataclass(frozen=True, slots=True)
class GioFileInfo:
    name: str
    uri: str
    size: int


async def discover_mtp_mounts() -> list[MtpMount]:
    try:
        _, stdout, _ = await _run_gio("mount", "-l")
    except (asyncio.TimeoutError, FileNotFoundError):
        return []

    mounts: list[MtpMount] = []
    seen_uris: set[str] = set()
    lines = stdout.splitlines()

    for i, line in enumerate(lines):
        match = MTP_MOUNT_RE.search(line)
        if not match:
            continue
        uri = match.group(1).rstrip("/")
        if uri in seen_uris:
            continue
        seen_uris.add(uri)

        name = ""
        if i > 0:
            prev = lines[i - 1].strip()
            if prev.startswith("Mount("):
                name_part = prev.split(":", 1)
                if len(name_part) > 1:
                    name = name_part[1].strip().split(" -> ")[0].strip()

        mounts.append(MtpMount(uri=uri, name=name or uri))

    return mounts


async def list_files(uri: str) -> list[str]:
    try:
        returncode, stdout, stderr = await _run_gio("list", uri)
    except asyncio.TimeoutError:
        log.warning("gio list timed out for %s", uri)
        return []
    if returncode != 0:
        log.warning("gio list failed for %s: %s", uri, stderr.strip())
        return []
    return [line.strip() for line in stdout.splitlines() if line.strip()]


async def file_info(uri: str) -> GioFileInfo | None:
    try:
        returncode, stdout, _ = await _run_gio("info", uri)
    except asyncio.TimeoutError:
        return None
    if returncode != 0:
        return None

    name = ""
    size = 0
    for line in stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("standard::name:"):
            name = stripped.split(":", 2)[-1].strip()
        elif stripped.startswith("standard::size:"):
            try:
                size = int(stripped.split(":", 2)[-1].strip())
            except ValueError:
                pass

    if not name:
        return None
    return GioFileInfo(name=name, uri=uri, size=size)


async def copy_file(src_uri: str, dest_path: Path, *, timeout: float = 3600) -> bool:
    try:
        returncode, _, stderr = await _run_gio("copy", src_uri, str(dest_path), timeout=timeout)
    except asyncio.TimeoutError:
        log.warning("gio copy timed out for %s", src_uri)
        return False
    if returncode != 0:
        log.warning("gio copy failed for %s: %s", src_uri, stderr.strip())
        return False
    return True


async def find_media_root(base_uri: str, media_dir: str = "DCIM") -> str | None:
    entries = await list_files(base_uri)
    if media_dir in entries:
        return f"{base_uri}/{media_dir}"
    for entry in entries:
        sub_entries = await list_files(f"{base_uri}/{entry}")
        if media_dir in sub_entries:
            return f"{base_uri}/{entry}/{media_dir}"
    return None


async def scan_media_files(
    media_root_uri: str,
    extensions: frozenset[str],
) -> list[GioFileInfo]:
    files: list[GioFileInfo] = []
    dirs_to_scan = [media_root_uri]

    while dirs_to_scan:
        current = dirs_to_scan.pop(0)
        entries = await list_files(current)
        for entry in entries:
            if entry.startswith("."):
                continue
            entry_uri = f"{current}/{entry}"
            if "." in entry:
                suffix = "." + entry.rsplit(".", 1)[-1]
                if suffix.lower() in extensions:
                    info = await file_info(entry_uri)
                    if info:
                        files.append(info)
            else:
                dirs_to_scan.append(entry_uri)

    return sorted(files, key=lambda f: f.name)
