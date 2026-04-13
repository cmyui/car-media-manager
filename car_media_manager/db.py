from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import databases

SCHEMA = """\
CREATE TABLE IF NOT EXISTS media_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    local_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    uploaded_at TEXT,
    UNIQUE(source, original_filename, file_size)
);
"""

SQLITE_PRAGMAS = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA busy_timeout=5000",
    "PRAGMA foreign_keys=ON",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA cache_size=-20000",
)


@dataclass(frozen=True, slots=True)
class MediaFile:
    id: int
    source: str
    original_filename: str
    local_path: str
    file_size: int
    created_at: datetime
    ingested_at: datetime
    uploaded_at: datetime | None


def _parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _row_to_media_file(row: Any) -> MediaFile:
    return MediaFile(
        id=row["id"],
        source=row["source"],
        original_filename=row["original_filename"],
        local_path=row["local_path"],
        file_size=row["file_size"],
        created_at=datetime.fromisoformat(row["created_at"]),
        ingested_at=datetime.fromisoformat(row["ingested_at"]),
        uploaded_at=_parse_dt(row["uploaded_at"]),
    )


class Database:
    def __init__(self, db_path: Path) -> None:
        self._url = f"sqlite+aiosqlite:///{db_path}"
        self._database = databases.Database(self._url)

    async def connect(self) -> None:
        await self._database.connect()
        for pragma in SQLITE_PRAGMAS:
            await self._database.execute(pragma)
        await self._database.execute(SCHEMA)

    async def disconnect(self) -> None:
        await self._database.disconnect()

    async def is_ingested(
        self,
        *,
        source: str,
        original_filename: str,
        file_size: int,
    ) -> bool:
        row = await self._database.fetch_one(
            query=(
                "SELECT 1 FROM media_files "
                "WHERE source = :source "
                "AND original_filename = :original_filename "
                "AND file_size = :file_size"
            ),
            values={
                "source": source,
                "original_filename": original_filename,
                "file_size": file_size,
            },
        )
        return row is not None

    async def insert_media_file(
        self,
        *,
        source: str,
        original_filename: str,
        local_path: str,
        file_size: int,
        created_at: datetime,
    ) -> MediaFile:
        now = datetime.now(tz=timezone.utc)
        await self._database.execute(
            query=(
                "INSERT INTO media_files "
                "(source, original_filename, local_path, file_size, created_at, ingested_at) "
                "VALUES (:source, :original_filename, :local_path, :file_size, :created_at, :ingested_at)"
            ),
            values={
                "source": source,
                "original_filename": original_filename,
                "local_path": local_path,
                "file_size": file_size,
                "created_at": created_at.isoformat(),
                "ingested_at": now.isoformat(),
            },
        )
        row = await self._database.fetch_one(
            query=(
                "SELECT * FROM media_files "
                "WHERE source = :source "
                "AND original_filename = :original_filename "
                "AND file_size = :file_size"
            ),
            values={
                "source": source,
                "original_filename": original_filename,
                "file_size": file_size,
            },
        )
        assert row is not None
        return _row_to_media_file(row)

    async def list_pending_upload(self) -> list[MediaFile]:
        rows = await self._database.fetch_all(
            "SELECT * FROM media_files "
            "WHERE uploaded_at IS NULL "
            "ORDER BY ingested_at",
        )
        return [_row_to_media_file(r) for r in rows]

    async def mark_uploaded(self, file_id: int) -> None:
        await self._database.execute(
            query="UPDATE media_files SET uploaded_at = :uploaded_at WHERE id = :id",
            values={
                "uploaded_at": datetime.now(tz=timezone.utc).isoformat(),
                "id": file_id,
            },
        )

    async def get_stats(self) -> dict[str, int]:
        row = await self._database.fetch_one(
            "SELECT "
            "COUNT(*) AS total_files, "
            "SUM(CASE WHEN uploaded_at IS NULL THEN 1 ELSE 0 END) AS pending_files, "
            "COALESCE(SUM(file_size), 0) AS total_bytes, "
            "COALESCE(SUM(CASE WHEN uploaded_at IS NULL THEN file_size ELSE 0 END), 0) AS pending_bytes "
            "FROM media_files",
        )
        assert row is not None
        total_files = row["total_files"] or 0
        pending_files = row["pending_files"] or 0
        return {
            "total_files": total_files,
            "pending_files": pending_files,
            "uploaded_files": total_files - pending_files,
            "total_bytes": row["total_bytes"] or 0,
            "pending_bytes": row["pending_bytes"] or 0,
        }

    async def list_recent(self, *, limit: int = 50) -> list[MediaFile]:
        rows = await self._database.fetch_all(
            query="SELECT * FROM media_files ORDER BY ingested_at DESC LIMIT :limit",
            values={"limit": limit},
        )
        return [_row_to_media_file(r) for r in rows]
