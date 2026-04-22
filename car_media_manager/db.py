from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import databases

SCHEMA_MEDIA_FILES = """\
CREATE TABLE IF NOT EXISTS media_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    local_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    ingested_at TEXT,
    uploaded_at TEXT,
    UNIQUE(vendor, original_filename, file_size)
)
"""

SCHEMA_MULTIPART_UPLOADS = """\
CREATE TABLE IF NOT EXISTS multipart_uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_file_id INTEGER NOT NULL UNIQUE,
    s3_bucket TEXT NOT NULL,
    s3_key TEXT NOT NULL,
    s3_upload_id TEXT NOT NULL,
    part_size INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    FOREIGN KEY(media_file_id) REFERENCES media_files(id) ON DELETE CASCADE
)
"""

SCHEMA_MULTIPART_PARTS = """\
CREATE TABLE IF NOT EXISTS multipart_parts (
    multipart_upload_id INTEGER NOT NULL,
    part_number INTEGER NOT NULL,
    etag TEXT NOT NULL,
    size INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL,
    PRIMARY KEY (multipart_upload_id, part_number),
    FOREIGN KEY(multipart_upload_id) REFERENCES multipart_uploads(id) ON DELETE CASCADE
)
"""

SCHEMA_STATEMENTS = (
    SCHEMA_MEDIA_FILES,
    SCHEMA_MULTIPART_UPLOADS,
    SCHEMA_MULTIPART_PARTS,
)

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
    vendor: str
    original_filename: str
    local_path: str
    file_size: int
    created_at: datetime
    ingested_at: datetime | None
    uploaded_at: datetime | None


@dataclass(frozen=True, slots=True)
class MultipartUpload:
    id: int
    media_file_id: int
    s3_bucket: str
    s3_key: str
    s3_upload_id: str
    part_size: int
    started_at: datetime


@dataclass(frozen=True, slots=True)
class MultipartPart:
    part_number: int
    etag: str
    size: int


def _parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _row_to_media_file(row: Any) -> MediaFile:
    return MediaFile(
        id=row["id"],
        vendor=row["vendor"],
        original_filename=row["original_filename"],
        local_path=row["local_path"],
        file_size=row["file_size"],
        created_at=datetime.fromisoformat(row["created_at"]),
        ingested_at=_parse_dt(row["ingested_at"]),
        uploaded_at=_parse_dt(row["uploaded_at"]),
    )


def _row_to_multipart_upload(row: Any) -> MultipartUpload:
    return MultipartUpload(
        id=row["id"],
        media_file_id=row["media_file_id"],
        s3_bucket=row["s3_bucket"],
        s3_key=row["s3_key"],
        s3_upload_id=row["s3_upload_id"],
        part_size=row["part_size"],
        started_at=datetime.fromisoformat(row["started_at"]),
    )


class Database:
    def __init__(self, db_path: Path) -> None:
        self._url = f"sqlite+aiosqlite:///{db_path}"
        self._database = databases.Database(self._url)

    async def connect(self) -> None:
        await self._database.connect()
        for pragma in SQLITE_PRAGMAS:
            await self._database.execute(pragma)
        for statement in SCHEMA_STATEMENTS:
            await self._database.execute(statement)
        await self._migrate_source_to_vendor()

    async def _migrate_source_to_vendor(self) -> None:
        cols = await self._database.fetch_all("PRAGMA table_info(media_files)")
        col_names = {c["name"] for c in cols}
        if "source" in col_names and "vendor" not in col_names:
            await self._database.execute(
                "ALTER TABLE media_files RENAME COLUMN source TO vendor"
            )

    async def disconnect(self) -> None:
        await self._database.disconnect()

    async def is_ingested(
        self,
        *,
        vendor: str,
        original_filename: str,
        file_size: int,
    ) -> bool:
        row = await self._database.fetch_one(
            query=(
                "SELECT 1 FROM media_files "
                "WHERE vendor = :vendor "
                "AND original_filename = :original_filename "
                "AND file_size = :file_size"
            ),
            values={
                "vendor": vendor,
                "original_filename": original_filename,
                "file_size": file_size,
            },
        )
        return row is not None

    async def insert_media_file(
        self,
        *,
        vendor: str,
        original_filename: str,
        local_path: str,
        file_size: int,
        created_at: datetime,
    ) -> MediaFile:
        await self._database.execute(
            query=(
                "INSERT INTO media_files "
                "(vendor, original_filename, local_path, file_size, created_at) "
                "VALUES (:vendor, :original_filename, :local_path, :file_size, :created_at)"
            ),
            values={
                "vendor": vendor,
                "original_filename": original_filename,
                "local_path": local_path,
                "file_size": file_size,
                "created_at": created_at.isoformat(),
            },
        )
        row = await self._database.fetch_one(
            query=(
                "SELECT * FROM media_files "
                "WHERE vendor = :vendor "
                "AND original_filename = :original_filename "
                "AND file_size = :file_size"
            ),
            values={
                "vendor": vendor,
                "original_filename": original_filename,
                "file_size": file_size,
            },
        )
        assert row is not None
        return _row_to_media_file(row)

    async def mark_ingested(self, file_id: int) -> None:
        await self._database.execute(
            query="UPDATE media_files SET ingested_at = :ingested_at WHERE id = :id",
            values={
                "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
                "id": file_id,
            },
        )

    async def list_pending_upload(self) -> list[MediaFile]:
        rows = await self._database.fetch_all(
            "SELECT * FROM media_files "
            "WHERE ingested_at IS NOT NULL AND uploaded_at IS NULL "
            "ORDER BY ingested_at",
        )
        return [_row_to_media_file(r) for r in rows]

    async def list_active_copies(self) -> list[MediaFile]:
        rows = await self._database.fetch_all(
            "SELECT * FROM media_files WHERE ingested_at IS NULL",
        )
        return [_row_to_media_file(r) for r in rows]

    async def list_incomplete_copies(self) -> list[MediaFile]:
        rows = await self._database.fetch_all(
            "SELECT * FROM media_files WHERE ingested_at IS NULL",
        )
        return [_row_to_media_file(r) for r in rows]

    async def delete_media_file(self, file_id: int) -> None:
        await self._database.execute(
            query="DELETE FROM media_files WHERE id = :id",
            values={"id": file_id},
        )

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

    async def get_multipart_upload(
        self,
        media_file_id: int,
    ) -> MultipartUpload | None:
        row = await self._database.fetch_one(
            query="SELECT * FROM multipart_uploads WHERE media_file_id = :media_file_id",
            values={"media_file_id": media_file_id},
        )
        if row is None:
            return None
        return _row_to_multipart_upload(row)

    async def create_multipart_upload(
        self,
        *,
        media_file_id: int,
        s3_bucket: str,
        s3_key: str,
        s3_upload_id: str,
        part_size: int,
    ) -> MultipartUpload:
        now = datetime.now(tz=timezone.utc)
        await self._database.execute(
            query=(
                "INSERT INTO multipart_uploads "
                "(media_file_id, s3_bucket, s3_key, s3_upload_id, part_size, started_at) "
                "VALUES (:media_file_id, :s3_bucket, :s3_key, :s3_upload_id, :part_size, :started_at)"
            ),
            values={
                "media_file_id": media_file_id,
                "s3_bucket": s3_bucket,
                "s3_key": s3_key,
                "s3_upload_id": s3_upload_id,
                "part_size": part_size,
                "started_at": now.isoformat(),
            },
        )
        row = await self._database.fetch_one(
            query="SELECT * FROM multipart_uploads WHERE media_file_id = :media_file_id",
            values={"media_file_id": media_file_id},
        )
        assert row is not None
        return _row_to_multipart_upload(row)

    async def delete_multipart_upload(self, multipart_upload_id: int) -> None:
        await self._database.execute(
            query="DELETE FROM multipart_uploads WHERE id = :id",
            values={"id": multipart_upload_id},
        )

    async def record_part_uploaded(
        self,
        *,
        multipart_upload_id: int,
        part_number: int,
        etag: str,
        size: int,
    ) -> None:
        await self._database.execute(
            query=(
                "INSERT OR REPLACE INTO multipart_parts "
                "(multipart_upload_id, part_number, etag, size, uploaded_at) "
                "VALUES (:multipart_upload_id, :part_number, :etag, :size, :uploaded_at)"
            ),
            values={
                "multipart_upload_id": multipart_upload_id,
                "part_number": part_number,
                "etag": etag,
                "size": size,
                "uploaded_at": datetime.now(tz=timezone.utc).isoformat(),
            },
        )

    async def replace_parts(
        self,
        multipart_upload_id: int,
        parts: list[MultipartPart],
    ) -> None:
        await self._database.execute(
            query="DELETE FROM multipart_parts WHERE multipart_upload_id = :id",
            values={"id": multipart_upload_id},
        )
        if not parts:
            return
        now = datetime.now(tz=timezone.utc).isoformat()
        await self._database.execute_many(
            query=(
                "INSERT INTO multipart_parts "
                "(multipart_upload_id, part_number, etag, size, uploaded_at) "
                "VALUES (:multipart_upload_id, :part_number, :etag, :size, :uploaded_at)"
            ),
            values=[
                {
                    "multipart_upload_id": multipart_upload_id,
                    "part_number": p.part_number,
                    "etag": p.etag,
                    "size": p.size,
                    "uploaded_at": now,
                }
                for p in parts
            ],
        )

    async def list_completed_parts(
        self,
        multipart_upload_id: int,
    ) -> list[MultipartPart]:
        rows = await self._database.fetch_all(
            query=(
                "SELECT part_number, etag, size FROM multipart_parts "
                "WHERE multipart_upload_id = :id "
                "ORDER BY part_number"
            ),
            values={"id": multipart_upload_id},
        )
        return [
            MultipartPart(
                part_number=r["part_number"],
                etag=r["etag"],
                size=r["size"],
            )
            for r in rows
        ]

    async def list_active_multipart_progress(self) -> list[dict[str, Any]]:
        rows = await self._database.fetch_all(
            query=(
                "SELECT "
                "    m.id AS media_file_id, "
                "    m.vendor AS vendor, "
                "    m.original_filename AS original_filename, "
                "    m.file_size AS file_size, "
                "    COALESCE(SUM(p.size), 0) AS bytes_uploaded "
                "FROM multipart_uploads u "
                "JOIN media_files m ON m.id = u.media_file_id "
                "LEFT JOIN multipart_parts p ON p.multipart_upload_id = u.id "
                "GROUP BY u.id "
                "ORDER BY u.started_at"
            ),
        )
        result: list[dict[str, Any]] = []
        for r in rows:
            total = r["file_size"]
            done = r["bytes_uploaded"] or 0
            result.append(
                {
                    "media_file_id": r["media_file_id"],
                    "vendor": r["vendor"],
                    "original_filename": r["original_filename"],
                    "file_size": total,
                    "bytes_uploaded": done,
                    "percent": (done / total * 100) if total else 0.0,
                }
            )
        return result
