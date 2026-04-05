import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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


def _row_to_media_file(row: sqlite3.Row) -> MediaFile:
    return MediaFile(
        id=row["id"],
        source=row["source"],
        original_filename=row["original_filename"],
        local_path=row["local_path"],
        file_size=row["file_size"],
        created_at=datetime.fromisoformat(row["created_at"]),
        ingested_at=datetime.fromisoformat(row["ingested_at"]),
        uploaded_at=(
            datetime.fromisoformat(row["uploaded_at"])
            if row["uploaded_at"]
            else None
        ),
    )


class Database:
    def __init__(self, db_path: Path) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def is_ingested(
        self,
        *,
        source: str,
        original_filename: str,
        file_size: int,
    ) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM media_files "
            "WHERE source = ? AND original_filename = ? AND file_size = ?",
            (source, original_filename, file_size),
        ).fetchone()
        return row is not None

    def insert_media_file(
        self,
        *,
        source: str,
        original_filename: str,
        local_path: str,
        file_size: int,
        created_at: datetime,
    ) -> MediaFile:
        now = datetime.now().isoformat()
        cursor = self._conn.execute(
            "INSERT INTO media_files "
            "(source, original_filename, local_path, file_size, created_at, ingested_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, original_filename, local_path, file_size, created_at.isoformat(), now),
        )
        self._conn.commit()
        row = self._conn.execute(
            "SELECT * FROM media_files WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
        assert row is not None
        return _row_to_media_file(row)

    def list_pending_upload(self) -> list[MediaFile]:
        rows = self._conn.execute(
            "SELECT * FROM media_files WHERE uploaded_at IS NULL ORDER BY ingested_at",
        ).fetchall()
        return [_row_to_media_file(r) for r in rows]

    def mark_uploaded(self, file_id: int) -> None:
        self._conn.execute(
            "UPDATE media_files SET uploaded_at = ? WHERE id = ?",
            (datetime.now().isoformat(), file_id),
        )
        self._conn.commit()

    def get_stats(self) -> dict[str, int]:
        total = self._conn.execute("SELECT COUNT(*) FROM media_files").fetchone()[0]
        pending = self._conn.execute(
            "SELECT COUNT(*) FROM media_files WHERE uploaded_at IS NULL",
        ).fetchone()[0]
        uploaded = total - pending
        total_bytes = self._conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM media_files",
        ).fetchone()[0]
        pending_bytes = self._conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM media_files WHERE uploaded_at IS NULL",
        ).fetchone()[0]
        return {
            "total_files": total,
            "pending_files": pending,
            "uploaded_files": uploaded,
            "total_bytes": total_bytes,
            "pending_bytes": pending_bytes,
        }

    def list_recent(self, *, limit: int = 50) -> list[MediaFile]:
        rows = self._conn.execute(
            "SELECT * FROM media_files ORDER BY ingested_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_media_file(r) for r in rows]
