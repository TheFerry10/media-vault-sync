from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Self

from .domain import CaptureTimeSource, LedgerItem, Source, Status


class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)

    def __enter__(self) -> Self:
        self.db.__enter__()
        try:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, source_id TEXT, "
                "destination TEXT, staged_path TEXT, pending_path TEXT, status TEXT, error TEXT, "
                "content_hash BLOB, capture_time TEXT, capture_time_source TEXT)"
            )
            columns = {row[1] for row in self.db.execute("PRAGMA table_info(items)")}
            for name, column_type in (
                ("content_hash", "BLOB"),
                ("capture_time", "TEXT"),
                ("capture_time_source", "TEXT"),
            ):
                if name not in columns:
                    self.db.execute(
                        f"ALTER TABLE items ADD COLUMN {name} {column_type}"
                    )
        except sqlite3.Error:
            self.db.close()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self.db.__exit__(exc_type, exc, traceback)
        finally:
            self.db.close()

    def start(
        self,
        run_id: str,
        source: Source,
        destination: str,
        staged: Path,
        pending: str,
        capture_time: datetime,
        capture_time_source: CaptureTimeSource,
    ) -> None:
        self.db.execute(
            "INSERT INTO items "
            "(id, source_id, destination, staged_path, pending_path, status, error, "
            "capture_time, capture_time_source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                source.id,
                destination,
                str(staged),
                pending,
                Status.STAGING,
                None,
                capture_time.isoformat(),
                capture_time_source,
            ),
        )
        self.db.commit()

    def record_hash(self, run_id: str, content_hash: bytes) -> None:
        self.db.execute(
            "UPDATE items SET content_hash = ? WHERE id = ?",
            (content_hash, run_id),
        )
        self.db.commit()

    def update(self, run_id: str, status: Status, error: str | None = None) -> None:
        self.db.execute(
            "UPDATE items SET status = ?, error = ? WHERE id = ?",
            (status, error, run_id),
        )
        self.db.commit()

    def recoverable_items(self) -> list[LedgerItem]:
        rows = self.db.execute(
            "SELECT id, source_id, destination, staged_path, pending_path, status, error, "
            "content_hash, capture_time, capture_time_source FROM items ORDER BY rowid"
        ).fetchall()
        return [
            LedgerItem(
                run_id,
                source_id,
                destination,
                staged,
                pending,
                Status(status),
                error,
                content_hash,
                capture_time,
                CaptureTimeSource(capture_time_source)
                if capture_time_source is not None
                else None,
            )
            for (
                run_id,
                source_id,
                destination,
                staged,
                pending,
                status,
                error,
                content_hash,
                capture_time,
                capture_time_source,
            ) in rows
        ]

    def set_destination(self, run_id: str, destination: str) -> None:
        self.db.execute(
            "UPDATE items SET destination = ? WHERE id = ?", (destination, run_id)
        )
        self.db.commit()

    def find_published(self, source_id: str, content_hash: bytes) -> str | None:
        row = self.db.execute(
            "SELECT destination FROM items "
            "WHERE source_id = ? AND content_hash IN (?, ?) AND status = ? "
            "ORDER BY rowid LIMIT 1",
            (source_id, content_hash, content_hash.hex(), Status.PUBLISHED),
        ).fetchone()
        return None if row is None else str(row[0])


def list_items(path: Path) -> list[LedgerItem]:
    db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        columns = {row[1] for row in db.execute("PRAGMA table_info(items)")}
        item_id = "id" if "id" in columns else "''"
        content_hash = "content_hash" if "content_hash" in columns else "NULL"
        capture_time = "capture_time" if "capture_time" in columns else "NULL"
        capture_time_source = (
            "capture_time_source" if "capture_time_source" in columns else "NULL"
        )
        rows = db.execute(
            "SELECT "
            f"{item_id}, source_id, destination, staged_path, pending_path, status, error, "
            f"{content_hash}, {capture_time}, {capture_time_source} "
            "FROM items ORDER BY rowid"
        ).fetchall()
    finally:
        db.close()
    return [
        LedgerItem(
            run_id,
            source_id,
            destination,
            staged,
            pending,
            Status(status),
            error,
            content_hash,
            capture_time,
            CaptureTimeSource(capture_time_source)
            if capture_time_source is not None
            else None,
        )
        for (
            run_id,
            source_id,
            destination,
            staged,
            pending,
            status,
            error,
            content_hash,
            capture_time,
            capture_time_source,
        ) in rows
    ]


def backup_ledger(source: Path, destination: Path) -> None:
    source_db = sqlite3.connect(source)
    destination_db = sqlite3.connect(destination)
    try:
        source_db.backup(destination_db)
    finally:
        destination_db.close()
        source_db.close()
