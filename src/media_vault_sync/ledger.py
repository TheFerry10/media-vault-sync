from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Self

from .domain import (
    CaptureTimeSource,
    FailureKind,
    LedgerItem,
    Source,
    Status,
)


class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)

    def __enter__(self) -> Self:
        self.db.__enter__()
        try:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, source_id TEXT, "
                "source_path TEXT NOT NULL DEFAULT '', destination TEXT, staged_path TEXT, "
                "pending_path TEXT, status TEXT, error TEXT, failure_kind TEXT, "
                "attempt_count INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT, "
                "capture_time TEXT, capture_time_source TEXT, content_hash BLOB)"
            )
            columns = {
                row[1] for row in self.db.execute("PRAGMA table_info(items)").fetchall()
            }
            migrations = {
                "source_path": "TEXT NOT NULL DEFAULT ''",
                "failure_kind": "TEXT",
                "attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "next_retry_at": "TEXT",
                "capture_time": "TEXT",
                "capture_time_source": "TEXT",
                "content_hash": "BLOB",
            }
            for name, declaration in migrations.items():
                if name not in columns:
                    self.db.execute(
                        f"ALTER TABLE items ADD COLUMN {name} {declaration}"
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
        source_path: str,
        destination: str,
        staged: Path,
        pending: str,
        capture_time: datetime,
        capture_time_source: CaptureTimeSource,
        attempt_count: int,
    ) -> None:
        self.db.execute(
            "INSERT INTO items "
            "(id, source_id, source_path, destination, staged_path, pending_path, "
            "status, error, failure_kind, attempt_count, next_retry_at, capture_time, "
            "capture_time_source, content_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, ?, ?, NULL)",
            (
                run_id,
                source.id,
                source_path,
                destination,
                str(staged),
                pending,
                Status.STAGING,
                attempt_count,
                capture_time.isoformat(),
                capture_time_source,
            ),
        )
        self.db.commit()

    def update(
        self,
        run_id: str,
        status: Status,
        error: str | None = None,
        failure_kind: FailureKind | None = None,
        attempt_count: int | None = None,
        next_retry_at: str | None = None,
    ) -> None:
        if attempt_count is None:
            self.db.execute(
                "UPDATE items SET status = ?, error = ? WHERE id = ?",
                (status, error, run_id),
            )
        else:
            self.db.execute(
                "UPDATE items SET status = ?, error = ?, failure_kind = ?, "
                "attempt_count = ?, next_retry_at = ? WHERE id = ?",
                (status, error, failure_kind, attempt_count, next_retry_at, run_id),
            )
        self.db.commit()

    def set_content_hash(self, run_id: str, content_hash: bytes) -> None:
        self.db.execute(
            "UPDATE items SET content_hash = ? WHERE id = ?", (content_hash, run_id)
        )
        self.db.commit()

    def set_destination(self, run_id: str, destination: str) -> None:
        self.db.execute(
            "UPDATE items SET destination = ? WHERE id = ?", (destination, run_id)
        )
        self.db.commit()

    def find_published(self, source_id: str, content_hash: bytes) -> str | None:
        row = self.db.execute(
            "SELECT destination FROM items "
            "WHERE source_id = ? AND content_hash = ? AND status = ? "
            "ORDER BY rowid LIMIT 1",
            (source_id, content_hash, Status.PUBLISHED),
        ).fetchone()
        return None if row is None else str(row[0])

    def latest(self, source_id: str, source_path: str) -> LedgerItem | None:
        row = self.db.execute(
            "SELECT source_id, source_path, destination, staged_path, pending_path, "
            "status, error, failure_kind, attempt_count, next_retry_at, capture_time, "
            "capture_time_source FROM items "
            "WHERE source_id = ? AND source_path = ? ORDER BY rowid DESC LIMIT 1",
            (source_id, source_path),
        ).fetchone()
        return _ledger_item(row) if row is not None else None


def list_items(path: Path) -> list[LedgerItem]:
    db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        columns = {row[1] for row in db.execute("PRAGMA table_info(items)")}

        def column(name: str, fallback: str) -> str:
            return name if name in columns else fallback

        selected_columns = (
            column("source_id", "''"),
            column("source_path", "''"),
            column("destination", "''"),
            column("staged_path", "''"),
            column("pending_path", "''"),
            column("status", "'failed'"),
            column("error", "NULL"),
            column("failure_kind", "NULL"),
            column("attempt_count", "0"),
            column("next_retry_at", "NULL"),
            column("capture_time", "NULL"),
            column("capture_time_source", "NULL"),
        )
        rows = db.execute(
            f"SELECT {', '.join(selected_columns)} FROM items ORDER BY rowid"
        ).fetchall()
    finally:
        db.close()
    return [_ledger_item(row) for row in rows]


def list_failed_items(path: Path) -> list[LedgerItem]:
    latest: dict[tuple[str, str], LedgerItem] = {}
    for item in list_items(path):
        source_path = item.source_path or item.destination
        latest[(item.source_id, source_path)] = item
    return [item for item in latest.values() if item.status == Status.FAILED]


def _ledger_item(row: tuple[object, ...]) -> LedgerItem:
    (
        source_id,
        source_path,
        destination,
        staged_path,
        pending_path,
        status,
        error,
        failure_kind,
        attempt_count,
        next_retry_at,
        capture_time,
        capture_time_source,
    ) = row
    return LedgerItem(
        str(source_id),
        str(source_path),
        str(destination),
        str(staged_path),
        str(pending_path),
        Status(str(status)),
        str(error) if error is not None else None,
        FailureKind(str(failure_kind)) if failure_kind is not None else None,
        int(str(attempt_count)),
        str(next_retry_at) if next_retry_at is not None else None,
        str(capture_time) if capture_time is not None else None,
        CaptureTimeSource(str(capture_time_source))
        if capture_time_source is not None
        else None,
    )
