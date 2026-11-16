from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Self

from .domain import (
    CaptureTimeSource,
    LedgerItem,
    Source,
    Status,
)
from .domain import FailureKind as PipelineFailureKind
from .scheduler import FailureKind as IntakeFailureKind
from .scheduler import Snapshot

_ITEM_COLUMNS = (
    "id",
    "source_id",
    "source_path",
    "destination",
    "staged_path",
    "pending_path",
    "status",
    "error",
    "failure_kind",
    "attempt_count",
    "next_retry_at",
    "content_hash",
    "capture_time",
    "capture_time_source",
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
                "content_hash BLOB, capture_time TEXT, capture_time_source TEXT)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS intake ("
                "source_id TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, "
                "mtime_ns INTEGER NOT NULL, stable_runs INTEGER NOT NULL, "
                "failure_kind TEXT, failure_since REAL, notified INTEGER NOT NULL DEFAULT 0, "
                "PRIMARY KEY (source_id, path))"
            )
            columns = {
                row[1] for row in self.db.execute("PRAGMA table_info(items)").fetchall()
            }
            migrations = {
                "source_path": "TEXT NOT NULL DEFAULT ''",
                "failure_kind": "TEXT",
                "attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "next_retry_at": "TEXT",
                "content_hash": "BLOB",
                "capture_time": "TEXT",
                "capture_time_source": "TEXT",
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
            "status, error, failure_kind, attempt_count, next_retry_at, content_hash, "
            "capture_time, capture_time_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, NULL, ?, ?)",
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

    def record_hash(self, run_id: str, content_hash: bytes) -> None:
        self.db.execute(
            "UPDATE items SET content_hash = ? WHERE id = ?",
            (content_hash, run_id),
        )
        self.db.commit()

    def update(
        self,
        run_id: str,
        status: Status,
        error: str | None = None,
        failure_kind: PipelineFailureKind | None = None,
        attempt_count: int = 0,
        next_retry_at: str | None = None,
    ) -> None:
        self.db.execute(
            "UPDATE items SET status = ?, error = ?, failure_kind = ?, "
            "attempt_count = ?, next_retry_at = ? WHERE id = ?",
            (status, error, failure_kind, attempt_count, next_retry_at, run_id),
        )
        self.db.commit()

    def observe(self, source_id: str, path: str, snapshot: Snapshot) -> bool:
        row = self.db.execute(
            "SELECT size, mtime_ns, stable_runs FROM intake "
            "WHERE source_id = ? AND path = ?",
            (source_id, path),
        ).fetchone()
        if row is not None and (row[0], row[1]) == (snapshot.size, snapshot.mtime_ns):
            stable_runs = int(row[2]) + 1
            self.db.execute(
                "UPDATE intake SET stable_runs = ? WHERE source_id = ? AND path = ?",
                (stable_runs, source_id, path),
            )
        else:
            stable_runs = 1
            self.db.execute(
                "INSERT INTO intake "
                "(source_id, path, size, mtime_ns, stable_runs, failure_kind, "
                "failure_since, notified) VALUES (?, ?, ?, ?, 1, NULL, NULL, 0) "
                "ON CONFLICT(source_id, path) DO UPDATE SET size = excluded.size, "
                "mtime_ns = excluded.mtime_ns, stable_runs = 1, failure_kind = NULL, "
                "failure_since = NULL, notified = 0",
                (source_id, path, snapshot.size, snapshot.mtime_ns),
            )
        self.db.commit()
        return stable_runs >= 2

    def paths_for_source(self, source_id: str) -> set[str]:
        rows = self.db.execute(
            "SELECT path FROM intake WHERE source_id = ?", (source_id,)
        ).fetchall()
        return {row[0] for row in rows}

    def failure_kind(self, source_id: str, path: str) -> IntakeFailureKind | None:
        row = self.db.execute(
            "SELECT failure_kind FROM intake WHERE source_id = ? AND path = ?",
            (source_id, path),
        ).fetchone()
        return (
            IntakeFailureKind(row[0])
            if row is not None and row[0] is not None
            else None
        )

    def reset_stability(self, source_id: str, path: str) -> None:
        self.db.execute(
            "UPDATE intake SET stable_runs = 0 WHERE source_id = ? AND path = ?",
            (source_id, path),
        )
        self.db.commit()

    def reset_stability_for_source(self, source_id: str) -> None:
        self.db.execute(
            "UPDATE intake SET stable_runs = 0 WHERE source_id = ?", (source_id,)
        )
        self.db.commit()

    def forget(self, source_id: str, path: str) -> None:
        self.db.execute(
            "DELETE FROM intake WHERE source_id = ? AND path = ?", (source_id, path)
        )
        self.db.commit()

    def record_failure(
        self, source_id: str, path: str, kind: IntakeFailureKind, now: float
    ) -> tuple[float, bool]:
        row = self.db.execute(
            "SELECT failure_kind, failure_since, notified FROM intake "
            "WHERE source_id = ? AND path = ?",
            (source_id, path),
        ).fetchone()
        if row is not None and row[0] == kind.value and row[1] is not None:
            failure_since, notified = row[1], bool(row[2])
        else:
            failure_since, notified = now, False
        self.db.execute(
            "UPDATE intake SET failure_kind = ?, failure_since = ?, notified = ? "
            "WHERE source_id = ? AND path = ?",
            (kind.value, failure_since, int(notified), source_id, path),
        )
        self.db.commit()
        return failure_since, notified

    def mark_notified(self, source_id: str, path: str) -> None:
        self.db.execute(
            "UPDATE intake SET notified = 1 WHERE source_id = ? AND path = ?",
            (source_id, path),
        )
        self.db.commit()

    def recoverable_items(self) -> list[LedgerItem]:
        rows = self.db.execute(
            f"SELECT {', '.join(_ITEM_COLUMNS)} FROM items ORDER BY rowid"
        ).fetchall()
        return [_ledger_item(row) for row in rows]

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

    def latest(self, source_id: str, source_path: str) -> LedgerItem | None:
        row = self.db.execute(
            f"SELECT {', '.join(_ITEM_COLUMNS)} FROM items "
            "WHERE source_id = ? AND source_path = ? ORDER BY rowid DESC LIMIT 1",
            (source_id, source_path),
        ).fetchone()
        return _ledger_item(row) if row is not None else None


def list_items(path: Path) -> list[LedgerItem]:
    db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        columns = {row[1] for row in db.execute("PRAGMA table_info(items)")}

        def expression(name: str) -> str:
            if name in columns:
                return name
            return {
                "id": "''",
                "source_id": "''",
                "source_path": "''",
                "destination": "''",
                "staged_path": "''",
                "pending_path": "''",
                "status": "'failed'",
                "attempt_count": "0",
                "content_hash": "NULL",
            }.get(name, "NULL")

        rows = db.execute(
            f"SELECT {', '.join(expression(name) for name in _ITEM_COLUMNS)} "
            "FROM items ORDER BY rowid"
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
        item_id,
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
        content_hash,
        capture_time,
        capture_time_source,
    ) = row
    return LedgerItem(
        str(item_id),
        str(source_id),
        str(source_path),
        str(destination),
        str(staged_path),
        str(pending_path),
        Status(str(status)),
        str(error) if error is not None else None,
        PipelineFailureKind(str(failure_kind)) if failure_kind is not None else None,
        int(str(attempt_count)),
        str(next_retry_at) if next_retry_at is not None else None,
        content_hash.hex()
        if isinstance(content_hash, bytes)
        else str(content_hash)
        if isinstance(content_hash, str)
        else None,
        str(capture_time) if capture_time is not None else None,
        CaptureTimeSource(str(capture_time_source))
        if capture_time_source is not None
        else None,
    )


def backup_ledger(path: Path, destination: Path) -> None:
    if path.resolve() == destination.resolve():
        raise ValueError("Ledger backup must use a separate path")
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    os.close(fd)
    try:
        with sqlite3.connect(path) as source, sqlite3.connect(temporary) as target:
            source.backup(target)
        os.replace(temporary, destination)
    except Exception:
        Path(temporary).unlink(missing_ok=True)
        raise
