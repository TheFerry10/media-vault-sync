from __future__ import annotations

import sqlite3
from pathlib import Path
from types import TracebackType
from typing import Self

from .domain import FailureKind, LedgerItem, Source, Status


class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)

    def __enter__(self) -> Self:
        self.db.__enter__()
        try:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, source_id TEXT, "
                "destination TEXT, staged_path TEXT, pending_path TEXT, status TEXT, error TEXT)"
            )
            columns = {
                row[1] for row in self.db.execute("PRAGMA table_info(items)").fetchall()
            }
            migrations = {
                "source_path": "TEXT NOT NULL DEFAULT ''",
                "failure_kind": "TEXT",
                "attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "next_retry_at": "TEXT",
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
        staged: Path,
        pending: str,
        attempt_count: int,
    ) -> None:
        self.db.execute(
            "INSERT INTO items (id, source_id, source_path, destination, staged_path, "
            "pending_path, status, error, failure_kind, attempt_count, next_retry_at) "
            "VALUES (?, ?, ?, '', ?, ?, ?, NULL, NULL, ?, NULL)",
            (
                run_id,
                source.id,
                source_path,
                str(staged),
                pending,
                Status.STAGING,
                attempt_count,
            ),
        )
        self.db.commit()

    def set_destination(self, run_id: str, destination: str) -> None:
        self.db.execute(
            "UPDATE items SET destination = ? WHERE id = ?", (destination, run_id)
        )
        self.db.commit()

    def update(
        self,
        run_id: str,
        status: Status,
        error: str | None = None,
        failure_kind: FailureKind | None = None,
        attempt_count: int = 0,
        next_retry_at: str | None = None,
    ) -> None:
        self.db.execute(
            "UPDATE items SET status = ?, error = ?, failure_kind = ?, "
            "attempt_count = ?, next_retry_at = ? WHERE id = ?",
            (status, error, failure_kind, attempt_count, next_retry_at, run_id),
        )
        self.db.commit()

    def latest(self, source_id: str, source_path: str) -> LedgerItem | None:
        row = self.db.execute(
            "SELECT source_id, source_path, destination, staged_path, pending_path, "
            "status, error, failure_kind, attempt_count, next_retry_at FROM items "
            "WHERE source_id = ? AND source_path = ? ORDER BY rowid DESC LIMIT 1",
            (source_id, source_path),
        ).fetchone()
        return _ledger_item(row) if row is not None else None


def list_items(path: Path) -> list[LedgerItem]:
    with Ledger(path) as ledger:
        rows = ledger.db.execute(
            "SELECT source_id, source_path, destination, staged_path, pending_path, "
            "status, error, failure_kind, attempt_count, next_retry_at "
            "FROM items ORDER BY rowid"
        ).fetchall()
    return [_ledger_item(row) for row in rows]


def list_failed_items(path: Path) -> list[LedgerItem]:
    with Ledger(path) as ledger:
        rows = ledger.db.execute(
            "SELECT source_id, source_path, destination, staged_path, pending_path, "
            "status, error, failure_kind, attempt_count, next_retry_at FROM items "
            "WHERE rowid IN (SELECT MAX(rowid) FROM items GROUP BY source_id, source_path) "
            "AND status = ? ORDER BY rowid",
            (Status.FAILED,),
        ).fetchall()
    return [_ledger_item(row) for row in rows]


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
    )
