from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
from types import TracebackType
from typing import Self

from .domain import LedgerItem, Source, Status
from .scheduler import FailureKind, Snapshot


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
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS intake ("
                "source_id TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, "
                "mtime_ns INTEGER NOT NULL, stable_runs INTEGER NOT NULL, "
                "failure_kind TEXT, failure_since REAL, notified INTEGER NOT NULL DEFAULT 0, "
                "PRIMARY KEY (source_id, path))"
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
        self, run_id: str, source: Source, destination: str, staged: Path, pending: str
    ) -> None:
        self.db.execute(
            "INSERT INTO items VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                source.id,
                destination,
                str(staged),
                pending,
                Status.STAGING,
                None,
            ),
        )
        self.db.commit()

    def update(self, run_id: str, status: Status, error: str | None = None) -> None:
        self.db.execute(
            "UPDATE items SET status = ?, error = ? WHERE id = ?",
            (status, error, run_id),
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

    def failure_kind(self, source_id: str, path: str) -> FailureKind | None:
        row = self.db.execute(
            "SELECT failure_kind FROM intake WHERE source_id = ? AND path = ?",
            (source_id, path),
        ).fetchone()
        return FailureKind(row[0]) if row is not None and row[0] is not None else None

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
        self, source_id: str, path: str, kind: FailureKind, now: float
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


def list_items(path: Path) -> list[LedgerItem]:
    db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "SELECT source_id, destination, staged_path, pending_path, status, error "
            "FROM items ORDER BY rowid"
        ).fetchall()
    finally:
        db.close()
    return [
        LedgerItem(source_id, destination, staged, pending, Status(status), error)
        for source_id, destination, staged, pending, status, error in rows
    ]


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
