from __future__ import annotations

import sqlite3
from pathlib import Path
from types import TracebackType
from typing import Self

from .domain import LedgerItem, Source, Status


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
