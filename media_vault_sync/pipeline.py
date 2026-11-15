from __future__ import annotations

import hashlib
import shutil
import sqlite3
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

MEDIA_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".heif", ".tif", ".tiff",
    ".dng", ".cr2", ".cr3", ".nef", ".arw", ".raf", ".orf", ".rw2",
    ".mp4", ".mov", ".m4v", ".avi", ".mkv", ".3gp", ".mts", ".m2ts",
})


class Cloud(Protocol):
    def upload(self, path: Path, pending: str) -> None: ...

    def read_pending(self, pending: str) -> Iterator[bytes]: ...

    def publish(self, pending: str, destination: str) -> None: ...


@dataclass(frozen=True)
class Source:
    id: str
    inbox: Path


@dataclass(frozen=True)
class Result:
    status: str
    destination: str


@dataclass(frozen=True)
class LedgerItem:
    source_id: str
    destination: str
    staged_path: str
    pending_path: str
    status: str
    error: str | None


def list_items(ledger: Path) -> list[LedgerItem]:
    with sqlite3.connect(f"{ledger.resolve().as_uri()}?mode=ro", uri=True) as db:
        rows = db.execute(
            "SELECT source_id, destination, staged_path, pending_path, status, error "
            "FROM items ORDER BY rowid"
        ).fetchall()
    return [LedgerItem(*row) for row in rows]


def file_hash(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


def run_once(
    source: Source, item: Path, staging: Path, ledger: Path, cloud: Cloud
) -> Result:
    if not source.id.strip():
        raise ValueError("Source ID must not be empty")
    inbox = source.inbox.resolve(strict=True)
    if not inbox.is_dir() or not item.resolve(strict=True).is_relative_to(inbox):
        raise ValueError("Item must be inside the Source Inbox")
    if item.is_symlink() or not item.is_file():
        raise ValueError("Item must be a regular file, not a symlink")
    if item.suffix.lower() not in MEDIA_EXTENSIONS:
        raise ValueError("Unsupported media extension")
    if any(ord(c) < 32 or c in '"*:<>?\\|' for c in item.name):
        raise ValueError("Filename contains characters unsupported by Azure Files")
    timestamp = datetime.fromtimestamp(item.stat().st_mtime)
    source_hash = hashlib.sha256(source.id.encode("utf-8")).hexdigest()[:12]
    destination = (
        f"{timestamp:%Y/%Y-%m}/{timestamp:%Y%m%d_%H%M%S}_"
        f"{source_hash}_{item.name}"
    )
    if len(destination.rsplit("/", 1)[-1].encode("utf-16-le")) // 2 > 255:
        raise ValueError("Archive filename exceeds Azure Files' 255-character limit")
    run_id = uuid.uuid4().hex
    pending = f"_pending/{run_id}/data"
    stage_dir = staging / run_id
    stage_dir.mkdir(parents=True, mode=0o700)
    staged = stage_dir / item.name
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(ledger) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, source_id TEXT, "
            "destination TEXT, staged_path TEXT, pending_path TEXT, status TEXT, error TEXT)"
        )
        db.execute(
            "INSERT INTO items VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, source.id, destination, str(staged), pending, "staging", None),
        )
        db.commit()
        try:
            shutil.move(str(item), str(staged))
            db.execute("UPDATE items SET status = ? WHERE id = ?", ("pending", run_id))
            db.commit()
            local_hash = file_hash(staged)
            cloud.upload(staged, pending)
            remote_hash = hashlib.sha256()
            for chunk in cloud.read_pending(pending):
                remote_hash.update(chunk)
            if remote_hash.digest() != local_hash:
                raise ValueError("Pending upload verification failed")
            if file_hash(staged) != local_hash:
                raise ValueError("Staged file changed during upload")
            cloud.publish(pending, destination)
        except Exception as exc:
            db.execute(
                "UPDATE items SET status = ?, error = ? WHERE id = ?",
                ("failed", str(exc), run_id),
            )
            db.commit()
            raise
        db.execute("UPDATE items SET status = ? WHERE id = ?", ("published", run_id))
        db.commit()
        if file_hash(staged) != local_hash:
            raise ValueError("Staged file changed after publication; retained locally")
        staged.unlink()
        stage_dir.rmdir()
    return Result("published", destination)
