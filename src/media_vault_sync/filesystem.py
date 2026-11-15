from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from .domain import Source, sha256


def capture_time(source: Source, item: Path) -> datetime:
    inbox = source.inbox.resolve(strict=True)
    if not inbox.is_dir() or not item.resolve(strict=True).is_relative_to(inbox):
        raise ValueError("Item must be inside the Source Inbox")
    if item.is_symlink() or not item.is_file():
        raise ValueError("Item must be a regular file, not a symlink")
    return datetime.fromtimestamp(item.stat().st_mtime)


def prepare_stage(staging: Path, run_id: str, filename: str) -> Path:
    stage_dir = staging / run_id
    stage_dir.mkdir(parents=True, mode=0o700)
    return stage_dir / filename


def stage_item(item: Path, staged: Path) -> None:
    shutil.move(str(item), str(staged))


def file_hash(path: Path) -> bytes:
    with path.open("rb") as stream:
        return sha256(iter(lambda: stream.read(1024 * 1024), b""))


def cleanup_stage(staged: Path) -> None:
    staged.unlink()
    staged.parent.rmdir()
