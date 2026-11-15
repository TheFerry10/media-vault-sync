from __future__ import annotations

import shutil
from pathlib import Path

from .domain import MEDIA_EXTENSIONS, SIDECAR_EXTENSIONS, Source, sha256


def modification_time(source: Source, item: Path) -> float:
    inbox = source.inbox.resolve(strict=True)
    if not inbox.is_dir() or not item.resolve(strict=True).is_relative_to(inbox):
        raise ValueError("Item must be inside the Source Inbox")
    if item.is_symlink() or not item.is_file():
        raise ValueError("Item must be a regular file, not a symlink")
    return item.stat().st_mtime


def associated_sidecars(source: Source, item: Path) -> tuple[Path, ...]:
    inbox = source.inbox.resolve(strict=True)
    selected = item.resolve(strict=True)
    if not inbox.is_dir() or item.is_symlink() or not selected.is_relative_to(inbox):
        raise ValueError("Item must be inside the Source Inbox")
    if not item.is_file():
        raise ValueError("Item must be a regular file, not a symlink")

    media_with_stem = [
        path
        for path in inbox.rglob("*")
        if not path.is_symlink()
        and path.is_file()
        and path.stem == item.stem
        and path.suffix.lower() in MEDIA_EXTENSIONS
    ]
    if len(media_with_stem) != 1 or media_with_stem[0].resolve() != selected:
        return ()
    sidecars_by_name: dict[str, list[Path]] = {}
    for path in inbox.rglob("*"):
        if (
            not path.is_symlink()
            and path.is_file()
            and path.stem == item.stem
            and path.suffix.lower() in SIDECAR_EXTENSIONS
        ):
            sidecars_by_name.setdefault(path.name, []).append(path)
    return tuple(
        sorted(
            paths[0] for paths in sidecars_by_name.values() if len(paths) == 1
        )
    )


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
