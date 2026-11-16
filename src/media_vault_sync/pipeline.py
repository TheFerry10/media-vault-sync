from __future__ import annotations

import uuid
import warnings
from collections.abc import Iterator
from pathlib import Path
from time import localtime
from typing import Protocol

from . import filesystem
from .domain import (
    Result,
    Source,
    Status,
    archive_destination,
    capture_time,
    select_capture_time,
    sha256,
    validate_source_id,
)
from .ledger import Ledger, backup_ledger, list_items

__all__ = ["Cloud", "Result", "Source", "list_items", "run_once"]

_LEDGER_BACKUP = "_ledger/ledger.sqlite"


class Cloud(Protocol):
    def upload(self, path: Path, pending: str) -> None: ...

    def read_pending(self, pending: str) -> Iterator[bytes]: ...

    def read_archive(self, destination: str) -> Iterator[bytes]: ...

    def publish(self, pending: str, destination: str) -> None: ...

    def publish_backup(self, pending: str, destination: str) -> None: ...


def _is_safe_stage(staged: Path, staging: Path) -> bool:
    if staged.is_symlink() or staged.parent.is_symlink() or not staged.is_file():
        return False
    try:
        return (
            staged.resolve().is_relative_to(staging.resolve())
            and staged.parent.parent.resolve() == staging.resolve()
        )
    except OSError:
        return False


def _reconcile(db: Ledger, staging: Path, cloud: Cloud) -> None:
    for item in db.recoverable_items():
        staged = Path(item.staged_path)
        if item.status == Status.PUBLISHED and not staged.exists():
            continue
        if isinstance(item.content_hash, bytes):
            expected_hash = item.content_hash
        elif item.content_hash is not None:
            try:
                expected_hash = bytes.fromhex(item.content_hash)
            except ValueError:
                continue
        elif _is_safe_stage(staged, staging):
            expected_hash = filesystem.file_hash(staged)
            db.record_hash(item.id, expected_hash)
        else:
            continue
        try:
            archive_hash = sha256(cloud.read_archive(item.destination))
        except Exception as exc:
            warnings.warn(
                f"Could not reconcile {item.destination}: {exc}", RuntimeWarning
            )
            continue
        if archive_hash != expected_hash:
            continue
        db.update(item.id, Status.PUBLISHED)
        try:
            if (
                _is_safe_stage(staged, staging)
                and filesystem.file_hash(staged) == expected_hash
            ):
                filesystem.cleanup_stage(staged)
        except OSError as exc:
            warnings.warn(
                f"Could not clean reconciled Staging file: {exc}", RuntimeWarning
            )


def _refresh_backup(ledger: Path, cloud: Cloud) -> bool:
    run_id = uuid.uuid4().hex
    candidate = ledger.with_name(f".{ledger.name}.{run_id}.backup")
    pending = f"_ledger/pending/{run_id}.sqlite"
    stale = False
    try:
        backup_ledger(ledger, candidate)
        cloud.upload(candidate, pending)
        if filesystem.file_hash(candidate) != sha256(cloud.read_pending(pending)):
            raise ValueError("Ledger backup verification failed")
        cloud.publish_backup(pending, _LEDGER_BACKUP)
    except Exception as exc:
        stale = True
        warnings.warn(f"Ledger backup is stale: {exc}", RuntimeWarning)
    finally:
        try:
            candidate.unlink(missing_ok=True)
        except OSError as exc:
            stale = True
            warnings.warn(
                f"Could not remove local ledger backup candidate: {exc}", RuntimeWarning
            )
    return stale


class Metadata(Protocol):
    def extract(self, path: Path) -> dict[str, str]: ...


def run_once(
    source: Source,
    item: Path,
    staging: Path,
    ledger: Path,
    cloud: Cloud,
    metadata: Metadata | None = None,
) -> Result:
    validate_source_id(source.id)
    modified_at = capture_time(filesystem.modification_time(source, item), localtime)
    tags = {} if metadata is None else metadata.extract(item)
    selected_time = select_capture_time(item.name, tags, modified_at)
    destination = archive_destination(source.id, item.name, selected_time.value)
    run_id = uuid.uuid4().hex
    pending = f"_pending/{run_id}/data"
    staged = filesystem.prepare_stage(staging, run_id, item.name)
    try:
        with Ledger(ledger) as db:
            _reconcile(db, staging, cloud)
            db.start(
                run_id,
                source,
                destination,
                staged,
                pending,
                selected_time.value,
                selected_time.source,
            )
            try:
                filesystem.stage_item(item, staged)
                local_hash = filesystem.file_hash(staged)
                db.record_hash(run_id, local_hash)
                db.update(run_id, Status.PENDING)
                duplicate_destination = db.find_published(source.id, local_hash)
                if duplicate_destination is not None:
                    destination = duplicate_destination
                    db.set_destination(run_id, destination)
                else:
                    cloud.upload(staged, pending)
                    if sha256(cloud.read_pending(pending)) != local_hash:
                        raise ValueError("Pending upload verification failed")
                    if filesystem.file_hash(staged) != local_hash:
                        raise ValueError("Staged file changed during upload")
                    try:
                        cloud.publish(pending, destination)
                    except FileExistsError:
                        destination = archive_destination(
                            source.id, item.name, selected_time.value, local_hash
                        )
                        db.set_destination(run_id, destination)
                        cloud.publish(pending, destination)
            except Exception as exc:
                db.update(run_id, Status.FAILED, str(exc))
                raise
            db.update(run_id, Status.PUBLISHED)
            if filesystem.file_hash(staged) != local_hash:
                raise ValueError(
                    "Staged file changed after publication; retained locally"
                )
            filesystem.cleanup_stage(staged)
    except Exception:
        _refresh_backup(ledger, cloud)
        raise
    backup_stale = _refresh_backup(ledger, cloud)
    return Result(Status.PUBLISHED, destination, backup_stale)
