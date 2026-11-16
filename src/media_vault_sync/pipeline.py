from __future__ import annotations

import subprocess
import uuid
import warnings
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import localtime
from typing import Protocol, TypeVar

from . import filesystem
from .domain import (
    MEDIA_EXTENSIONS,
    FailureKind,
    Result,
    Source,
    Status,
    TransientTransferError,
    archive_destination,
    capture_time,
    select_capture_time,
    sha256,
    validate_source_id,
)
from .ledger import Ledger, backup_ledger, list_failed_items, list_items

__all__ = [
    "Cloud",
    "FailureKind",
    "Result",
    "Source",
    "Status",
    "TransientTransferError",
    "list_failed_items",
    "list_items",
    "run_inbox",
    "run_once",
]

_LEDGER_BACKUP = "_ledger/ledger.sqlite"


class Cloud(Protocol):
    def upload(self, path: Path, pending: str) -> None: ...

    def read_pending(self, pending: str) -> Iterator[bytes]: ...

    def read_archive(self, destination: str) -> Iterator[bytes]: ...

    def publish(self, pending: str, destination: str) -> None: ...

    def publish_backup(self, pending: str, destination: str) -> None: ...


class Metadata(Protocol):
    def extract(self, path: Path) -> dict[str, str]: ...


T = TypeVar("T")


def _transfer(operation: Callable[[], T], label: str) -> T:
    try:
        return operation()
    except FileExistsError:
        raise
    except TransientTransferError:
        raise
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        raise TransientTransferError(f"{label} failed: {exc}") from exc


def _error_summary(exc: Exception) -> str:
    summary = " ".join(str(exc).split())
    return summary[:500] or type(exc).__name__


def _retry_time(now: datetime, attempt_count: int) -> datetime:
    delay_minutes = min(15 * (2 ** min(attempt_count - 1, 5)), 360)
    return now + timedelta(minutes=delay_minutes)


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


def run_once(
    source: Source,
    item: Path,
    staging: Path,
    ledger: Path,
    cloud: Cloud,
    metadata: Metadata | None = None,
    *,
    now: datetime | None = None,
    retry: bool = False,
) -> Result:
    validate_source_id(source.id)
    modified_timestamp = filesystem.modification_time(source, item)
    source_path = item.resolve().relative_to(source.inbox.resolve()).as_posix()
    current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    destination = ""
    try:
        with Ledger(ledger) as db:
            _reconcile(db, staging, cloud)
            previous = db.latest(source.id, source_path)
            if not retry and previous is not None and previous.status == Status.FAILED:
                if previous.failure_kind == FailureKind.PERMANENT:
                    backup_stale = _refresh_backup(ledger, cloud)
                    return Result(
                        Status.SKIPPED,
                        previous.destination,
                        previous.error,
                        backup_stale,
                    )
                if (
                    previous.next_retry_at is not None
                    and current_time < datetime.fromisoformat(previous.next_retry_at)
                ):
                    backup_stale = _refresh_backup(ledger, cloud)
                    return Result(
                        Status.SKIPPED,
                        previous.destination,
                        previous.error,
                        backup_stale,
                    )
            attempt_count = (
                previous.attempt_count + 1
                if previous is not None
                and previous.status == Status.FAILED
                and previous.failure_kind == FailureKind.TRANSIENT
                and not retry
                else 1
            )
            modified_at = capture_time(modified_timestamp, localtime)
            tags = metadata.extract(item) if metadata is not None else {}
            selected_time = select_capture_time(item.name, tags, modified_at)
            destination = archive_destination(source.id, item.name, selected_time.value)
            run_id = uuid.uuid4().hex
            pending = f"_pending/{run_id}/data"
            staged = filesystem.prepare_stage(staging, run_id, item.name)
            db.start(
                run_id,
                source,
                source_path,
                destination,
                staged,
                pending,
                selected_time.value,
                selected_time.source,
                attempt_count,
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
                    _transfer(lambda: cloud.upload(staged, pending), "Upload")
                    pending_hash = sha256(
                        _transfer(lambda: cloud.read_pending(pending), "Read-back")
                    )
                    if pending_hash != local_hash:
                        raise TransientTransferError(
                            "Pending upload verification failed"
                        )
                    if filesystem.file_hash(staged) != local_hash:
                        raise ValueError("Staged file changed during upload")
                    try:
                        _transfer(
                            lambda: cloud.publish(pending, destination),
                            "Publication",
                        )
                    except FileExistsError:
                        destination = archive_destination(
                            source.id, item.name, selected_time.value, local_hash
                        )
                        db.set_destination(run_id, destination)
                        _transfer(
                            lambda: cloud.publish(pending, destination),
                            "Publication",
                        )
            except Exception as exc:
                transient = isinstance(exc, TransientTransferError)
                retry_at = (
                    _retry_time(current_time, attempt_count).isoformat()
                    if transient
                    else None
                )
                db.update(
                    run_id,
                    Status.FAILED,
                    _error_summary(exc),
                    FailureKind.TRANSIENT if transient else FailureKind.PERMANENT,
                    attempt_count if transient else 0,
                    retry_at,
                )
                raise
            db.update(run_id, Status.PUBLISHED)
            if filesystem.file_hash(staged) != local_hash:
                raise ValueError(
                    "Staged file changed after publication; retained locally"
                )
            if filesystem.file_hash(item) != local_hash:
                raise ValueError(
                    "Inbox file changed after publication; retained locally"
                )
            filesystem.cleanup_stage(staged)
            item.unlink()
    except Exception:
        _refresh_backup(ledger, cloud)
        raise
    backup_stale = _refresh_backup(ledger, cloud)
    return Result(Status.PUBLISHED, destination, backup_stale=backup_stale)


def run_inbox(
    source: Source,
    staging: Path,
    ledger: Path,
    cloud: Cloud,
    metadata: Metadata | None = None,
    *,
    now: datetime | None = None,
) -> list[Result]:
    results: list[Result] = []
    for item in sorted(source.inbox.rglob("*")):
        if (
            not item.is_file()
            or item.is_symlink()
            or item.suffix.lower() not in MEDIA_EXTENSIONS
        ):
            continue
        try:
            results.append(
                run_once(
                    source,
                    item,
                    staging,
                    ledger,
                    cloud,
                    metadata,
                    now=now,
                )
            )
        except Exception as exc:
            results.append(Result(Status.FAILED, "", _error_summary(exc)))
    return results
