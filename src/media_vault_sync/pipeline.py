from __future__ import annotations

import subprocess
import uuid
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
    sha256,
    validate_source_id,
)
from .ledger import Ledger, list_failed_items, list_items

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


class Cloud(Protocol):
    def upload(self, path: Path, pending: str) -> None: ...

    def read_pending(self, pending: str) -> Iterator[bytes]: ...

    def publish(self, pending: str, destination: str) -> None: ...


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


def run_once(
    source: Source,
    item: Path,
    staging: Path,
    ledger: Path,
    cloud: Cloud,
    *,
    now: datetime | None = None,
    retry: bool = False,
) -> Result:
    validate_source_id(source.id)
    filesystem.modification_time(source, item)
    source_path = item.resolve().relative_to(source.inbox.resolve()).as_posix()
    current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    with Ledger(ledger) as db:
        previous = db.latest(source.id, source_path)
        if not retry and previous is not None and previous.status == Status.FAILED:
            if previous.failure_kind == FailureKind.PERMANENT:
                return Result(Status.SKIPPED, previous.destination, previous.error)
            if (
                previous.next_retry_at is not None
                and current_time < datetime.fromisoformat(previous.next_retry_at)
            ):
                return Result(Status.SKIPPED, previous.destination, previous.error)
        attempt_count = (
            previous.attempt_count + 1
            if previous is not None
            and previous.status == Status.FAILED
            and previous.failure_kind == FailureKind.TRANSIENT
            and not retry
            else 1
        )
        run_id = uuid.uuid4().hex
        pending = f"_pending/{run_id}/data"
        staged = filesystem.prepare_stage(staging, run_id, item.name)
        db.start(run_id, source, source_path, staged, pending, attempt_count)
        try:
            timestamp = capture_time(
                filesystem.modification_time(source, item), localtime
            )
            destination = archive_destination(source.id, item.name, timestamp)
            db.set_destination(run_id, destination)
            filesystem.stage_item(item, staged)
            db.update(run_id, Status.PENDING)
            local_hash = filesystem.file_hash(staged)
            _transfer(lambda: cloud.upload(staged, pending), "Upload")
            pending_hash = sha256(
                _transfer(lambda: cloud.read_pending(pending), "Read-back")
            )
            if pending_hash != local_hash:
                raise TransientTransferError("Pending upload verification failed")
            if filesystem.file_hash(staged) != local_hash:
                raise ValueError("Staged file changed during upload")
            _transfer(lambda: cloud.publish(pending, destination), "Publication")
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
            raise ValueError("Staged file changed after publication; retained locally")
        if filesystem.file_hash(item) != local_hash:
            raise ValueError("Inbox file changed after publication; retained locally")
        filesystem.cleanup_stage(staged)
        item.unlink()
    return Result(Status.PUBLISHED, destination)


def run_inbox(
    source: Source,
    staging: Path,
    ledger: Path,
    cloud: Cloud,
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
            results.append(run_once(source, item, staging, ledger, cloud, now=now))
        except Exception as exc:
            results.append(Result(Status.FAILED, "", _error_summary(exc)))
    return results
