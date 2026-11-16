from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from azure.core.exceptions import AzureError, ResourceExistsError

from .domain import MEDIA_EXTENSIONS, Result, Source
from .exiftool import ExifTool
from .ledger import Ledger, backup_ledger
from .locking import exclusive_lock
from .pipeline import Cloud, run_once
from .scheduler import (
    TRANSIENT_NOTIFICATION_DELAY,
    FailureKind,
    Snapshot,
    backup_is_stale,
    notification_due,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PollConfig:
    sources: dict[str, Path]
    staging: Path
    ledger: Path
    backup: Path
    backup_max_age: float = TRANSIENT_NOTIFICATION_DELAY

    def __post_init__(self) -> None:
        if self.ledger.resolve() == self.backup.resolve():
            raise ValueError("Ledger backup must use a separate path")
        if self.backup_max_age <= 0:
            raise ValueError("Ledger backup maximum age must be positive")


@dataclass(frozen=True, slots=True)
class PollSummary:
    seen: int = 0
    waiting: int = 0
    published: int = 0
    failed: int = 0
    overlap: bool = False


Notifier = Callable[[str, str], bool]
Clock = Callable[[], float]
Runner = Callable[[Source, Path, Path, Path, Cloud], Result]


def _run_once(
    source: Source, item: Path, staging: Path, ledger: Path, cloud: Cloud
) -> Result:
    return run_once(source, item, staging, ledger, cloud, ExifTool())


def notify_desktop(title: str, message: str) -> bool:
    if not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        return False
    executable = shutil.which("notify-send")
    if executable is None:
        return False
    try:
        result = subprocess.run(
            [executable, "--app-name=Media Vault Sync", title, message],
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        logger.warning("desktop notification could not be delivered")
        return False
    return result.returncode == 0


def _failure_kind(error: Exception) -> FailureKind:
    if isinstance(error, (ValueError, FileExistsError, ResourceExistsError)):
        return FailureKind.PERMANENT
    if isinstance(
        error, (OSError, subprocess.SubprocessError, RuntimeError, AzureError)
    ):
        return FailureKind.TRANSIENT
    return FailureKind.UNKNOWN


def _failure_guidance(kind: FailureKind) -> str:
    if kind is FailureKind.PERMANENT:
        return "check the file format, naming rules, or archive conflict"
    if kind is FailureKind.TRANSIENT:
        return "check Azure connectivity and credentials; retry will continue"
    return "inspect the ledger status and retained local copy"


def _send_item_notification(
    ledger: Ledger,
    source_id: str,
    path: str,
    kind: FailureKind,
    now: float,
    notifier: Notifier,
) -> None:
    failure_since, notified = ledger.record_failure(source_id, path, kind, now)
    if notified or not notification_due(kind, failure_since, now):
        return
    messages = {
        FailureKind.PERMANENT: "A stable item needs manual attention.",
        FailureKind.TRANSIENT: "A stable item has failed repeatedly for 24 hours.",
        FailureKind.UNKNOWN: "A stable item could not be classified and needs review.",
    }
    logger.warning("desktop notification due for %s item", kind.value)
    if notifier("Media Vault Sync", messages[kind]):
        ledger.mark_notified(source_id, path)


def poll_once(
    config: PollConfig,
    cloud: Cloud,
    notifier: Notifier = notify_desktop,
    clock: Clock = time.time,
    runner: Runner = _run_once,
) -> PollSummary:
    lock_path = config.ledger.with_suffix(config.ledger.suffix + ".lock")
    with exclusive_lock(lock_path) as acquired:
        if not acquired:
            logger.warning("poll skipped because another run is active")
            return PollSummary(overlap=True)

        seen = waiting = published = failed = 0
        now = clock()
        with Ledger(config.ledger) as ledger:
            try:
                backup_stat = config.backup.stat()
                backup_mtime: float | None = backup_stat.st_mtime
            except OSError:
                backup_mtime = None
            if backup_is_stale(backup_mtime, now, config.backup_max_age):
                logger.warning("ledger backup is missing or stale")
                notifier("Media Vault Sync", "The ledger backup is missing or stale.")
                try:
                    backup_ledger(config.ledger, config.backup)
                except (OSError, RuntimeError, sqlite3.Error, ValueError):
                    logger.exception("ledger backup could not be refreshed")

            for source_id, inbox in config.sources.items():
                source = Source(source_id, inbox)
                try:
                    items = [
                        path
                        for path in inbox.iterdir()
                        if not path.is_symlink() and path.is_file()
                    ]
                except OSError:
                    ledger.reset_stability_for_source(source_id)
                    logger.exception("Inbox could not be scanned")
                    failed += 1
                    continue
                present = {str(path) for path in items}
                for old_path in ledger.paths_for_source(source_id) - present:
                    failure_kind = ledger.failure_kind(source_id, old_path)
                    if failure_kind is None:
                        ledger.forget(source_id, old_path)
                    else:
                        ledger.reset_stability(source_id, old_path)
                        _send_item_notification(
                            ledger,
                            source_id,
                            old_path,
                            failure_kind,
                            now,
                            notifier,
                        )

                for item in items:
                    seen += 1
                    try:
                        item_stat = item.stat()
                        snapshot = Snapshot(item_stat.st_size, item_stat.st_mtime_ns)
                    except OSError:
                        ledger.reset_stability(source_id, str(item))
                        logger.exception("Inbox item could not be inspected")
                        failed += 1
                        continue
                    if not ledger.observe(source_id, str(item), snapshot):
                        waiting += 1
                        continue
                    if item.suffix.lower() not in MEDIA_EXTENSIONS:
                        logger.warning(
                            "stable item has an unsupported file type; use a supported "
                            "media format or remove it from this Inbox"
                        )
                        _send_item_notification(
                            ledger,
                            source_id,
                            str(item),
                            FailureKind.UNKNOWN,
                            now,
                            notifier,
                        )
                        failed += 1
                        continue
                    try:
                        runner(source, item, config.staging, config.ledger, cloud)
                    except Exception as error:
                        kind = _failure_kind(error)
                        logger.error(
                            "stable item failed (%s); %s",
                            type(error).__name__,
                            _failure_guidance(kind),
                        )
                        _send_item_notification(
                            ledger, source_id, str(item), kind, now, notifier
                        )
                        failed += 1
                    else:
                        ledger.forget(source_id, str(item))
                        published += 1

        summary = PollSummary(seen, waiting, published, failed)
        logger.info(
            "poll complete: seen=%d waiting=%d published=%d failed=%d",
            summary.seen,
            summary.waiting,
            summary.published,
            summary.failed,
        )
        return summary
