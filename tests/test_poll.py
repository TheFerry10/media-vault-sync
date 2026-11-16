from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from azure.core.exceptions import ResourceExistsError

from media_vault_sync.domain import Result, Source, Status
from media_vault_sync.locking import exclusive_lock
from media_vault_sync.pipeline import Cloud
from media_vault_sync.poll import PollConfig, poll_once
from media_vault_sync.scheduler import FailureKind, notification_due


class FakeCloud:
    def upload(self, path: Path, pending: str) -> None:
        raise AssertionError("cloud should not be called by the fake runner")

    def read_pending(self, pending: str) -> Iterator[bytes]:
        raise AssertionError("cloud should not be called by the fake runner")
        yield b""

    def publish(self, pending: str, destination: str) -> None:
        raise AssertionError("cloud should not be called by the fake runner")

    def read_archive(self, destination: str) -> Iterator[bytes]:
        raise AssertionError("cloud should not be called by the fake runner")
        yield b""

    def publish_backup(self, pending: str, destination: str) -> None:
        raise AssertionError("cloud should not be called by the fake runner")


def _config(tmp_path: Path, *, backup_max_age: float = 10**12) -> PollConfig:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    ledger = tmp_path / "ledger.sqlite"
    backup = tmp_path / "ledger.sqlite.backup"
    backup.touch()
    os.utime(backup, (time.time(), time.time()))
    return PollConfig(
        {"Phone-1": inbox},
        tmp_path / "staging",
        ledger,
        backup,
        backup_max_age,
    )


def test_item_is_processed_only_after_two_matching_polls(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = config.sources["Phone-1"] / "photo.jpg"
    item.write_bytes(b"media")
    calls: list[str] = []

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        calls.append(path.name)
        return Result(Status.PUBLISHED, "archive-path")

    first = poll_once(config, FakeCloud(), runner=runner)
    assert first.waiting == 1
    assert calls == []

    second = poll_once(config, FakeCloud(), runner=runner)
    assert second.published == 1
    assert calls == ["photo.jpg"]


def test_changed_snapshot_restarts_the_stability_count(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = config.sources["Phone-1"] / "photo.jpg"
    item.write_bytes(b"first")
    calls: list[str] = []

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        calls.append(path.name)
        return Result(Status.PUBLISHED, "archive-path")

    poll_once(config, FakeCloud(), runner=runner)
    item.write_bytes(b"changed")
    second = poll_once(config, FakeCloud(), runner=runner)
    assert second.waiting == 1
    assert calls == []

    third = poll_once(config, FakeCloud(), runner=runner)
    assert third.published == 1
    assert calls == ["photo.jpg"]


def test_failed_inbox_scan_restarts_stability_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    inbox = config.sources["Phone-1"]
    item = inbox / "photo.jpg"
    item.write_bytes(b"media")
    calls: list[str] = []
    should_fail = [False]
    original_iterdir = Path.iterdir

    def iterdir(path: Path) -> Iterator[Path]:
        if path == inbox and should_fail[0]:
            raise OSError("inbox unavailable")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", iterdir)

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        calls.append(path.name)
        return Result(Status.PUBLISHED, "archive-path")

    poll_once(config, FakeCloud(), runner=runner)
    should_fail[0] = True
    failed_scan = poll_once(config, FakeCloud(), runner=runner)
    assert failed_scan.failed == 1
    should_fail[0] = False

    resumed = poll_once(config, FakeCloud(), runner=runner)
    assert resumed.waiting == 1
    assert calls == []
    poll_once(config, FakeCloud(), runner=runner)
    assert calls == ["photo.jpg"]


def test_failed_item_inspection_restarts_stability_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    item = config.sources["Phone-1"] / "photo.jpg"
    item.write_bytes(b"media")
    calls: list[str] = []
    should_fail = [False]
    followed_stat_calls = [0]
    original_stat = Path.stat

    def stat(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        if path == item and follow_symlinks:
            followed_stat_calls[0] += 1
            if should_fail[0] and followed_stat_calls[0] % 2 == 0:
                should_fail[0] = False
                raise OSError("item unavailable")
        return original_stat(path, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", stat)

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        calls.append(path.name)
        return Result(Status.PUBLISHED, "archive-path")

    poll_once(config, FakeCloud(), runner=runner)
    should_fail[0] = True
    failed_inspection = poll_once(config, FakeCloud(), runner=runner)
    assert failed_inspection.failed == 1

    resumed = poll_once(config, FakeCloud(), runner=runner)
    assert resumed.waiting == 1
    assert calls == []
    poll_once(config, FakeCloud(), runner=runner)
    assert calls == ["photo.jpg"]


def test_poll_lock_prevents_overlapping_runs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    lock_path = config.ledger.with_suffix(".sqlite.lock")
    calls: list[str] = []

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        calls.append(path.name)
        return Result(Status.PUBLISHED, "archive-path")

    with exclusive_lock(lock_path) as acquired:
        assert acquired
        summary = poll_once(config, FakeCloud(), runner=runner)

    assert summary.overlap
    assert calls == []


def test_permanent_and_unknown_items_notify_immediately(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = config.sources["Phone-1"] / "notes.txt"
    item.write_text("not media")
    notifications: list[str] = []

    def notify(title: str, message: str) -> bool:
        notifications.append(message)
        return True

    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: 100)
    assert notifications == []
    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: 101)

    assert notifications == ["A stable item could not be classified and needs review."]
    assert "notes.txt" not in notifications[0]


def test_permanent_failure_notifies_immediately(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = config.sources["Phone-1"] / "photo.jpg"
    item.write_bytes(b"media")
    notifications: list[str] = []

    def notify(title: str, message: str) -> bool:
        notifications.append(message)
        return True

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        raise ValueError("invalid archive name")

    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: 100, runner=runner)
    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: 101, runner=runner)

    assert notifications == ["A stable item needs manual attention."]


def test_azure_destination_conflict_notifies_immediately(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = config.sources["Phone-1"] / "photo.jpg"
    item.write_bytes(b"media")
    notifications: list[str] = []

    def notify(title: str, message: str) -> bool:
        notifications.append(message)
        return True

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        raise ResourceExistsError("destination exists")

    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: 100, runner=runner)
    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: 101, runner=runner)

    assert notifications == ["A stable item needs manual attention."]


def test_transient_failure_notifies_after_24_hours_once(tmp_path: Path) -> None:
    config = _config(tmp_path, backup_max_age=10**12)
    item = config.sources["Phone-1"] / "photo.jpg"
    item.write_bytes(b"media")
    notifications: list[str] = []
    now = [time.time()]

    def notify(title: str, message: str) -> bool:
        notifications.append(message)
        return True

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        staging.mkdir(exist_ok=True)
        path.rename(staging / path.name)
        raise OSError("network unavailable")

    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: now[0], runner=runner)
    now[0] += 1
    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: now[0], runner=runner)
    assert notifications == []

    now[0] += 24 * 60 * 60 - 1
    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: now[0], runner=runner)
    assert notifications == []

    now[0] += 1
    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: now[0], runner=runner)
    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: now[0], runner=runner)

    assert notifications == ["A stable item has failed repeatedly for 24 hours."]


def test_reappearing_failed_item_needs_two_new_polls(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = config.sources["Phone-1"] / "photo.jpg"
    item.write_bytes(b"media")
    calls = 0

    def runner(
        source: Source, path: Path, staging: Path, ledger: Path, cloud: Cloud
    ) -> Result:
        nonlocal calls
        calls += 1
        staging.mkdir(exist_ok=True)
        path.rename(staging / path.name)
        raise OSError("network unavailable")

    poll_once(config, FakeCloud(), runner=runner, clock=lambda: 100)
    poll_once(config, FakeCloud(), runner=runner, clock=lambda: 101)
    poll_once(config, FakeCloud(), runner=runner, clock=lambda: 102)
    assert calls == 1

    staged = config.staging / item.name
    staged.rename(item)
    first_reappearance = poll_once(
        config, FakeCloud(), runner=runner, clock=lambda: 103
    )
    assert first_reappearance.waiting == 1
    assert calls == 1

    poll_once(config, FakeCloud(), runner=runner, clock=lambda: 104)
    assert calls == 2


def test_missing_backup_triggers_notification_and_is_created(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.backup.unlink()
    notifications: list[str] = []

    def notify(title: str, message: str) -> bool:
        notifications.append(message)
        return True

    poll_once(config, FakeCloud(), notifier=notify)

    assert notifications == ["The ledger backup is missing or stale."]
    assert config.backup.is_file()


def test_stale_backup_triggers_notification_and_refresh(tmp_path: Path) -> None:
    config = _config(tmp_path, backup_max_age=60)
    os.utime(config.backup, (1, 1))
    notifications: list[str] = []

    def notify(title: str, message: str) -> bool:
        notifications.append(message)
        return True

    poll_once(config, FakeCloud(), notifier=notify, clock=lambda: 1000)

    assert notifications == ["The ledger backup is missing or stale."]
    assert config.backup.read_bytes().startswith(b"SQLite format 3")


def test_notification_decisions_use_the_24_hour_threshold() -> None:
    since = 100.0
    assert not notification_due(FailureKind.TRANSIENT, since, since + 86399)
    assert notification_due(FailureKind.TRANSIENT, since, since + 86400)
    assert notification_due(FailureKind.PERMANENT, since, since)
    assert notification_due(FailureKind.UNKNOWN, since, since)
