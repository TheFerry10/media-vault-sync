import os
import sys
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from media_vault_sync.cli import main
from media_vault_sync.pipeline import (
    FailureKind,
    Source,
    Status,
    TransientTransferError,
    list_failed_items,
    list_items,
    run_inbox,
    run_once,
)


class FakeCloud:
    def __init__(self) -> None:
        self.pending: dict[str, bytes] = {}
        self.archive: dict[str, bytes] = {}

    def upload(self, path: Path, pending: str) -> None:
        self.pending[pending] = path.read_bytes()

    def read_pending(self, pending: str) -> Iterator[bytes]:
        yield self.pending[pending]

    def publish(self, pending: str, destination: str) -> None:
        if destination in self.archive:
            raise FileExistsError(destination)
        self.archive[destination] = self.pending.pop(pending)


def test_archives_one_media_item_after_verification(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original media bytes")
    timestamp = datetime(2024, 1, 15, 12).timestamp()
    os.utime(media, (timestamp, timestamp))
    cloud = FakeCloud()

    result = run_once(
        Source("Phone-1", inbox),
        media,
        tmp_path / "staging",
        tmp_path / "ledger.sqlite",
        cloud,
    )

    assert result.status == "published"
    assert result.destination.startswith("2024/2024-01/")
    assert "07c3650bb7b5" in result.destination
    assert result.destination.endswith("_photo.jpg")
    assert cloud.archive[result.destination] == b"original media bytes"
    assert not media.exists()
    assert not list((tmp_path / "staging").rglob("photo.jpg"))


@pytest.mark.parametrize(
    ("timezone", "timestamp", "destination"),
    [
        ("UTC+02", 1704069000, "2023/2023-12/20231231_223000_"),
        ("Europe/Berlin", 1721082600, "2024/2024-07/20240716_003000_"),
    ],
)
def test_mtime_uses_local_timezone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timezone: str,
    timestamp: int,
    destination: str,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original")
    os.utime(media, (timestamp, timestamp))
    try:
        with monkeypatch.context() as environment:
            environment.setenv("TZ", timezone)
            time.tzset()
            result = run_once(
                Source("Phone-1", inbox),
                media,
                tmp_path / "staging",
                tmp_path / "ledger.sqlite",
                FakeCloud(),
            )
    finally:
        time.tzset()
    assert result.destination.startswith(destination)


def test_corrupt_pending_upload_is_not_published_or_cleaned(tmp_path: Path) -> None:
    class CorruptCloud(FakeCloud):
        def read_pending(self, pending: str) -> Iterator[bytes]:
            yield b"corrupted"

    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original")
    cloud = CorruptCloud()
    ledger = tmp_path / "ledger.sqlite"

    with pytest.raises(TransientTransferError, match="verification"):
        run_once(Source("Phone-1", inbox), media, tmp_path / "staging", ledger, cloud)

    assert cloud.archive == {}
    assert (
        list((tmp_path / "staging").rglob("photo.jpg"))[0].read_bytes() == b"original"
    )
    assert list_items(ledger)[0].status == "failed"


def test_changed_staged_file_is_not_published_or_cleaned(tmp_path: Path) -> None:
    class ChangedCloud(FakeCloud):
        def upload(self, path: Path, pending: str) -> None:
            super().upload(path, pending)
            path.write_bytes(b"new local content")

    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original")
    cloud = ChangedCloud()

    with pytest.raises(ValueError, match="changed"):
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            tmp_path / "ledger.sqlite",
            cloud,
        )

    assert cloud.archive == {}
    assert (
        list((tmp_path / "staging").rglob("photo.jpg"))[0].read_bytes()
        == b"new local content"
    )


def test_existing_archive_conflict_retains_both_copies(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"first")
    timestamp = datetime(2024, 1, 15, 12).timestamp()
    os.utime(media, (timestamp, timestamp))
    cloud = FakeCloud()
    ledger = tmp_path / "ledger.sqlite"
    first = run_once(
        Source("Phone-1", inbox), media, tmp_path / "staging", ledger, cloud
    )
    media.write_bytes(b"second")
    os.utime(media, (timestamp, timestamp))

    with pytest.raises(FileExistsError):
        run_once(Source("Phone-1", inbox), media, tmp_path / "staging", ledger, cloud)

    assert cloud.archive[first.destination] == b"first"
    assert list((tmp_path / "staging").rglob("photo.jpg"))[0].read_bytes() == b"second"
    assert [item.status for item in list_items(ledger)] == ["published", "failed"]


def test_source_id_stays_stable_when_inbox_moves(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"first")
    timestamp = datetime(2024, 1, 15, 12).timestamp()
    os.utime(media, (timestamp, timestamp))
    cloud = FakeCloud()
    first = run_once(
        Source("Phone-1", inbox),
        media,
        tmp_path / "staging",
        tmp_path / "ledger.sqlite",
        cloud,
    )
    inbox.rename(tmp_path / "moved-inbox")
    moved = tmp_path / "moved-inbox" / "photo.jpg"
    moved.write_bytes(b"second")
    os.utime(moved, (timestamp, timestamp))

    with pytest.raises(FileExistsError):
        run_once(
            Source("Phone-1", moved.parent),
            moved,
            tmp_path / "staging",
            tmp_path / "ledger.sqlite",
            cloud,
        )
    assert cloud.archive[first.destination] == b"first"


@pytest.mark.parametrize(
    "filename,source_id,outside",
    [
        ("photo.jpg", "", False),
        ("notes.txt", "Phone-1", False),
        ("photo.jpg", "Phone-1", True),
    ],
)
def test_invalid_intake_remains_untouched(
    tmp_path: Path, filename: str, source_id: str, outside: bool
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = (tmp_path if outside else inbox) / filename
    media.write_bytes(b"untouched")
    cloud = FakeCloud()

    with pytest.raises(ValueError):
        run_once(
            Source(source_id, inbox),
            media,
            tmp_path / "staging",
            tmp_path / "ledger.sqlite",
            cloud,
        )

    assert media.read_bytes() == b"untouched"
    assert not cloud.archive and not cloud.pending


def test_azure_adapter_publishes_verified_upload_without_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from media_vault_sync import azure

    remote = tmp_path / "remote"
    remote.mkdir()
    executable = tmp_path / "rclone"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os, pathlib, shutil, sys\n"
        "root = pathlib.Path(os.environ['FAKE_REMOTE'])\n"
        "args = sys.argv[3:]\n"
        "def remote(s): return root / s.removeprefix(':azurefiles:')\n"
        "if args[0] == 'copyto':\n"
        "    target = remote(args[2]); target.parent.mkdir(parents=True, exist_ok=True)\n"
        "    shutil.copyfile(args[1], target)\n"
        "elif args[0] == 'cat':\n"
        "    sys.stdout.buffer.write(remote(args[1]).read_bytes())\n"
        "elif args[0] == 'mkdir':\n"
        "    remote(args[1]).mkdir(parents=True, exist_ok=True)\n"
        "else: sys.exit(1)\n"
    )
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_REMOTE", str(remote))
    credentials = tmp_path / "credentials.json"
    credentials.write_text(
        '{"tenant_id":"tenant","client_id":"client","client_secret":"secret"}'
    )
    credentials.chmod(0o600)

    class FakeAzureFile:
        def __init__(
            self,
            *,
            account_url: str,
            share_name: str,
            file_path: str,
            credential: object,
            token_intent: str,
        ) -> None:
            assert account_url == "https://myaccount.file.core.windows.net"
            assert share_name == "archive"
            assert token_intent == "backup"
            self.path = remote / file_path

        def __enter__(self) -> "FakeAzureFile":
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def rename_file(self, destination: str, *, overwrite: bool) -> None:
            assert overwrite is False
            target = remote / destination
            if target.exists():
                raise FileExistsError(destination)
            self.path.rename(target)

    monkeypatch.setattr(azure, "ShareFileClient", FakeAzureFile)
    cloud = azure.AzureFiles("myaccount", "archive", credentials)
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    item = inbox / "photo.jpg"
    item.write_bytes(b"actual original")
    timestamp = datetime(2024, 1, 15, 12).timestamp()
    os.utime(item, (timestamp, timestamp))

    result = run_once(
        Source("Phone-1", inbox),
        item,
        tmp_path / "staging",
        tmp_path / "ledger.sqlite",
        cloud,
    )

    assert (remote / result.destination).read_bytes() == b"actual original"
    assert list_items(tmp_path / "ledger.sqlite")[0].status == "published"
    item.write_bytes(b"replacement")
    os.utime(item, (timestamp, timestamp))
    with pytest.raises(FileExistsError):
        run_once(
            Source("Phone-1", inbox),
            item,
            tmp_path / "staging",
            tmp_path / "ledger.sqlite",
            cloud,
        )
    assert (remote / result.destination).read_bytes() == b"actual original"
    assert (
        list((tmp_path / "staging").rglob("photo.jpg"))[0].read_bytes()
        == b"replacement"
    )


def test_transient_failure_retries_after_backoff_and_keeps_inbox_copy(
    tmp_path: Path,
) -> None:
    class FlakyCloud(FakeCloud):
        failures = 1

        def upload(self, path: Path, pending: str) -> None:
            if self.failures:
                self.failures -= 1
                raise ConnectionError("network unavailable")
            super().upload(path, pending)

    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original")
    ledger = tmp_path / "ledger.sqlite"
    cloud = FlakyCloud()
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(TransientTransferError, match="network unavailable"):
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            ledger,
            cloud,
            now=now,
        )

    failed = list_items(ledger)[0]
    assert media.read_bytes() == b"original"
    assert failed.failure_kind == FailureKind.TRANSIENT
    assert failed.attempt_count == 1
    assert failed.next_retry_at == (now + timedelta(minutes=15)).isoformat()
    assert (
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            ledger,
            cloud,
            now=now + timedelta(minutes=14),
        ).status
        == Status.SKIPPED
    )
    assert len(list_items(ledger)) == 1
    assert (
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            ledger,
            cloud,
            now=now + timedelta(minutes=15),
        ).status
        == Status.PUBLISHED
    )
    assert not media.exists()


def test_transient_backoff_grows_exponentially(tmp_path: Path) -> None:
    class OfflineCloud(FakeCloud):
        def upload(self, path: Path, pending: str) -> None:
            raise ConnectionError("network unavailable")

    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original")
    ledger = tmp_path / "ledger.sqlite"
    cloud = OfflineCloud()
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    elapsed_minutes = (0, 15, 45, 105, 225, 465)
    for attempt, elapsed in enumerate(elapsed_minutes, start=1):
        with pytest.raises(TransientTransferError, match="network unavailable"):
            run_once(
                Source("Phone-1", inbox),
                media,
                tmp_path / "staging",
                ledger,
                cloud,
                now=now + timedelta(minutes=elapsed),
            )
        failure = list_items(ledger)[-1]
        assert failure.attempt_count == attempt
        delay = min(15 * 2 ** (attempt - 1), 360)
        assert (
            failure.next_retry_at
            == (now + timedelta(minutes=elapsed + delay)).isoformat()
        )


def test_permanent_failure_requires_explicit_retry_and_review_shows_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class InvalidMediaOnceCloud(FakeCloud):
        failures = 1

        def upload(self, path: Path, pending: str) -> None:
            if self.failures:
                self.failures -= 1
                raise ValueError("Invalid media encoding")
            super().upload(path, pending)

    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original")
    ledger = tmp_path / "ledger.sqlite"
    cloud = InvalidMediaOnceCloud()
    source = Source("Phone-1", inbox)

    with pytest.raises(ValueError, match="Invalid media encoding"):
        run_once(source, media, tmp_path / "staging", ledger, cloud)

    failed = list_failed_items(ledger)
    assert len(failed) == 1
    assert failed[0].source_path == "photo.jpg"
    assert failed[0].failure_kind == FailureKind.PERMANENT
    assert failed[0].error == "Invalid media encoding"
    assert media.read_bytes() == b"original"
    config = tmp_path / "config.toml"
    staging = tmp_path / "staging"
    config.write_text(
        f'ledger = "{ledger}"\n'
        f'staging = "{staging}"\n'
        "[sources]\n"
        f'Phone-1 = "{inbox}"\n'
        "[azure]\n"
        'account = "myaccount"\n'
        'share = "archive"\n'
        f'credentials = "{tmp_path / "credentials.json"}"\n'
    )
    for command in ("status", "review"):
        monkeypatch.setattr(
            sys, "argv", ["media-vault-sync", "--config", str(config), command]
        )
        assert main() == 0
        output = capsys.readouterr().out
        assert "photo.jpg" in output
        assert "Invalid media encoding" in output
    assert run_once(source, media, tmp_path / "staging", ledger, cloud).status == (
        Status.SKIPPED
    )
    assert len(list_items(ledger)) == 1
    from media_vault_sync import azure

    def fake_azure(account: str, share: str, credentials: Path) -> FakeCloud:
        return cloud

    monkeypatch.setattr(azure, "AzureFiles", fake_azure)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "media-vault-sync",
            "--config",
            str(config),
            "retry",
            "Phone-1",
            "photo.jpg",
        ],
    )
    assert main() == 0
    assert "published" in capsys.readouterr().out
    assert not media.exists()
    assert list_failed_items(ledger) == []


def test_batch_failure_does_not_stop_unrelated_items(tmp_path: Path) -> None:
    class OneItemFailsCloud(FakeCloud):
        def upload(self, path: Path, pending: str) -> None:
            if path.name == "bad.jpg":
                raise ValueError("invalid media")
            super().upload(path, pending)

    inbox = tmp_path / "inbox"
    inbox.mkdir()
    bad = inbox / "bad.jpg"
    good = inbox / "good.jpg"
    bad.write_bytes(b"bad")
    good.write_bytes(b"good")

    results = run_inbox(
        Source("Phone-1", inbox),
        tmp_path / "staging",
        tmp_path / "ledger.sqlite",
        OneItemFailsCloud(),
        now=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    assert [result.status for result in results] == [Status.FAILED, Status.PUBLISHED]
    assert bad.read_bytes() == b"bad"
    assert not good.exists()
    failures = list_failed_items(tmp_path / "ledger.sqlite")
    assert len(failures) == 1
    assert failures[0].error == "invalid media"
