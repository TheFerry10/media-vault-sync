import os
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest

from media_vault_sync.pipeline import Source, list_items, run_once


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

    with pytest.raises(ValueError, match="verification"):
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
