import os
import sqlite3
import subprocess
import time
from collections.abc import Iterator
from datetime import datetime
from hashlib import sha256 as hashlib_sha256
from pathlib import Path

import pytest

from media_vault_sync.exiftool import ExifTool
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


class FakeMetadata:
    def __init__(self, tags: dict[str, str] | None = None) -> None:
        self.tags = tags or {}

    def extract(self, path: Path) -> dict[str, str]:
        return self.tags


def fake_exiftool(tmp_path: Path, item: Path, output: str, exit_code: int = 0) -> str:
    executable = tmp_path / "exiftool"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "assert sys.argv[1:-1] == "
        "['-json', '-DateTimeOriginal', '-MediaCreateDate', '-CreateDate', '--']\n"
        f"assert sys.argv[-1] == {str(item)!r}\n"
        f"sys.stdout.write({output!r})\n"
        f"sys.exit({exit_code})\n"
    )
    executable.chmod(0o755)
    return str(executable)


def test_exiftool_failure_reports_sanitized_diagnostic_and_retains_inbox(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"original")
    cloud = FakeCloud()

    def fail(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(
            2,
            "exiftool",
            stderr=f"Error: invalid image - {media}\nSensitive second line",
        )

    monkeypatch.setattr(subprocess, "run", fail)

    with pytest.raises(
        RuntimeError,
        match=r"ExifTool failed with exit status 2: Error: invalid image - <media file>",
    ) as error:
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            tmp_path / "ledger.sqlite",
            cloud,
            ExifTool(),
        )

    assert str(media) not in str(error.value)
    assert "\n" not in str(error.value)
    assert media.read_bytes() == b"original"
    assert not cloud.archive and not cloud.pending


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
        FakeMetadata(),
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
                FakeMetadata(),
            )
    finally:
        time.tzset()
    assert result.destination.startswith(destination)


@pytest.mark.parametrize(
    ("filename", "tags", "expected_time", "expected_source"),
    [
        (
            "photo.jpg",
            {
                "DateTimeOriginal": "2022:03:04 05:06:07",
                "CreateDate": "2023:04:05 06:07:08",
            },
            datetime(2022, 3, 4, 5, 6, 7),
            "DateTimeOriginal",
        ),
        (
            "video.mp4",
            {
                "MediaCreateDate": "2022:03:04 05:06:07+09:00",
                "CreateDate": "2023:04:05 06:07:08",
                "DateTimeOriginal": "2021:02:03 04:05:06",
            },
            datetime(2022, 3, 4, 5, 6, 7),
            "MediaCreateDate",
        ),
        (
            "photo.jpg",
            {
                "DateTimeOriginal": "not a date",
                "CreateDate": "2023:04:05 06:07:08",
            },
            datetime(2023, 4, 5, 6, 7, 8),
            "CreateDate",
        ),
        (
            "WhatsApp Image 2024-03-04 at 05.06.07.jpeg",
            {"DateTimeOriginal": "0000:00:00 00:00:00"},
            datetime(2024, 3, 4, 5, 6, 7),
            "WhatsApp filename",
        ),
        (
            "IMG-20240304-WA0001.jpg",
            {},
            datetime(2024, 3, 4),
            "WhatsApp filename",
        ),
        (
            "Screenshot_20240304-050607.jpg",
            {},
            datetime.fromtimestamp(datetime(2024, 2, 1, 12).timestamp()),
            "filesystem mtime",
        ),
    ],
)
def test_capture_time_selection_is_recorded_at_pipeline_boundary(
    tmp_path: Path,
    filename: str,
    tags: dict[str, str],
    expected_time: datetime,
    expected_source: str,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / filename
    media.write_bytes(b"media")
    modified_timestamp = datetime(2024, 2, 1, 12).timestamp()
    os.utime(media, (modified_timestamp, modified_timestamp))
    ledger = tmp_path / "ledger.sqlite"

    result = run_once(
        Source("Phone-1", inbox),
        media,
        tmp_path / "staging",
        ledger,
        FakeCloud(),
        FakeMetadata(tags),
    )

    item = list_items(ledger)[0]
    assert item.capture_time == expected_time.isoformat()
    assert item.capture_time_source == expected_source
    assert result.destination.startswith(f"{expected_time:%Y/%Y-%m}/")


def test_exiftool_tags_select_destination_and_ledger_source(
    tmp_path: Path,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"media")
    modified_timestamp = datetime(2024, 2, 1, 12).timestamp()
    os.utime(media, (modified_timestamp, modified_timestamp))
    capture_time = datetime(2022, 3, 4, 5, 6, 7)
    executable = fake_exiftool(
        tmp_path,
        media,
        '[{"DateTimeOriginal":"2022:03:04 05:06:07"}]',
    )
    ledger = tmp_path / "ledger.sqlite"
    cloud = FakeCloud()

    result = run_once(
        Source("Phone-1", inbox),
        media,
        tmp_path / "staging",
        ledger,
        cloud,
        ExifTool(executable),
    )

    item = list_items(ledger)[0]
    assert result.destination.startswith("2022/2022-03/20220304_050607_")
    assert item.capture_time == capture_time.isoformat()
    assert item.capture_time_source == "DateTimeOriginal"
    assert cloud.archive[result.destination] == b"media"


@pytest.mark.parametrize(
    ("output", "exit_code", "error"),
    [
        ("not JSON", 0, ValueError),
        ("", 1, RuntimeError),
    ],
)
def test_exiftool_failure_retains_inbox_item_without_upload(
    tmp_path: Path,
    output: str,
    exit_code: int,
    error: type[Exception],
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"media")
    executable = fake_exiftool(tmp_path, media, output, exit_code)
    cloud = FakeCloud()

    with pytest.raises(error):
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            tmp_path / "ledger.sqlite",
            cloud,
            ExifTool(executable),
        )

    assert media.read_bytes() == b"media"
    assert cloud.pending == {}
    assert cloud.archive == {}


def test_legacy_ledger_remains_readable(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.sqlite"
    with sqlite3.connect(ledger) as db:
        db.execute(
            "CREATE TABLE items (id TEXT PRIMARY KEY, source_id TEXT, "
            "destination TEXT, staged_path TEXT, pending_path TEXT, status TEXT, error TEXT)"
        )
        db.execute(
            "INSERT INTO items VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "run",
                "Phone-1",
                "2024/2024-01/item.jpg",
                "/staging/item.jpg",
                "_pending/run/data",
                "published",
                None,
            ),
        )

    item = list_items(ledger)[0]

    assert item.status == "published"
    assert item.capture_time is None
    assert item.capture_time_source is None


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
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            ledger,
            cloud,
            FakeMetadata(),
        )

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
            FakeMetadata(),
        )

    assert cloud.archive == {}
    assert (
        list((tmp_path / "staging").rglob("photo.jpg"))[0].read_bytes()
        == b"new local content"
    )


def test_different_content_collision_gets_deterministic_hash_suffix(
    tmp_path: Path,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"first")
    timestamp = datetime(2024, 1, 15, 12).timestamp()
    os.utime(media, (timestamp, timestamp))
    cloud = FakeCloud()
    ledger = tmp_path / "ledger.sqlite"
    first = run_once(
        Source("Phone-1", inbox),
        media,
        tmp_path / "staging",
        ledger,
        cloud,
        FakeMetadata(),
    )
    media.write_bytes(b"second")
    os.utime(media, (timestamp, timestamp))

    with pytest.raises(FileExistsError):
        run_once(
            Source("Phone-1", inbox),
            media,
            tmp_path / "staging",
            ledger,
            cloud,
            FakeMetadata(),
        )

    assert cloud.archive[first.destination] == b"first"
    assert cloud.archive[second.destination] == b"second"
    assert second.destination != first.destination
    assert second.destination.endswith(f"_{hashlib_sha256(b'second').hexdigest()}.jpg")
    assert not list((tmp_path / "staging").rglob("photo.jpg"))
    assert [item.status for item in list_items(ledger)] == ["published", "published"]


def test_same_source_reimport_is_removed_after_verified_publication(
    tmp_path: Path,
) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    media = inbox / "photo.jpg"
    media.write_bytes(b"same media")
    timestamp = datetime(2024, 1, 15, 12).timestamp()
    os.utime(media, (timestamp, timestamp))
    cloud = FakeCloud()
    ledger = tmp_path / "ledger.sqlite"
    first = run_once(
        Source("Phone-1", inbox), media, tmp_path / "staging", ledger, cloud
    )

    media.write_bytes(b"same media")
    os.utime(media, (timestamp, timestamp))
    duplicate = run_once(
        Source("Phone-1", inbox), media, tmp_path / "staging", ledger, cloud
    )

    assert duplicate.destination == first.destination
    assert cloud.archive == {first.destination: b"same media"}
    assert not media.exists()
    assert not list((tmp_path / "staging").rglob("photo.jpg"))
    assert [item.status for item in list_items(ledger)] == ["published", "published"]


def test_identical_content_from_different_sources_keeps_provenance(
    tmp_path: Path,
) -> None:
    cloud = FakeCloud()
    ledger = tmp_path / "ledger.sqlite"
    results = []
    for source_id in ("Phone-1", "Phone-2"):
        inbox = tmp_path / source_id
        inbox.mkdir()
        media = inbox / "photo.jpg"
        media.write_bytes(b"shared media")
        timestamp = datetime(2024, 1, 15, 12).timestamp()
        os.utime(media, (timestamp, timestamp))
        results.append(
            run_once(
                Source(source_id, inbox),
                media,
                tmp_path / "staging",
                ledger,
                cloud,
            )
        )

    assert results[0].destination != results[1].destination
    assert len(cloud.archive) == 2
    assert set(cloud.archive.values()) == {b"shared media"}
    assert [item.status for item in list_items(ledger)] == ["published", "published"]


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
        FakeMetadata(),
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
            FakeMetadata(),
        )
    assert cloud.archive[first.destination] == b"first"
    assert cloud.archive[second.destination] == b"second"


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
            FakeMetadata(),
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
        FakeMetadata(),
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
            FakeMetadata(),
        )
    assert (remote / result.destination).read_bytes() == b"actual original"
    assert (
        list((tmp_path / "staging").rglob("photo.jpg"))[0].read_bytes()
        == b"replacement"
    )
    assert (remote / result.destination).read_bytes() == b"actual original"
    assert (remote / replacement.destination).read_bytes() == b"replacement"
    assert replacement.destination != result.destination
    assert not list((tmp_path / "staging").rglob("photo.jpg"))
