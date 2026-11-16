from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from time import struct_time

MEDIA_EXTENSIONS = frozenset(
    {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".heic",
        ".heif",
        ".tif",
        ".tiff",
        ".dng",
        ".cr2",
        ".cr3",
        ".nef",
        ".arw",
        ".raf",
        ".orf",
        ".rw2",
        ".mp4",
        ".mov",
        ".m4v",
        ".avi",
        ".mkv",
        ".3gp",
        ".mts",
        ".m2ts",
    }
)
VIDEO_EXTENSIONS = frozenset(
    {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".3gp", ".mts", ".m2ts"}
)
PHOTO_EXTENSIONS = MEDIA_EXTENSIONS - VIDEO_EXTENSIONS

_WHATSAPP_TIMESTAMP = re.compile(
    r"WhatsApp (?:Image|Video) "
    r"(?P<date>\d{4}-\d{2}-\d{2}) at "
    r"(?P<hour>\d{2})\.(?P<minute>\d{2})\.(?P<second>\d{2})",
    re.IGNORECASE,
)
_WHATSAPP_DATE = re.compile(
    r"(?:IMG|VID)-(?P<date>\d{4}\d{2}\d{2})-WA\d{4}(?: \(\d+\))?",
    re.IGNORECASE,
)
_WHATSAPP_IMAGE_DATE = re.compile(
    r"WhatsApp (?:Image|Video) (?P<date>\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)


class Status(StrEnum):
    STAGING = "staging"
    PENDING = "pending"
    FAILED = "failed"
    PUBLISHED = "published"
    SKIPPED = "skipped"


class FailureKind(StrEnum):
    PERMANENT = "permanent"
    TRANSIENT = "transient"


class TransientTransferError(RuntimeError):
    pass


class CaptureTimeSource(StrEnum):
    DATETIME_ORIGINAL = "DateTimeOriginal"
    MEDIA_CREATE_DATE = "MediaCreateDate"
    CREATE_DATE = "CreateDate"
    WHATSAPP_FILENAME = "WhatsApp filename"
    FILESYSTEM_MTIME = "filesystem mtime"


@dataclass(frozen=True, slots=True)
class Source:
    id: str
    inbox: Path


@dataclass(frozen=True, slots=True)
class Result:
    status: Status
    destination: str
    error: str | None = None


@dataclass(frozen=True, slots=True)
class LedgerItem:
    source_id: str
    source_path: str
    destination: str
    staged_path: str
    pending_path: str
    status: Status
    error: str | None
    failure_kind: FailureKind | None
    attempt_count: int
    next_retry_at: str | None
    capture_time: str | None
    capture_time_source: CaptureTimeSource | None


@dataclass(frozen=True, slots=True)
class CaptureTime:
    value: datetime
    source: CaptureTimeSource


def sha256(chunks: Iterable[bytes]) -> bytes:
    digest = hashlib.sha256()
    for chunk in chunks:
        digest.update(chunk)
    return digest.digest()


def validate_source_id(source_id: str) -> None:
    if not source_id.strip():
        raise ValueError("Source ID must not be empty")


def capture_time(
    timestamp: float, local_time: Callable[[float], struct_time]
) -> datetime:
    parts = local_time(timestamp)
    return datetime(
        parts.tm_year,
        parts.tm_mon,
        parts.tm_mday,
        parts.tm_hour,
        parts.tm_min,
        parts.tm_sec,
    )


def parse_source_timestamp(value: str) -> datetime | None:
    normalized = value.strip()
    if len(normalized) >= 10 and normalized[4] == ":" and normalized[7] == ":":
        normalized = f"{normalized[:4]}-{normalized[5:7]}-{normalized[8:]}"
    if len(normalized) > 10 and normalized[10] == " ":
        normalized = f"{normalized[:10]}T{normalized[11:]}"
    try:
        return datetime.fromisoformat(normalized).replace(tzinfo=None)
    except ValueError:
        return None


def whatsapp_filename_time(filename: str) -> datetime | None:
    stem = Path(filename).stem
    match = _WHATSAPP_TIMESTAMP.fullmatch(stem)
    if match is not None:
        try:
            return datetime.strptime(
                f"{match['date']} {match['hour']}:{match['minute']}:{match['second']}",
                "%Y-%m-%d %H:%M:%S",
            )
        except ValueError:
            return None

    match = _WHATSAPP_DATE.fullmatch(stem)
    date_format = "%Y%m%d"
    if match is None:
        match = _WHATSAPP_IMAGE_DATE.fullmatch(stem)
        date_format = "%Y-%m-%d"
    if match is not None:
        try:
            return datetime.strptime(match["date"], date_format)
        except ValueError:
            return None
    return None


def select_capture_time(
    filename: str, tags: dict[str, str], modified_at: datetime
) -> CaptureTime:
    extension = Path(filename).suffix.lower()
    if extension in VIDEO_EXTENSIONS:
        candidates = (
            ("MediaCreateDate", CaptureTimeSource.MEDIA_CREATE_DATE),
            ("CreateDate", CaptureTimeSource.CREATE_DATE),
        )
    elif extension in PHOTO_EXTENSIONS:
        candidates = (
            ("DateTimeOriginal", CaptureTimeSource.DATETIME_ORIGINAL),
            ("CreateDate", CaptureTimeSource.CREATE_DATE),
        )
    else:
        raise ValueError("Unsupported media extension")

    for tag, source in candidates:
        value = tags.get(tag)
        if value is not None:
            parsed = parse_source_timestamp(value)
            if parsed is not None:
                return CaptureTime(parsed, source)

    filename_time = whatsapp_filename_time(filename)
    if filename_time is not None:
        return CaptureTime(filename_time, CaptureTimeSource.WHATSAPP_FILENAME)
    return CaptureTime(modified_at, CaptureTimeSource.FILESYSTEM_MTIME)


def archive_destination(
    source_id: str,
    filename: str,
    capture_time: datetime,
    content_hash: bytes | None = None,
) -> str:
    if Path(filename).suffix.lower() not in MEDIA_EXTENSIONS:
        raise ValueError("Unsupported media extension")
    if any(ord(c) < 32 or c in '"*:<>?\\|' for c in filename):
        raise ValueError("Filename contains characters unsupported by Azure Files")
    source_hash = sha256([source_id.encode("utf-8")]).hex()[:12]
    prefix = f"{capture_time:%Y%m%d_%H%M%S}_{source_hash}_"
    if content_hash is None:
        name = f"{prefix}{filename}"
    else:
        if len(content_hash) != 32:
            raise ValueError("Content hash must be a SHA-256 digest")
        suffix = f"_{content_hash.hex()}{Path(filename).suffix}"
        stem = Path(filename).stem
        max_stem_length = (
            255
            - len(prefix.encode("utf-16-le")) // 2
            - len(suffix.encode("utf-16-le")) // 2
        )
        if max_stem_length < 0:
            raise ValueError(
                "Archive filename exceeds Azure Files' 255-character limit"
            )
        while len(stem.encode("utf-16-le")) // 2 > max_stem_length:
            stem = stem[:-1]
        name = f"{prefix}{stem}{suffix}"
    if len(name.encode("utf-16-le")) // 2 > 255:
        raise ValueError("Archive filename exceeds Azure Files' 255-character limit")
    return f"{capture_time:%Y/%Y-%m}/{name}"
