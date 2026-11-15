from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

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


class Status(StrEnum):
    STAGING = "staging"
    PENDING = "pending"
    FAILED = "failed"
    PUBLISHED = "published"


@dataclass(frozen=True, slots=True)
class Source:
    id: str
    inbox: Path


@dataclass(frozen=True, slots=True)
class Result:
    status: Status
    destination: str


@dataclass(frozen=True, slots=True)
class LedgerItem:
    source_id: str
    destination: str
    staged_path: str
    pending_path: str
    status: Status
    error: str | None


def sha256(chunks: Iterable[bytes]) -> bytes:
    digest = hashlib.sha256()
    for chunk in chunks:
        digest.update(chunk)
    return digest.digest()


def validate_source_id(source_id: str) -> None:
    if not source_id.strip():
        raise ValueError("Source ID must not be empty")


def archive_destination(source_id: str, filename: str, capture_time: datetime) -> str:
    if Path(filename).suffix.lower() not in MEDIA_EXTENSIONS:
        raise ValueError("Unsupported media extension")
    if any(ord(c) < 32 or c in '"*:<>?\\|' for c in filename):
        raise ValueError("Filename contains characters unsupported by Azure Files")
    source_hash = sha256([source_id.encode("utf-8")]).hex()[:12]
    name = f"{capture_time:%Y%m%d_%H%M%S}_{source_hash}_{filename}"
    if len(name.encode("utf-16-le")) // 2 > 255:
        raise ValueError("Archive filename exceeds Azure Files' 255-character limit")
    return f"{capture_time:%Y/%Y-%m}/{name}"
