from __future__ import annotations

import hashlib
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
        ".avif",
        ".bmp",
        ".jxl",
        ".tif",
        ".tiff",
        ".3fr",
        ".ari",
        ".bay",
        ".cap",
        ".dcs",
        ".dcr",
        ".drf",
        ".dng",
        ".eip",
        ".erf",
        ".fff",
        ".gpr",
        ".iiq",
        ".k25",
        ".kdc",
        ".mdc",
        ".mef",
        ".mos",
        ".mrw",
        ".nrw",
        ".obm",
        ".pef",
        ".ptx",
        ".pxn",
        ".r3d",
        ".raw",
        ".cr2",
        ".cr3",
        ".nef",
        ".arw",
        ".raf",
        ".orf",
        ".rw2",
        ".rwz",
        ".sr2",
        ".srf",
        ".srw",
        ".sti",
        ".x3f",
        ".mp4",
        ".mov",
        ".m4v",
        ".avi",
        ".mkv",
        ".3gp",
        ".3g2",
        ".flv",
        ".m2v",
        ".mts",
        ".m2ts",
        ".mpe",
        ".mpeg",
        ".mpg",
        ".mpv",
        ".mxf",
        ".ogv",
        ".ts",
        ".vob",
        ".webm",
        ".wmv",
    }
)
SIDECAR_EXTENSIONS = frozenset({".aae", ".thm", ".xmp"})


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


def archive_destination(source_id: str, filename: str, capture_time: datetime) -> str:
    if Path(filename).suffix.lower() not in MEDIA_EXTENSIONS | SIDECAR_EXTENSIONS:
        raise ValueError("Unsupported media extension")
    if any(ord(c) < 32 or c in '"*:<>?\\|' for c in filename):
        raise ValueError("Filename contains characters unsupported by Azure Files")
    source_hash = sha256([source_id.encode("utf-8")]).hex()[:12]
    name = f"{capture_time:%Y%m%d_%H%M%S}_{source_hash}_{filename}"
    if len(name.encode("utf-16-le")) // 2 > 255:
        raise ValueError("Archive filename exceeds Azure Files' 255-character limit")
    return f"{capture_time:%Y/%Y-%m}/{name}"
