from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

TRANSIENT_NOTIFICATION_DELAY = 24 * 60 * 60


@dataclass(frozen=True, slots=True)
class Snapshot:
    size: int
    mtime_ns: int


class FailureKind(StrEnum):
    PERMANENT = "permanent"
    TRANSIENT = "transient"
    UNKNOWN = "unknown"


def notification_due(kind: FailureKind, failure_since: float, now: float) -> bool:
    if kind is FailureKind.TRANSIENT:
        return now - failure_since >= TRANSIENT_NOTIFICATION_DELAY
    return True


def backup_is_stale(backup_mtime: float | None, now: float, max_age: float) -> bool:
    return backup_mtime is None or now - backup_mtime > max_age
