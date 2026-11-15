from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from time import localtime
from typing import Protocol

from . import filesystem
from .domain import (
    MEDIA_EXTENSIONS,
    Result,
    Source,
    Status,
    archive_destination,
    capture_time,
    sha256,
    validate_source_id,
)
from .ledger import Ledger, list_items

__all__ = ["Cloud", "Result", "Source", "list_items", "run_once"]


class Cloud(Protocol):
    def upload(self, path: Path, pending: str) -> None: ...

    def read_pending(self, pending: str) -> Iterator[bytes]: ...

    def publish(self, pending: str, destination: str) -> None: ...


def run_once(
    source: Source, item: Path, staging: Path, ledger: Path, cloud: Cloud
) -> Result:
    validate_source_id(source.id)
    timestamp = capture_time(filesystem.modification_time(source, item), localtime)
    if item.suffix.lower() not in MEDIA_EXTENSIONS:
        raise ValueError("Unsupported media extension")
    sidecars = filesystem.associated_sidecars(source, item)
    destination = archive_destination(source.id, item.name, timestamp)
    sidecar_destinations = [
        (sidecar, archive_destination(source.id, sidecar.name, timestamp))
        for sidecar in sidecars
    ]

    _publish_item(source, item, destination, staging, ledger, cloud)
    for sidecar, sidecar_destination in sidecar_destinations:
        filesystem.modification_time(source, sidecar)
        _publish_item(source, sidecar, sidecar_destination, staging, ledger, cloud)
    return Result(Status.PUBLISHED, destination)


def _publish_item(
    source: Source,
    item: Path,
    destination: str,
    staging: Path,
    ledger: Path,
    cloud: Cloud,
) -> None:
    run_id = uuid.uuid4().hex
    pending = f"_pending/{run_id}/data"
    staged = filesystem.prepare_stage(staging, run_id, item.name)
    with Ledger(ledger) as db:
        db.start(run_id, source, destination, staged, pending)
        try:
            filesystem.stage_item(item, staged)
            db.update(run_id, Status.PENDING)
            local_hash = filesystem.file_hash(staged)
            cloud.upload(staged, pending)
            if sha256(cloud.read_pending(pending)) != local_hash:
                raise ValueError("Pending upload verification failed")
            if filesystem.file_hash(staged) != local_hash:
                raise ValueError("Staged file changed during upload")
            cloud.publish(pending, destination)
        except Exception as exc:
            db.update(run_id, Status.FAILED, str(exc))
            raise
        db.update(run_id, Status.PUBLISHED)
        if filesystem.file_hash(staged) != local_hash:
            raise ValueError("Staged file changed after publication; retained locally")
        filesystem.cleanup_stage(staged)
