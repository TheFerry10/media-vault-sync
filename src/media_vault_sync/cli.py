from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import tomllib
from dataclasses import asdict
from pathlib import Path

from azure.core.exceptions import AzureError

from .exiftool import ExifTool
from .locking import exclusive_lock
from .pipeline import Source, list_items, run_once
from .poll import PollConfig, poll_once


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish one media item to the append-only archive"
    )
    parser.add_argument("--config", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("run-once")
    ingest.add_argument("source")
    ingest.add_argument("item", help="Path relative to the Source Inbox")
    commands.add_parser("poll")
    commands.add_parser("status")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        with args.config.open("rb") as stream:
            config = tomllib.load(stream)
        ledger = Path(config["ledger"])
        if args.command == "status":
            print(json.dumps([asdict(item) for item in list_items(ledger)], indent=2))
        else:
            from .azure import AzureFiles

            azure = config["azure"]
            cloud = AzureFiles(
                azure["account"], azure["share"], Path(azure["credentials"])
            )
            if args.command == "poll":
                backup = Path(config.get("ledger_backup", f"{ledger}.backup"))
                max_age = float(config.get("ledger_backup_max_age_hours", 24)) * 3600
                summary = poll_once(
                    PollConfig(
                        {name: Path(path) for name, path in config["sources"].items()},
                        Path(config["staging"]),
                        ledger,
                        backup,
                        max_age,
                    ),
                    cloud,
                )
                print(json.dumps(asdict(summary)))
            else:
                inbox = Path(config["sources"][args.source])
                if Path(args.item).is_absolute():
                    raise ValueError("Item path must be relative to the Source Inbox")
                lock_path = ledger.with_suffix(ledger.suffix + ".lock")
                with exclusive_lock(lock_path) as acquired:
                    if not acquired:
                        raise RuntimeError("Another pipeline run is already active")
                    result = run_once(
                        Source(args.source, inbox),
                        inbox / args.item,
                        Path(config["staging"]),
                        ledger,
                        cloud,
                        ExifTool(),
                    )
                print(json.dumps(asdict(result)))
        return 0
    except (
        OSError,
        ValueError,
        KeyError,
        RuntimeError,
        subprocess.SubprocessError,
        AzureError,
    ) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
