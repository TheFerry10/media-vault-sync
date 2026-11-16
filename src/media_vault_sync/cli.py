from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from azure.core.exceptions import AzureError

from .exiftool import ExifTool
from .pipeline import Source, list_failed_items, list_items, run_inbox, run_once


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Process Inbox media and manage archive retries"
    )
    parser.add_argument("--config", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("run-once")
    ingest.add_argument("source")
    ingest.add_argument("item", help="Path relative to the Source Inbox")
    retry = commands.add_parser("retry")
    retry.add_argument("source")
    retry.add_argument("item", help="Path relative to the Source Inbox")
    commands.add_parser("process")
    commands.add_parser("status")
    commands.add_parser("review")
    args = parser.parse_args()

    try:
        with args.config.open("rb") as stream:
            config = tomllib.load(stream)
        ledger = Path(config["ledger"])
        if args.command == "status":
            print(json.dumps([asdict(item) for item in list_items(ledger)], indent=2))
        elif args.command == "review":
            print(
                json.dumps(
                    [asdict(item) for item in list_failed_items(ledger)], indent=2
                )
            )
        else:
            from .azure import AzureFiles

            azure = config["azure"]
            cloud = AzureFiles(
                azure["account"], azure["share"], Path(azure["credentials"])
            )
            metadata = ExifTool()
            if args.command == "process":
                now = datetime.now(timezone.utc)
                results = [
                    result
                    for source_id, inbox_path in config["sources"].items()
                    for result in run_inbox(
                        Source(source_id, Path(inbox_path)),
                        Path(config["staging"]),
                        ledger,
                        cloud,
                        metadata,
                        now=now,
                    )
                ]
                print(json.dumps([asdict(result) for result in results], indent=2))
            else:
                inbox = Path(config["sources"][args.source])
                if Path(args.item).is_absolute():
                    raise ValueError("Item path must be relative to the Source Inbox")
                result = run_once(
                    Source(args.source, inbox),
                    inbox / args.item,
                    Path(config["staging"]),
                    ledger,
                    cloud,
                    metadata,
                    now=datetime.now(timezone.utc),
                    retry=args.command == "retry",
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
