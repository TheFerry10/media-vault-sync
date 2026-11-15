from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from dataclasses import asdict
from pathlib import Path

from azure.core.exceptions import AzureError

from .pipeline import Source, list_items, run_once


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish one media item to the append-only archive")
    parser.add_argument("--config", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("run-once")
    ingest.add_argument("source")
    ingest.add_argument("item", help="Path relative to the Source Inbox")
    commands.add_parser("status")
    args = parser.parse_args()

    try:
        with args.config.open("rb") as stream:
            config = tomllib.load(stream)
        ledger = Path(config["ledger"])
        if args.command == "status":
            print(json.dumps([asdict(item) for item in list_items(ledger)], indent=2))
        else:
            from .azure import AzureFiles

            inbox = Path(config["sources"][args.source])
            if Path(args.item).is_absolute():
                raise ValueError("Item path must be relative to the Source Inbox")
            azure = config["azure"]
            cloud = AzureFiles(azure["account"], azure["share"], Path(azure["credentials"]))
            result = run_once(
                Source(args.source, inbox), inbox / args.item, Path(config["staging"]), ledger, cloud
            )
            print(json.dumps(asdict(result)))
        return 0
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError, AzureError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
