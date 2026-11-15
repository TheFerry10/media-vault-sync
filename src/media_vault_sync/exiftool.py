from __future__ import annotations

import json
import subprocess
from pathlib import Path


class ExifTool:
    def __init__(self, executable: str = "exiftool") -> None:
        self.executable = executable

    def extract(self, path: Path) -> dict[str, str]:
        result = subprocess.run(
            [
                self.executable,
                "-json",
                "-DateTimeOriginal",
                "-MediaCreateDate",
                "-CreateDate",
                "--",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        records = json.loads(result.stdout)
        if (
            not isinstance(records, list)
            or not records
            or not isinstance(records[0], dict)
        ):
            raise ValueError("ExifTool returned an invalid metadata response")
        return {
            tag: value
            for tag in ("DateTimeOriginal", "MediaCreateDate", "CreateDate")
            if isinstance((value := records[0].get(tag)), str)
        }
