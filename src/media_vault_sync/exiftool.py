from __future__ import annotations

import json
import subprocess
from pathlib import Path


def _sanitized_diagnostic(stderr: str | None, path: Path) -> str:
    if stderr is None:
        return "no diagnostic output"
    diagnostic = next(
        (line.strip() for line in stderr.splitlines() if line.strip()), ""
    )
    diagnostic = diagnostic.replace(str(path), "<media file>").replace(
        path.name, "<media file>"
    )
    diagnostic = " ".join(
        "".join(
            character for character in diagnostic if character.isprintable()
        ).split()
    )
    return diagnostic[:200] or "no diagnostic output"


class ExifTool:
    def __init__(self, executable: str = "exiftool") -> None:
        self.executable = executable

    def extract(self, path: Path) -> dict[str, str]:
        try:
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
        except subprocess.CalledProcessError as exc:
            diagnostic = exc.stderr if isinstance(exc.stderr, str) else None
            raise RuntimeError(
                f"ExifTool failed with exit status {exc.returncode}: "
                f"{_sanitized_diagnostic(diagnostic, path)}"
            ) from exc
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
