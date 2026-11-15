# Project instructions

## Python style

- Target Python 3.12+ and annotate all function parameters and return values.
  Use built-in generics (`list[str]`, `dict[str, int]`) and `X | None`.
- Use `pathlib.Path` for paths. Use `@dataclass(frozen=True, slots=True)` for
  immutable value objects; make mutation explicit when it is required.
- Use `subprocess` argument lists with `shell=False`; use `check=True` with
  `subprocess.run`, and check the exit status when streaming with `Popen`.
  Raise or report failures with context; do not silently ignore them.

## Architecture

- Put the package under `src/`. Keep date parsing, destination-path calculation,
  and hash calculation in pure domain modules: pass values or byte chunks in,
  return values out, and inject time and timezone rather than reading the clock.
- Put ExifTool and rclone subprocesses, filesystem mutations, SQLite, and Azure
  operations in adapters. Domain modules must not import adapters or perform
  I/O. Orchestration may call adapters through typed interfaces.
- Test observable pipeline behavior with temporary directories and fake
  external adapters; tests must not require Azure credentials or live services.

## Security and data safety

- The Cloud archive is append-only: never overwrite or delete published media.
  Upload to a separate pending path, verify its bytes, then publish with an
  atomic no-replace operation. An existence check alone is not sufficient.
- Retain the local copy if transfer, verification, publication, or ledger
  recording fails or is inconclusive. Remove staged media only after verified
  publication is recorded in SQLite.
- Do not run `rclone sync` or `rclone delete`, or use `shutil.rmtree` on
  archive, pending-upload, or target paths. Use narrowly scoped cleanup only
  where the safety rules above permit it.
- Never hardcode credentials or SAS tokens. Keep secrets in a user-only file
  outside the repository; do not include them in logs, command arguments, or
  test fixtures. Do not log media contents, GPS data, or full EXIF metadata.

## Definition of done

After modifying code, configure the project and its development dependencies
for `uv`, Ruff, mypy, and pytest, then run all of the following from the
repository root. Fix failures before completion; report any blocked command.

```sh
uv run --extra dev ruff check .
uv run --extra dev ruff format --check .
uv run --extra dev mypy --strict src tests
uv run --extra dev pytest
```
