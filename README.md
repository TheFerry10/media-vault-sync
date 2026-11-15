# Media Vault Sync

This initial slice manually archives one completed media file at a time to an
SMB Azure Files share. It does not yet monitor Inboxes, detect stable files,
retry failures, deduplicate, resolve metadata dates, or recover after crashes.
Only run it on a file whose transfer into the Inbox has finished. Do not edit
files in Staging while the pipeline is running.

## Setup

Install Python 3.12+, [uv](https://docs.astral.sh/uv/) and rclone 1.65 or newer
with the Azure Files backend. Run `uv sync --extra dev` in this directory.

Create a service principal with Azure Files data permissions including backup
semantics (for example, Storage File Data Privileged Contributor) on the share.
Create a JSON credentials file **outside this repository**, owned by your user
and readable only by that user (`chmod 600`):

```json
{"tenant_id": "TENANT_ID", "client_id": "CLIENT_ID", "client_secret": "SECRET"}
```

Create a TOML configuration file outside the repository, using absolute paths:

```toml
staging = "/home/you/media-staging"
ledger = "/home/you/media-state/ledger.sqlite"

[sources]
Phone-1 = "/home/you/media-inbox/phone-1"
Phone-2 = "/home/you/media-inbox/phone-2"

[azure]
account = "yourstorageaccount"
share = "yourfileshare"
credentials = "/home/you/.config/media-vault-sync/credentials.json"
```

Source IDs are stable identifiers, independent of Inbox paths. Configure the
same Azure account and share for pending and final files. Use an SMB share;
Azure's no-replace rename API is not available on NFS shares.

## Use

```sh
uv run media-vault-sync --config /path/to/config.toml run-once Phone-1 photo.jpg
uv run media-vault-sync --config /path/to/config.toml status
```

The item path is relative to the selected Inbox. Supported photo/video
extensions are listed in the pipeline; other files remain untouched. This
slice uses the file's modification time in the host's local timezone and
creates `YYYY/YYYY-MM/<timestamp>_<source-hash>_<original-name>`.

The item moves to a unique local Staging directory. rclone uploads it to a
separate `_pending/` path, then reads it back for SHA-256 verification. Azure
Files performs publication using an atomic server-side rename with replacement
disabled. A destination conflict fails rather than overwriting. The ledger
records publication before Staging is cleaned.

On failure, inspect `status`: it includes the staged and pending paths and an
error. Staged data and pending uploads are intentionally retained. Do not
manually delete the only remaining copy; automated retries and orphan cleanup
are not yet implemented. At the start of the next run,
the pipeline checks unresolved ledger entries against their final Cloud archive
objects and marks a matching object published before cleaning up its verified
Staging copy. If the final object cannot be verified, the local copy is retained.

After each run, the SQLite ledger is copied to `_ledger/ledger.sqlite`, separate
from media objects. Each refresh uploads and verifies a uniquely named candidate
before replacing the prior backup. If refresh fails, the previous backup stays
intact and the operation reports `backup_stale`; media processing is not halted.
Manual SMB changes are outside the automated append-only guarantee. Validate
actual Azure costs before provisioning recovery protection.

## Development

CI runs the lint, formatting, type, and test checks on pull requests and
pushes to `main`. Run the same checks locally:

```sh
uv run --extra dev pytest
uv run --extra dev ruff check .
uv run --extra dev ruff format --check .
uv run --extra dev mypy --strict src tests
```
