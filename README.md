# Media Vault Sync

Media Vault Sync archives completed media files from configured Inboxes to an
SMB Azure Files share. It supports individual runs, batch processing, and
scheduled polling of stable Inbox files. Only process files whose transfer into
the Inbox has finished. Do not edit files in Staging while the pipeline is
running.

## Setup

Install Python 3.12+, [uv](https://docs.astral.sh/uv/), ExifTool, and rclone
1.65 or newer with the Azure Files backend. Run `uv sync --extra dev` in this
directory.

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
ledger_backup = "/home/you/media-state/ledger.sqlite.backup"
ledger_backup_max_age_hours = 24

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
uv run media-vault-sync --config /path/to/config.toml poll
uv run media-vault-sync --config /path/to/config.toml process
uv run media-vault-sync --config /path/to/config.toml status
uv run media-vault-sync --config /path/to/config.toml review
uv run media-vault-sync --config /path/to/config.toml retry Phone-1 photo.jpg
```

The item path is relative to the selected Inbox. Supported photo/video
extensions are listed in the pipeline; other files remain untouched. Capture
time prefers photo `DateTimeOriginal` / `CreateDate` tags and video
`MediaCreateDate` / `CreateDate` tags. Invalid or missing metadata falls back to
recognized WhatsApp filename dates and then the file's modification time in the
host's local timezone. Source timestamps are used as recorded without timezone
conversion; WhatsApp date-only filenames use `000000` for the unknown time. The
selected timestamp and source are recorded in the ledger. The pipeline creates
`YYYY/YYYY-MM/<timestamp>_<source-hash>_<original-name>`.

The item is copied to a unique local Staging directory, leaving the Inbox copy
in place until publication is verified and recorded. rclone uploads the staged
copy to a separate `_pending/` path, then reads it back for SHA-256 verification.
Azure Files performs publication using an atomic server-side rename with
replacement disabled. Re-imports are deduplicated only within the same Source
after the ledger confirms the earlier item was published. A destination
collision is resolved with a content-hash suffix rather than overwriting. After
successful publication, the ledger records it before the Inbox and Staging
copies are cleaned.

## Automatic polling on Ubuntu

Install the package as a user tool so systemd can invoke the executable:

```sh
uv tool install --from /path/to/media-vault-sync media-vault-sync
mkdir -p ~/.config/systemd/user
cp /path/to/media-vault-sync/systemd/user/media-vault-sync.{service,timer} \
  ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now media-vault-sync.timer
```

The timer polls every 15 minutes. The poll command uses an exclusive local lock,
so timer and manual polls cannot overlap. It records file size and nanosecond
mtime in the ledger and only processes an unchanged file on its second
consecutive poll. The ledger backup is refreshed if missing or older than the
configured maximum age (24 hours by default).

Run details and actionable errors are written to standard output/error, which
the service sends to journald. Inspect them with
`journalctl --user -u media-vault-sync.service`. Permanent or unknown stable
items and stale ledger backups trigger a local desktop notification; transient
failures trigger one after 24 hours. Notifications never include filenames,
media contents, or metadata. Desktop notifications require an active local
graphical session and `notify-send`.

`process` attempts every eligible media file in all configured Inboxes; a
failure on one file does not stop the rest. Transient transfer failures retry
when `process` is run again after an increasing delay (15 minutes initially,
capped at 6 hours). Permanent failures remain in the Inbox and are suppressed
until explicitly retried with `retry`. Use `review` to see the latest failed
items and their reasons; `status` shows the full ledger journal. Staged data and
pending uploads are intentionally retained after failure. Do not manually
delete the only remaining copy. A crash or uncertain rename response may leave
a published item with a non-published ledger status.

At the start of a run, the pipeline checks unresolved ledger entries against
their final Cloud archive objects and marks a matching object published before
cleaning up its verified Staging copy. If the final object cannot be verified,
the local copy is retained. Manual SMB changes are outside the automated
append-only guarantee.

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
