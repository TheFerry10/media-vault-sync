# Media Vault Sync

Media Vault Sync archives media to an SMB Azure Files share. It can publish one
manually selected file or poll configured Inboxes for stable files. A file must
have the same size and modification time in two consecutive polls before it is
processed. Stable files that remain in an Inbox are retried by later polls.

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

On failure, inspect `status`: it includes the staged and pending paths and an
error. Staged data and pending uploads are intentionally retained. Do not
manually delete the only remaining copy; automated retries, orphan cleanup and
crash reconciliation are not yet implemented. A crash or uncertain rename
response may leave a published item with a non-published ledger status; review
both paths before taking action. Manual SMB changes are outside the automated
append-only guarantee. Azure cost validation is separate work.

## Development

CI runs the lint, formatting, type, and test checks on pull requests and
pushes to `main`. Run the same checks locally:

```sh
uv run --extra dev pytest
uv run --extra dev ruff check .
uv run --extra dev ruff format --check .
uv run --extra dev mypy --strict src tests
```
