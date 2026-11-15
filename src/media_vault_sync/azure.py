from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

from azure.identity import ClientSecretCredential
from azure.storage.fileshare import ShareFileClient


def load_credentials(path: Path) -> tuple[str, str, str]:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "r", encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
        ):
            raise ValueError(
                "Credential file must be owned by the current user and mode 0600 or stricter"
            )
        data = json.load(stream)
    if not isinstance(data, dict):
        raise ValueError("Credential file must contain a JSON object")
    tenant = data.get("tenant_id")
    client = data.get("client_id")
    secret = data.get("client_secret")
    if not (
        isinstance(tenant, str)
        and tenant
        and isinstance(client, str)
        and client
        and isinstance(secret, str)
        and secret
    ):
        raise ValueError(
            "Credential file must contain tenant_id, client_id and client_secret"
        )
    return tenant, client, secret


class AzureFiles:
    def __init__(self, account: str, share: str, credentials: Path) -> None:
        if not re.fullmatch(r"[a-z0-9]{3,24}", account):
            raise ValueError("Invalid Azure storage account name")
        if (
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])", share)
            or "--" in share
        ):
            raise ValueError("Invalid Azure Files share name")
        if shutil.which("rclone") is None:
            raise RuntimeError("rclone is required but was not found on PATH")
        tenant, client, secret = load_credentials(credentials)
        self.account = account
        self.share = share
        self.credential = ClientSecretCredential(tenant, client, secret)
        self.env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("RCLONE_")
        }
        self.env.update(
            {
                "AZURE_TENANT_ID": tenant,
                "AZURE_CLIENT_ID": client,
                "AZURE_CLIENT_SECRET": secret,
                "RCLONE_AZUREFILES_ACCOUNT": account,
                "RCLONE_AZUREFILES_SHARE_NAME": share,
                "RCLONE_AZUREFILES_ENV_AUTH": "true",
            }
        )

    def _command(self, *args: str) -> list[str]:
        return ["rclone", "--config", "/dev/null", *args]

    def upload(self, path: Path, pending: str) -> None:
        subprocess.run(
            self._command("copyto", str(path), f":azurefiles:{pending}"),
            env=self.env,
            check=True,
        )

    def read_pending(self, pending: str) -> Iterator[bytes]:
        with tempfile.TemporaryFile() as errors:
            with subprocess.Popen(
                self._command("cat", f":azurefiles:{pending}"),
                env=self.env,
                stdout=subprocess.PIPE,
                stderr=errors,
            ) as process:
                output = process.stdout
                assert output is not None
                yield from iter(lambda: output.read(1024 * 1024), b"")
                if process.wait():
                    errors.seek(0)
                    raise RuntimeError(
                        f"rclone read-back failed: {errors.read().decode(errors='replace')}"
                    )

    def publish(self, pending: str, destination: str) -> None:
        directory = destination.rsplit("/", 1)[0]
        subprocess.run(
            self._command("mkdir", f":azurefiles:{directory}"),
            env=self.env,
            check=True,
        )
        with ShareFileClient(
            account_url=f"https://{self.account}.file.core.windows.net",
            share_name=self.share,
            file_path=pending,
            credential=self.credential,
            token_intent="backup",
        ) as client:
            client.rename_file(destination, overwrite=False)
