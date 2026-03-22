"""
storage/adls.py — Azure Data Lake Storage Gen2 backend.

Handles abfss:// and abfs:// URI schemes.
URI format: abfss://container@account.dfs.core.windows.net/path/to/object
"""

from __future__ import annotations

import logging
import re
from typing import Iterator, Optional

from iceberg_sync.storage.base import FileInfo, StorageBackend

log = logging.getLogger(__name__)

# Regex to parse abfss://container@account.dfs.core.windows.net/path
_ADLS_URI_RE = re.compile(
    r"^abfss?://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net/(?P<path>.*)"
)


class ADLSStorageBackend(StorageBackend):
    """
    Storage backend for Azure Data Lake Storage Gen2 (ADLS).

    Authentication:
    •  Default: uses DefaultAzureCredential (managed identity, az login, env vars)
    •  Connection string: pass storage_connection_string
    •  Account key: pass storage_account_key
    """

    def __init__(
        self,
        storage_account_name: Optional[str] = None,
        storage_account_key: Optional[str] = None,
        storage_connection_string: Optional[str] = None,
    ):
        from azure.storage.blob import BlobServiceClient

        if storage_connection_string:
            self._client = BlobServiceClient.from_connection_string(storage_connection_string)
        elif storage_account_key:
            url = f"https://{storage_account_name}.blob.core.windows.net"
            self._client = BlobServiceClient(url, credential=storage_account_key)
        else:
            from azure.identity import DefaultAzureCredential
            url = f"https://{storage_account_name}.blob.core.windows.net"
            self._client = BlobServiceClient(url, credential=DefaultAzureCredential())

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, str, str]:
        """Parse abfss:// URI into (account, container, path)."""
        m = _ADLS_URI_RE.match(uri)
        if not m:
            raise ValueError(
                f"Invalid ADLS URI: '{uri}'. "
                f"Expected format: abfss://container@account.dfs.core.windows.net/path"
            )
        return m.group("account"), m.group("container"), m.group("path")

    @staticmethod
    def _build_uri(account: str, container: str, path: str) -> str:
        return f"abfss://{container}@{account}.dfs.core.windows.net/{path}"

    def _container_client(self, container: str):
        return self._client.get_container_client(container)

    def list_objects(self, prefix: str) -> Iterator[FileInfo]:
        account, container, path_prefix = self._parse_uri(prefix)
        cc = self._container_client(container)

        for blob in cc.list_blobs(name_starts_with=path_prefix):
            full_uri = self._build_uri(account, container, blob.name)
            relative = blob.name[len(path_prefix):]
            yield FileInfo(
                uri=full_uri,
                relative_path=relative,
                size_bytes=blob.size,
                etag=blob.etag,
            )

    def read_bytes(self, uri: str) -> bytes:
        _, container, path = self._parse_uri(uri)
        blob_client = self._container_client(container).get_blob_client(path)
        return blob_client.download_blob().readall()

    def write_bytes(self, uri: str, data: bytes) -> None:
        _, container, path = self._parse_uri(uri)
        blob_client = self._container_client(container).get_blob_client(path)
        blob_client.upload_blob(data, overwrite=True)

    def exists(self, uri: str) -> bool:
        _, container, path = self._parse_uri(uri)
        blob_client = self._container_client(container).get_blob_client(path)
        try:
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    def delete(self, uri: str) -> None:
        _, container, path = self._parse_uri(uri)
        blob_client = self._container_client(container).get_blob_client(path)
        blob_client.delete_blob()

    def copy_from(self, source_backend: StorageBackend, source_uri: str, target_uri: str) -> None:
        data = source_backend.read_bytes(source_uri)
        self.write_bytes(target_uri, data)
