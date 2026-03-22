"""
storage/gcs.py — Google Cloud Storage backend.

Handles gs:// URI scheme.
URI format: gs://bucket/path/to/object
"""

from __future__ import annotations

import logging
from typing import Iterator, Optional
from urllib.parse import urlparse

from iceberg_sync.storage.base import FileInfo, StorageBackend

log = logging.getLogger(__name__)


class GCSStorageBackend(StorageBackend):
    """
    Storage backend for Google Cloud Storage.

    Authentication: uses Application Default Credentials (ADC).
    Set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`.
    """

    def __init__(self, project: Optional[str] = None):
        from google.cloud import storage as gcs_storage
        self._client = gcs_storage.Client(project=project)

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, str]:
        """Parse gs://bucket/key into (bucket, key)."""
        parsed = urlparse(uri)
        return parsed.netloc, parsed.path.lstrip("/")

    @staticmethod
    def _build_uri(bucket: str, key: str) -> str:
        return f"gs://{bucket}/{key}"

    def list_objects(self, prefix: str) -> Iterator[FileInfo]:
        bucket_name, key_prefix = self._parse_uri(prefix)
        bucket = self._client.bucket(bucket_name)

        for blob in bucket.list_blobs(prefix=key_prefix):
            full_uri = self._build_uri(bucket_name, blob.name)
            relative = blob.name[len(key_prefix):]
            yield FileInfo(
                uri=full_uri,
                relative_path=relative,
                size_bytes=blob.size or 0,
                etag=blob.etag,
            )

    def read_bytes(self, uri: str) -> bytes:
        bucket_name, key = self._parse_uri(uri)
        blob = self._client.bucket(bucket_name).blob(key)
        return blob.download_as_bytes()

    def write_bytes(self, uri: str, data: bytes) -> None:
        bucket_name, key = self._parse_uri(uri)
        blob = self._client.bucket(bucket_name).blob(key)
        blob.upload_from_string(data)

    def exists(self, uri: str) -> bool:
        bucket_name, key = self._parse_uri(uri)
        return self._client.bucket(bucket_name).blob(key).exists()

    def delete(self, uri: str) -> None:
        bucket_name, key = self._parse_uri(uri)
        self._client.bucket(bucket_name).blob(key).delete()

    def copy_from(self, source_backend: StorageBackend, source_uri: str, target_uri: str) -> None:
        data = source_backend.read_bytes(source_uri)
        self.write_bytes(target_uri, data)
