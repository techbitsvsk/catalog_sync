"""
storage/s3.py — S3 and S3-compatible (MinIO) storage backend.
"""

from __future__ import annotations

import logging
from typing import Iterator, Optional
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from iceberg_sync.storage.base import FileInfo, StorageBackend

log = logging.getLogger(__name__)


class S3StorageBackend(StorageBackend):
    """
    Storage backend for AWS S3 and S3-compatible stores (MinIO, Ceph, etc.).

    Handles both s3:// and s3a:// URI schemes — they map to the same bucket/key
    structure; the scheme difference only matters to Hadoop's FileSystem layer.
    """

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        region_name: str = "eu-west-2",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        kwargs = {"region_name": region_name}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if aws_access_key_id:
            kwargs["aws_access_key_id"] = aws_access_key_id
            kwargs["aws_secret_access_key"] = aws_secret_access_key

        self._s3 = boto3.client("s3", **kwargs, config=BotoConfig(
            retries={"max_attempts": 3, "mode": "adaptive"},
            signature_version="s3v4",
        ))

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, str]:
        """Parse s3://bucket/key or s3a://bucket/key into (bucket, key)."""
        parsed = urlparse(uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    @staticmethod
    def _build_uri(bucket: str, key: str, scheme: str = "s3") -> str:
        return f"{scheme}://{bucket}/{key}"

    def list_objects(self, prefix: str) -> Iterator[FileInfo]:
        bucket, key_prefix = self._parse_uri(prefix)
        scheme = prefix.split("://")[0]
        paginator = self._s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                full_uri = self._build_uri(bucket, obj["Key"], scheme)
                relative = obj["Key"][len(key_prefix):]
                yield FileInfo(
                    uri=full_uri,
                    relative_path=relative,
                    size_bytes=obj["Size"],
                    etag=obj.get("ETag", "").strip('"'),
                )

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = self._parse_uri(uri)
        response = self._s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def write_bytes(self, uri: str, data: bytes) -> None:
        bucket, key = self._parse_uri(uri)
        self._s3.put_object(Bucket=bucket, Key=key, Body=data)

    def exists(self, uri: str) -> bool:
        bucket, key = self._parse_uri(uri)
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def delete(self, uri: str) -> None:
        bucket, key = self._parse_uri(uri)
        self._s3.delete_object(Bucket=bucket, Key=key)

    def copy_from(self, source_backend: StorageBackend, source_uri: str, target_uri: str) -> None:
        data = source_backend.read_bytes(source_uri)
        self.write_bytes(target_uri, data)
