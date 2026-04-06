"""
storage/factory.py — Create the right StorageBackend from a URI or config.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from iceberg_sync.storage.base import StorageBackend


def create_storage(
    uri_or_scheme: str,
    **kwargs: Any,
) -> StorageBackend:
    """
    Create a StorageBackend based on the URI scheme.

    Args:
        uri_or_scheme: A full URI (s3://bucket/...) or just a scheme (s3, abfss, gs).
        **kwargs:      Backend-specific arguments passed through to the constructor.
                       S3:   endpoint_url, region_name, aws_access_key_id, aws_secret_access_key
                       ADLS: storage_account_name, storage_account_key, storage_connection_string
                       GCS:  project

    Examples:
        create_storage("s3://my-bucket/path", region_name="us-east-1")
        create_storage("abfss://container@account.dfs.core.windows.net/path",
                       storage_account_name="account")
        create_storage("s3a://minio-bucket/path",
                       endpoint_url="http://localhost:9000",
                       aws_access_key_id="minioadmin",
                       aws_secret_access_key="minioadmin")
    """
    uri_or_scheme = uri_or_scheme.strip("\"'")
    scheme = uri_or_scheme.split("://")[0].lower()

    if scheme in ("s3", "s3a", "s3n"):
        from iceberg_sync.storage.s3 import S3StorageBackend
        return S3StorageBackend(**kwargs)

    elif scheme in ("abfss", "abfs"):
        from iceberg_sync.storage.adls import ADLSStorageBackend
        return ADLSStorageBackend(**kwargs)

    elif scheme == "gs":
        from iceberg_sync.storage.gcs import GCSStorageBackend
        return GCSStorageBackend(**kwargs)

    else:
        raise ValueError(
            f"Unknown storage scheme: '{scheme}'. "
            f"Supported: s3, s3a, abfss, abfs, gs"
        )
