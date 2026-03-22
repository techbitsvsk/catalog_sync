"""
storage/base.py — Abstract storage interface for cloud-agnostic file operations.

Every cloud has a different SDK for reading/writing blobs.  This interface
normalises them so the sync engine never sees boto3 vs azure-storage-blob
vs google-cloud-storage directly.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Iterator, List, Optional


@dataclass(frozen=True)
class FileInfo:
    """Metadata about a single storage object."""
    uri: str              # Full absolute URI (s3://bucket/key)
    relative_path: str    # Path below the root prefix
    size_bytes: int
    etag: Optional[str] = None  # For change detection


class StorageBackend(abc.ABC):
    """
    Abstract interface for cloud object storage.

    Implementations must handle:
    •  Listing objects under a prefix
    •  Reading object bytes
    •  Writing object bytes
    •  Checking existence
    •  Cross-cloud copy (source backend reads, target backend writes)
    """

    @abc.abstractmethod
    def list_objects(self, prefix: str) -> Iterator[FileInfo]:
        """
        Yield FileInfo for every object under the given URI prefix.
        The prefix is a full URI (e.g. s3://bucket/iceberg/gold/).
        """

    @abc.abstractmethod
    def read_bytes(self, uri: str) -> bytes:
        """Read the full contents of an object."""

    @abc.abstractmethod
    def write_bytes(self, uri: str, data: bytes) -> None:
        """Write bytes to an object (creates or overwrites)."""

    @abc.abstractmethod
    def exists(self, uri: str) -> bool:
        """Return True if the object exists."""

    @abc.abstractmethod
    def delete(self, uri: str) -> None:
        """Delete a single object."""

    @abc.abstractmethod
    def copy_from(self, source_backend: "StorageBackend", source_uri: str, target_uri: str) -> None:
        """
        Copy a single object from another backend.
        Default: read from source, write to target.
        Subclasses may override with native cross-cloud copy.
        """

    def read_text(self, uri: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(uri).decode(encoding)

    def write_text(self, uri: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(uri, text.encode(encoding))
