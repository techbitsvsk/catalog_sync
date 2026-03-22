"""
storage/memory.py — In-memory storage backend for testing.

No cloud credentials or network access required.  Stores all objects in a
Python dict, making tests fast, deterministic, and fully isolated.
"""

from __future__ import annotations

from typing import Dict, Iterator

from iceberg_sync.storage.base import FileInfo, StorageBackend


class MemoryStorageBackend(StorageBackend):
    """
    In-memory storage for unit/integration tests.

    All objects are stored in a flat dict keyed by full URI.
    """

    def __init__(self):
        self._store: Dict[str, bytes] = {}

    def list_objects(self, prefix: str) -> Iterator[FileInfo]:
        for uri, data in sorted(self._store.items()):
            if uri.startswith(prefix):
                relative = uri[len(prefix):]
                yield FileInfo(
                    uri=uri,
                    relative_path=relative,
                    size_bytes=len(data),
                    etag=None,
                )

    def read_bytes(self, uri: str) -> bytes:
        if uri not in self._store:
            raise FileNotFoundError(f"Object not found: {uri}")
        return self._store[uri]

    def write_bytes(self, uri: str, data: bytes) -> None:
        self._store[uri] = data

    def exists(self, uri: str) -> bool:
        return uri in self._store

    def delete(self, uri: str) -> None:
        self._store.pop(uri, None)

    def copy_from(self, source_backend: StorageBackend, source_uri: str, target_uri: str) -> None:
        data = source_backend.read_bytes(source_uri)
        self.write_bytes(target_uri, data)

    # ── Test helpers ─────────────────────────────────────────────────────

    def put(self, uri: str, data: bytes | str) -> None:
        """Convenience: accept str or bytes."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._store[uri] = data

    def keys(self) -> list[str]:
        return sorted(self._store.keys())

    def clear(self) -> None:
        self._store.clear()
