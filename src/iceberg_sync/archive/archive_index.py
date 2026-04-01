"""
archive/archive_index.py — Read and write the .archive-manifest.json index file.

The index is a small JSON file written alongside archived table data.  It records
every snapshot that was archived, when, and from where — enabling the restore
workflow to list available restore points without scanning the full Avro chain.

File location inside the archive storage:
    <archive_root>/<table>/.archive-manifest.json
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import orjson

from iceberg_sync.storage.base import StorageBackend

log = logging.getLogger(__name__)

ARCHIVE_INDEX_FILENAME = ".archive-manifest.json"


@dataclass
class ArchivedSnapshotEntry:
    """Metadata for a single archived snapshot."""
    snapshot_id: int
    timestamp_ms: int
    operation: str                        # append | overwrite | delete | replace
    added_data_files: int = 0
    added_records: int = 0
    manifest_list_path: str = ""          # relative path within archive storage
    data_files_count: int = 0
    size_bytes: int = 0

    @property
    def timestamp_iso(self) -> str:
        return datetime.fromtimestamp(
            self.timestamp_ms / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ArchivedSnapshotEntry":
        return cls(
            snapshot_id=d["snapshot_id"],
            timestamp_ms=d["timestamp_ms"],
            operation=d.get("operation", "unknown"),
            added_data_files=d.get("added_data_files", 0),
            added_records=d.get("added_records", 0),
            manifest_list_path=d.get("manifest_list_path", ""),
            data_files_count=d.get("data_files_count", 0),
            size_bytes=d.get("size_bytes", 0),
        )


@dataclass
class ArchiveIndex:
    """
    Index of all archived snapshots for a single table.

    Written to <archive_root>/<table>/.archive-manifest.json after every
    archive run.  Append-only — new entries are added on each run.
    """
    source_root: str
    table: str
    archive_root: str
    snapshots: List[ArchivedSnapshotEntry] = field(default_factory=list)
    schema: Optional[Dict[str, Any]] = None
    partition_spec: Optional[List[Dict[str, Any]]] = None
    last_archived_at: str = ""

    # ── Persistence ───────────────────────────────────────────────────────────

    def index_uri(self) -> str:
        table_root = f"{self.archive_root.rstrip('/')}/{self.table}"
        return f"{table_root}/{ARCHIVE_INDEX_FILENAME}"

    def save(self, storage: StorageBackend) -> None:
        uri = self.index_uri()
        payload = {
            "source_root": self.source_root,
            "table": self.table,
            "archive_root": self.archive_root,
            "last_archived_at": self.last_archived_at,
            "schema": self.schema,
            "partition_spec": self.partition_spec,
            "snapshots": [s.to_dict() for s in self.snapshots],
        }
        storage.write_bytes(uri, orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        log.debug("Wrote archive index → %s", uri)

    @classmethod
    def load(cls, storage: StorageBackend, archive_root: str, table: str) -> "ArchiveIndex":
        table_root = f"{archive_root.rstrip('/')}/{table}"
        uri = f"{table_root}/{ARCHIVE_INDEX_FILENAME}"
        raw = orjson.loads(storage.read_bytes(uri))
        idx = cls(
            source_root=raw.get("source_root", ""),
            table=raw.get("table", table),
            archive_root=raw.get("archive_root", archive_root),
            last_archived_at=raw.get("last_archived_at", ""),
            schema=raw.get("schema"),
            partition_spec=raw.get("partition_spec"),
        )
        idx.snapshots = [
            ArchivedSnapshotEntry.from_dict(s) for s in raw.get("snapshots", [])
        ]
        return idx

    @classmethod
    def load_or_new(
        cls,
        storage: StorageBackend,
        archive_root: str,
        table: str,
        source_root: str,
    ) -> "ArchiveIndex":
        """Load existing index or create a fresh one if none exists yet."""
        try:
            return cls.load(storage, archive_root, table)
        except Exception:
            log.debug("No existing archive index for %s — starting fresh.", table)
            return cls(source_root=source_root, table=table, archive_root=archive_root)

    # ── Query helpers ─────────────────────────────────────────────────────────

    def find_snapshot(
        self,
        *,
        snapshot_id: Optional[int] = None,
        as_of_iso: Optional[str] = None,
    ) -> Optional[ArchivedSnapshotEntry]:
        """
        Find a snapshot by explicit ID or by point-in-time (returns the latest
        snapshot whose timestamp is <= the requested date).
        """
        if snapshot_id is not None:
            for s in self.snapshots:
                if s.snapshot_id == snapshot_id:
                    return s
            return None

        if as_of_iso is not None:
            cutoff_ms = int(
                datetime.fromisoformat(as_of_iso)
                .replace(tzinfo=timezone.utc)
                .timestamp() * 1000
            )
            candidates = [s for s in self.snapshots if s.timestamp_ms <= cutoff_ms]
            if not candidates:
                return None
            return max(candidates, key=lambda s: s.timestamp_ms)

        # Default: most recent archived snapshot
        if not self.snapshots:
            return None
        return max(self.snapshots, key=lambda s: s.timestamp_ms)

    def snapshot_ids(self) -> List[int]:
        return [s.snapshot_id for s in self.snapshots]
