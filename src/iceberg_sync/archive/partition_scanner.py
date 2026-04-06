"""
archive/partition_scanner.py — Scan Iceberg manifest Avro files and filter
entries by requested partition values.

Iceberg stores partition values as a nested record inside each manifest entry.
The record's field names match the partition field names in the table's
partition spec.  For example, a table partitioned by (year, month) has
partition records like {"year": 2025, "month": 11}.

Partial matching is supported: specifying {"year": 2025} matches all entries
where year == 2025 regardless of the month value.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Set

import fastavro

from iceberg_sync.storage.base import StorageBackend

log = logging.getLogger(__name__)

# Iceberg content type constants (spec §3.1.3)
CONTENT_DATA = 0
CONTENT_POSITION_DELETES = 1
CONTENT_EQUALITY_DELETES = 2

# Manifest entry status
STATUS_EXISTING = 0
STATUS_ADDED = 1
STATUS_DELETED = 2


@dataclass(frozen=True)
class ScannedFile:
    """A single data file (or delete file) found in an archived manifest."""
    file_path: str          # absolute URI in archive storage
    partition: Dict[str, Any]
    file_size_bytes: int
    content: int            # 0=DATA, 1=POS_DEL, 2=EQ_DEL
    record_count: int = 0


def _normalize_partition_value(v: Any) -> Any:
    """Decode bytes partition values to str (Iceberg stores strings as binary in Avro)."""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8")
    return v


def _partition_matches(file_partition: Dict[str, Any], requested: Dict[str, Any]) -> bool:
    """
    Return True if *file_partition* satisfies *requested*.

    Supports partial key matching: a requested dict with fewer keys than the
    partition spec matches any file whose specified keys are equal.
    """
    for key, value in requested.items():
        actual = _normalize_partition_value(file_partition.get(key))
        if actual != value:
            return False
    return True


def _any_partition_matches(
    file_partition: Dict[str, Any],
    requested_partitions: List[Dict[str, Any]],
) -> bool:
    """Return True if the file partition matches ANY of the requested specs."""
    if not requested_partitions:
        return True  # empty list = full table restore
    return any(_partition_matches(file_partition, req) for req in requested_partitions)


def _read_manifest(storage: StorageBackend, manifest_uri: str) -> Iterator[dict]:
    """Yield raw Avro records from a single manifest file."""
    data = storage.read_bytes(manifest_uri)
    reader = fastavro.reader(io.BytesIO(data))
    yield from reader


def _extract_file_info(record: dict) -> tuple[str, Dict[str, Any], int, int, int]:
    """
    Extract (file_path, partition, file_size, content, record_count) from a
    manifest record.  Handles both Iceberg v1 (flat) and v2 (nested data_file)
    formats.
    """
    df = record.get("data_file") or record
    file_path: str = df.get("file_path", "")
    partition: Dict[str, Any] = dict(df.get("partition") or {})
    file_size: int = df.get("file_size_in_bytes", 0)
    content: int = df.get("content", 0)
    record_count: int = df.get("record_count", 0)
    return file_path, partition, file_size, content, record_count


def scan_snapshot(
    storage: StorageBackend,
    metadata: dict,
    snapshot_id: int,
    requested_partitions: List[Dict[str, Any]],
    *,
    include_delete_files: bool = True,
    path_rewriter: Optional[Any] = None,
) -> List[ScannedFile]:
    """
    Walk the manifest chain for *snapshot_id* and return only the files
    whose partition matches at least one entry in *requested_partitions*.

    Args:
        storage:               StorageBackend pointing at the archive root.
        metadata:              Parsed metadata.json dict from the archive.
        snapshot_id:           The snapshot to scan.
        requested_partitions:  List of partition dicts to filter on.
                               Pass an empty list to return ALL files
                               (full table restore).
        include_delete_files:  If False, skip positional / equality delete files.

    Returns:
        List of ScannedFile (deduplicated by file_path).
    """
    # Find the snapshot entry
    target_snap: Optional[dict] = None
    for snap in metadata.get("snapshots", []):
        if snap.get("snapshot-id") == snapshot_id:
            target_snap = snap
            break

    if target_snap is None:
        raise ValueError(f"Snapshot {snapshot_id} not found in metadata.")

    manifest_list_uri: str = target_snap.get("manifest-list", "")
    if not manifest_list_uri:
        raise ValueError(f"Snapshot {snapshot_id} has no manifest-list URI.")

    # Read manifest-list to get individual manifest URIs
    manifest_list_data = storage.read_bytes(manifest_list_uri)
    manifest_entries = list(fastavro.reader(io.BytesIO(manifest_list_data)))

    seen_paths: Set[str] = set()
    results: List[ScannedFile] = []

    for manifest_entry in manifest_entries:
        manifest_path: str = manifest_entry.get("manifest_path", "")
        if not manifest_path:
            continue
        if path_rewriter is not None:
            manifest_path = path_rewriter(manifest_path)

        try:
            for record in _read_manifest(storage, manifest_path):
                status: int = record.get("status", STATUS_EXISTING)
                if status == STATUS_DELETED:
                    continue

                file_path, partition, file_size, content, record_count = _extract_file_info(record)

                if not file_path or file_path in seen_paths:
                    continue

                if not include_delete_files and content != CONTENT_DATA:
                    continue

                if not _any_partition_matches(partition, requested_partitions):
                    continue

                seen_paths.add(file_path)
                results.append(ScannedFile(
                    file_path=file_path,
                    partition=partition,
                    file_size_bytes=file_size,
                    content=content,
                    record_count=record_count,
                ))
        except Exception as exc:
            log.warning("Failed to read manifest %s: %s", manifest_path, exc)

    return results


def unique_partitions(files: List[ScannedFile]) -> List[Dict[str, Any]]:
    """Return the distinct partition values present in a list of ScannedFiles."""
    seen: Set[str] = set()
    result: List[Dict[str, Any]] = []
    for f in files:
        key = str(sorted(f.partition.items()))
        if key not in seen:
            seen.add(key)
            result.append(f.partition)
    return result


def total_bytes(files: List[ScannedFile]) -> int:
    return sum(f.file_size_bytes for f in files)
