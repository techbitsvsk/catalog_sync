"""
metadata/reader.py — Read data file information directly from Iceberg manifests.

Used by CatalogSync to discover which data files belong to the current snapshot
without relying on a specific directory layout.

Why this matters
────────────────
The naive approach lists everything under table_root/data/ and diffs by relative
path.  That breaks for tables with a custom write.data.path setting, and silently
misses delete files (Iceberg v2 row-level deletes live alongside data files).

This module reads the manifest chain directly:

    metadata.json
        └── manifest-list  (current snapshot)
                └── manifests
                        └── data_file.file_path  ← the ground truth

It returns only files that the current snapshot actually references, regardless
of where on storage they live.  The diff in CatalogSync then checks each of
those paths against the target rather than doing a broad prefix scan.

Content type constants (Iceberg spec §3.1.3)
────────────────────────────────────────────
    0 = DATA               (Parquet / ORC / Avro data files)
    1 = POSITION_DELETES   (positional delete files, Iceberg v2)
    2 = EQUALITY_DELETES   (equality delete files, Iceberg v2)

All three types are returned by default — the sync must copy them all for
the table to be queryable on the target.
"""

from __future__ import annotations

import io
import logging
from typing import Dict, List, Optional, Tuple

import fastavro

from iceberg_sync.storage.base import FileInfo, StorageBackend

log = logging.getLogger(__name__)

# Iceberg manifest content-type constants
_CONTENT_DATA = 0
_CONTENT_POSITION_DELETES = 1
_CONTENT_EQUALITY_DELETES = 2
_ALL_CONTENT_TYPES = {_CONTENT_DATA, _CONTENT_POSITION_DELETES, _CONTENT_EQUALITY_DELETES}


def read_snapshot_data_files(
    storage: StorageBackend,
    metadata: dict,
    table_root: str,
    *,
    snapshot_id: Optional[int] = None,
    content_types: Optional[set] = None,
) -> Dict[str, FileInfo]:
    """
    Read all files referenced by an Iceberg snapshot from its manifest chain.

    Args:
        storage:       Backend to read from (source storage).
        metadata:      Parsed metadata.json dict for the table.
        table_root:    Absolute URI of the table root — used to compute
                       relative paths for diffing against the target.
        snapshot_id:   Snapshot to read.  Defaults to current-snapshot-id.
        content_types: Set of Iceberg content-type ints to include.
                       Defaults to all three: DATA, POSITION_DELETES,
                       EQUALITY_DELETES.  Pass {0} for data files only.

    Returns:
        Dict mapping relative-path-from-table-root → FileInfo(uri, size_bytes).
        The relative path is used as the key for target-side diffing.

    Raises:
        ValueError:  No current snapshot found in metadata.
        Exception:   Propagates storage / Avro read errors.
    """
    if content_types is None:
        content_types = _ALL_CONTENT_TYPES

    target_snapshot_id = snapshot_id if snapshot_id is not None else metadata.get("current-snapshot-id", -1)
    if target_snapshot_id == -1:
        log.warning("Metadata has no current-snapshot-id — returning empty file set")
        return {}

    snapshots = metadata.get("snapshots", [])
    snapshot = next((s for s in snapshots if s["snapshot-id"] == target_snapshot_id), None)
    if snapshot is None:
        raise ValueError(
            f"Snapshot {target_snapshot_id} not found in metadata "
            f"(available: {[s['snapshot-id'] for s in snapshots]})"
        )

    manifest_list_uri = snapshot.get("manifest-list")
    if not manifest_list_uri:
        log.warning(f"Snapshot {target_snapshot_id} has no manifest-list URI")
        return {}

    log.debug(f"Reading manifest list: {manifest_list_uri}")
    raw_ml = storage.read_bytes(manifest_list_uri)
    manifest_entries = list(fastavro.reader(io.BytesIO(raw_ml)))

    table_prefix = table_root.rstrip("/") + "/"
    result: Dict[str, FileInfo] = {}

    for ml_entry in manifest_entries:
        manifest_uri = ml_entry["manifest_path"]

        # manifest_content tells us whether this manifest contains DATA (0),
        # POSITION_DELETES (1), or EQUALITY_DELETES (2).  Skip if not wanted.
        manifest_content = ml_entry.get("content", _CONTENT_DATA)
        if manifest_content not in content_types:
            log.debug(f"Skipping manifest (content={manifest_content}): {manifest_uri}")
            continue

        log.debug(f"Reading manifest: {manifest_uri}")
        try:
            raw_m = storage.read_bytes(manifest_uri)
        except Exception as e:
            log.warning(f"Could not read manifest {manifest_uri}: {e} — skipping")
            continue

        try:
            records = list(fastavro.reader(io.BytesIO(raw_m)))
        except Exception as e:
            log.warning(f"Could not parse manifest {manifest_uri}: {e} — skipping")
            continue

        for record in records:
            # status: 0=EXISTING, 1=ADDED, 2=DELETED
            # Skip DELETED entries — those files are no longer part of this snapshot
            status = record.get("status", 1)
            if status == 2:
                continue

            # Iceberg v2: file info is in a 'data_file' struct
            # Iceberg v1: fields are at the top level of the record
            data_file = record.get("data_file") or record

            file_path = data_file.get("file_path")
            if not file_path:
                continue

            # Per-record content type (may differ from manifest-level content)
            file_content = data_file.get("content", _CONTENT_DATA)
            if file_content not in content_types:
                continue

            size = data_file.get("file_size_in_bytes", 0)

            # Compute path relative to table root for target diffing
            if file_path.startswith(table_prefix):
                rel = file_path[len(table_prefix):]
            else:
                # File lives outside the table root (custom write.data.path).
                # Use the path component after the scheme+authority so the
                # relative key is still unique and translatable.
                after_scheme = file_path.split("://", 1)[-1]   # "bucket/prefix/file.parquet"
                rel = after_scheme
                log.debug(f"Data file outside table root — using full path as key: {file_path}")

            result[rel] = FileInfo(uri=file_path, relative_path=rel, size_bytes=size)

    log.debug(
        f"Snapshot {target_snapshot_id}: found {len(result)} files "
        f"across {len(manifest_entries)} manifests"
    )
    return result


def read_all_snapshot_files(
    storage: StorageBackend,
    metadata: dict,
    table_root: str,
) -> Dict[str, FileInfo]:
    """
    Read files referenced by ALL snapshots (not just the current one).

    Used by cleanup_orphan_files to build the complete set of referenced files
    so that historical files needed for time-travel are not deleted.
    """
    all_files: Dict[str, FileInfo] = {}
    for snapshot in metadata.get("snapshots", []):
        snap_id = snapshot["snapshot-id"]
        try:
            snap_files = read_snapshot_data_files(
                storage, metadata, table_root, snapshot_id=snap_id
            )
            all_files.update(snap_files)
        except Exception as e:
            log.warning(f"Could not read files for snapshot {snap_id}: {e}")
    return all_files
