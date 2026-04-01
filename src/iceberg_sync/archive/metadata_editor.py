"""
archive/metadata_editor.py — Reconstruct Iceberg metadata.json for restore.

Three operations are supported:

  write_table_metadata()   Build a fresh metadata.json for a new_table restore.
  splice_manifests()       Merge restored manifests into the live table's current
                           snapshot (replace/append mode).
  expire_snapshots()       Rewrite metadata.json to remove expired snapshot
                           entries after archival.

All writes use fastavro for Avro files and orjson for JSON, consistent with
the rest of the iceberg_sync codebase.
"""

from __future__ import annotations

import copy
import io
import logging
import time
from typing import Any, Dict, List, Optional, Set

import fastavro
import orjson

from iceberg_sync.storage.base import StorageBackend

log = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _next_version(metadata: dict) -> int:
    """Return the next sequential metadata version number."""
    versions = [
        int(e.get("metadata-file", "v0.metadata.json")
            .split("/")[-1].split(".")[0].lstrip("v") or 0)
        for e in metadata.get("metadata-log", [])
    ]
    return (max(versions) + 1) if versions else 1


def _rewrite_avro_paths(
    storage: StorageBackend,
    src_uri: str,
    dst_uri: str,
    old_root: str,
    new_root: str,
    field_names: List[str],
) -> int:
    """
    Copy an Avro file from src_uri to dst_uri, replacing *old_root* with
    *new_root* in all fields listed in *field_names*.

    Returns the number of path translations performed.
    """
    raw = storage.read_bytes(src_uri)
    records = list(fastavro.reader(io.BytesIO(raw)))
    schema = fastavro.reader(io.BytesIO(raw)).writer_schema

    translations = 0
    for record in records:
        for fname in field_names:
            val = record.get(fname)
            if isinstance(val, str) and val.startswith(old_root):
                record[fname] = new_root + val[len(old_root):]
                translations += 1
            # Handle nested data_file struct (Iceberg v2)
            df = record.get("data_file")
            if df and isinstance(df, dict):
                val2 = df.get(fname)
                if isinstance(val2, str) and val2.startswith(old_root):
                    df[fname] = new_root + val2[len(old_root):]
                    translations += 1

    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    storage.write_bytes(dst_uri, buf.getvalue())
    return translations


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def write_table_metadata(
    storage: StorageBackend,
    archived_metadata: dict,
    target_table_root: str,
    new_manifest_list_uri: str,
    new_snapshot_id: int,
) -> str:
    """
    Write a fresh metadata.json for a new_table or full-replace restore.

    Clones the archived metadata, resets the snapshot list to a single new
    snapshot pointing at *new_manifest_list_uri*, and writes to target storage.

    Returns the URI of the written metadata file.
    """
    meta = copy.deepcopy(archived_metadata)
    now = _now_ms()

    new_snap: Dict[str, Any] = {
        "snapshot-id": new_snapshot_id,
        "timestamp-ms": now,
        "manifest-list": new_manifest_list_uri,
        "summary": {"operation": "append", "iceberg-restore": "true"},
        "schema-id": meta.get("current-schema-id", 0),
    }

    meta["snapshots"] = [new_snap]
    meta["current-snapshot-id"] = new_snapshot_id
    meta["last-updated-ms"] = now
    meta["last-sequence-number"] = meta.get("last-sequence-number", 0) + 1
    meta["location"] = target_table_root
    meta.pop("metadata-log", None)
    meta.pop("refs", None)

    next_ver = _next_version(archived_metadata)
    meta_uri = f"{target_table_root.rstrip('/')}/metadata/v{next_ver:05d}.metadata.json"
    storage.write_bytes(meta_uri, orjson.dumps(meta, option=orjson.OPT_INDENT_2))
    log.info("Wrote fresh metadata → %s", meta_uri)
    return meta_uri


def splice_manifests(
    *,
    target_storage: StorageBackend,
    current_metadata: dict,
    current_table_root: str,
    restored_manifest_uris: List[str],
    restored_partitions: List[Dict[str, Any]],
    mode: str,                            # "replace" or "append"
    archive_root: str,
    target_root: str,
) -> str:
    """
    Produce a new metadata.json that splices restored manifests into the live
    table's current snapshot.

    For *replace* mode:
      - Load all current manifests.
      - Filter OUT entries whose partition matches a restored partition.
      - Append the restored manifests.
      - Write a new manifest-list and a new snapshot.

    For *append* mode:
      - Keep all current manifest entries untouched.
      - Append the restored manifests as an additional snapshot.

    Returns the URI of the new metadata file.
    """
    table_meta_root = f"{current_table_root.rstrip('/')}/metadata"
    now = _now_ms()
    new_snap_id = int(time.time() * 1000)   # 64-bit epoch-ms; no modulo

    current_snap_id: int = current_metadata.get("current-snapshot-id", -1)
    current_snap: Optional[dict] = None
    for snap in current_metadata.get("snapshots", []):
        if snap.get("snapshot-id") == current_snap_id:
            current_snap = snap
            break

    if current_snap is None:
        raise RuntimeError("Cannot find current snapshot in live metadata.")

    manifest_list_uri: str = current_snap.get("manifest-list", "")

    # ── Build the new manifest-list entries ──────────────────────────────────

    ml_data = target_storage.read_bytes(manifest_list_uri)
    ml_records = list(fastavro.reader(io.BytesIO(ml_data)))
    ml_schema = fastavro.reader(io.BytesIO(ml_data)).writer_schema

    if mode == "replace" and restored_partitions:
        # For each current manifest, rewrite it to remove entries whose
        # partition matches a restored partition.  Manifests that become
        # empty are dropped from the list entirely.
        from iceberg_sync.archive.partition_scanner import _any_partition_matches

        filtered_ml_records = []
        for ml_entry in ml_records:
            mpath: str = ml_entry.get("manifest_path", "")
            if not mpath:
                filtered_ml_records.append(ml_entry)
                continue

            try:
                m_data = target_storage.read_bytes(mpath)
                m_records = list(fastavro.reader(io.BytesIO(m_data)))
                m_schema = fastavro.reader(io.BytesIO(m_data)).writer_schema
            except Exception as exc:
                log.warning("Cannot read manifest %s for filtering: %s", mpath, exc)
                filtered_ml_records.append(ml_entry)
                continue

            # Keep only entries whose partition does NOT match any restored partition
            kept = [
                r for r in m_records
                if not _any_partition_matches(
                    dict((r.get("data_file") or r).get("partition") or {}),
                    restored_partitions,
                )
            ]

            if not kept:
                # All entries removed — drop this manifest from the list
                log.debug("Dropping fully-replaced manifest %s", mpath)
                continue

            if len(kept) == len(m_records):
                # Nothing removed — keep original manifest entry as-is
                filtered_ml_records.append(ml_entry)
                continue

            # Write a filtered copy of the manifest
            filtered_path = mpath.replace(".avro", f"-filtered-{new_snap_id}.avro")
            buf = io.BytesIO()
            fastavro.writer(buf, m_schema, kept)
            target_storage.write_bytes(filtered_path, buf.getvalue())
            filtered_bytes = buf.getvalue()

            updated_entry = dict(ml_entry)
            updated_entry["manifest_path"] = filtered_path
            updated_entry["manifest_length"] = len(filtered_bytes)
            updated_entry["existing_data_files_count"] = sum(
                1 for r in kept if r.get("status") == 0
            )
            updated_entry["deleted_data_files_count"] = sum(
                1 for r in kept if r.get("status") == 2
            )
            filtered_ml_records.append(updated_entry)

        ml_records = filtered_ml_records

    # Add restored manifest entries to the manifest-list
    for restored_uri in restored_manifest_uris:
        try:
            manifest_bytes = target_storage.read_bytes(restored_uri)
            records_in_manifest = list(fastavro.reader(io.BytesIO(manifest_bytes)))
            added = sum(1 for r in records_in_manifest if r.get("status") == 1)
            existing = sum(1 for r in records_in_manifest if r.get("status") == 0)

            ml_records.append({
                "manifest_path": restored_uri,
                "manifest_length": len(manifest_bytes),
                "partition_spec_id": current_metadata.get("default-spec-id", 0),
                "added_snapshot_id": new_snap_id,
                "added_data_files_count": added,
                "existing_data_files_count": existing,
                "deleted_data_files_count": 0,
                "partitions": [],
            })
        except Exception as exc:
            log.warning("Could not read restored manifest %s: %s", restored_uri, exc)

    # Write new manifest-list
    new_ml_uri = f"{table_meta_root}/snap-{new_snap_id}-manifest-list.avro"
    buf = io.BytesIO()
    fastavro.writer(buf, ml_schema, ml_records)
    target_storage.write_bytes(new_ml_uri, buf.getvalue())

    # ── Build new snapshot + metadata ────────────────────────────────────────

    meta = copy.deepcopy(current_metadata)
    new_snap: Dict[str, Any] = {
        "snapshot-id": new_snap_id,
        "parent-snapshot-id": current_snap_id,
        "timestamp-ms": now,
        "manifest-list": new_ml_uri,
        "summary": {
            "operation": "overwrite" if mode == "replace" else "append",
            "iceberg-restore": "true",
        },
        "schema-id": meta.get("current-schema-id", 0),
    }

    meta.setdefault("snapshots", []).append(new_snap)
    meta["current-snapshot-id"] = new_snap_id
    meta["last-updated-ms"] = now
    meta["last-sequence-number"] = meta.get("last-sequence-number", 0) + 1

    next_ver = _next_version(current_metadata)
    new_meta_uri = f"{table_meta_root}/v{next_ver:05d}.metadata.json"

    meta.setdefault("metadata-log", []).append({"metadata-file": new_meta_uri, "timestamp-ms": now})
    target_storage.write_bytes(new_meta_uri, orjson.dumps(meta, option=orjson.OPT_INDENT_2))
    log.info("Wrote spliced metadata → %s", new_meta_uri)
    return new_meta_uri


def expire_snapshots_in_metadata(
    metadata: dict,
    snapshot_ids_to_remove: Set[int],
    table_root: str,
    storage: StorageBackend,
) -> str:
    """
    Rewrite metadata.json to remove specified snapshot IDs and write a new
    versioned metadata file.

    Called by the archiver after it has safely copied the to-be-expired
    snapshots to cold storage.

    Returns the URI of the new metadata file.
    """
    meta = copy.deepcopy(metadata)
    now = _now_ms()

    # Filter snapshots list
    meta["snapshots"] = [
        s for s in meta.get("snapshots", [])
        if s.get("snapshot-id") not in snapshot_ids_to_remove
    ]

    # Filter snapshot-log
    meta["snapshot-log"] = [
        e for e in meta.get("snapshot-log", [])
        if e.get("snapshot-id") not in snapshot_ids_to_remove
    ]

    meta["last-updated-ms"] = now

    table_meta_root = f"{table_root.rstrip('/')}/metadata"
    next_ver = _next_version(metadata)
    new_meta_uri = f"{table_meta_root}/v{next_ver:05d}.metadata.json"

    meta.setdefault("metadata-log", []).append(
        {"metadata-file": new_meta_uri, "timestamp-ms": now}
    )
    storage.write_bytes(new_meta_uri, orjson.dumps(meta, option=orjson.OPT_INDENT_2))
    log.info("Wrote expired-snapshot metadata → %s", new_meta_uri)
    return new_meta_uri
