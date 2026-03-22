"""
metadata/rewriter.py — Rewrite Iceberg metadata with translated storage paths.

This module handles the complete Iceberg metadata chain:

    metadata.json (JSON)
        └── snapshot.manifest-list (Avro: manifest_path)
                └── manifest (Avro: data_file.file_path)

Every absolute URI at every level is translated using a PathTranslator.

Design decisions
────────────────
•  We rewrite at the byte level using fastavro — no dependency on PyIceberg
   or the Iceberg Java runtime.  This means the rewriter works as a standalone
   Python CLI with zero Spark/JVM dependencies.
•  Rewritten metadata files are placed at the translated path on the target.
   The relative path structure is preserved so a Hadoop catalog reader on the
   target cloud can find everything.
•  The version-hint.text file (which just contains the current metadata version
   number) is written last — this is the atomic pointer that makes the table
   readable.  If the sync fails mid-way, the target still points at the previous
   good state (or doesn't exist yet).
•  Only the latest snapshot's manifests are rewritten by default.  Historical
   snapshots are replicated with untranslated paths — they'll fail if accessed
   on the target, which is acceptable because you only query the current state
   during failover.  Pass rewrite_all_snapshots=True to rewrite everything
   (slower but enables time-travel on the target).
"""

from __future__ import annotations

import copy
import io
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import fastavro

from iceberg_sync.path_translator import PathTranslator
from iceberg_sync.storage.base import StorageBackend

log = logging.getLogger(__name__)


@dataclass
class RewriteStats:
    """Counters from a metadata rewrite operation."""
    metadata_files_rewritten: int = 0
    manifest_lists_rewritten: int = 0
    manifests_rewritten: int = 0
    data_file_paths_translated: int = 0
    errors: List[str] = field(default_factory=list)


class MetadataRewriter:
    """
    Rewrites an Iceberg table's metadata chain with translated storage paths.

    Usage:
        translator = PathTranslator([
            ("s3://warehouse/iceberg/", "abfss://iceberg@acct.dfs.core.windows.net/iceberg/"),
        ])

        rewriter = MetadataRewriter(
            translator=translator,
            source_storage=s3_backend,
            target_storage=adls_backend,
        )

        stats = rewriter.rewrite_table(
            source_metadata_uri="s3://warehouse/iceberg/gold/top_customers/metadata/v3.metadata.json"
        )
    """

    def __init__(
        self,
        translator: PathTranslator,
        source_storage: StorageBackend,
        target_storage: StorageBackend,
        rewrite_all_snapshots: bool = False,
    ):
        self._translator = translator
        self._source = source_storage
        self._target = target_storage
        self._rewrite_all = rewrite_all_snapshots

    def rewrite_table(self, source_metadata_uri: str) -> RewriteStats:
        """
        Rewrite the full metadata chain for one Iceberg table.

        Args:
            source_metadata_uri: Full URI to the source metadata.json
                (e.g. s3://warehouse/iceberg/gold/table/metadata/v3.metadata.json)

        Returns:
            RewriteStats with counters and any non-fatal errors.
        """
        stats = RewriteStats()

        log.info(f"Reading source metadata: {source_metadata_uri}")
        metadata = json.loads(self._source.read_bytes(source_metadata_uri))

        # ── Determine which snapshots to rewrite ─────────────────────────
        current_snapshot_id = metadata.get("current-snapshot-id", -1)
        snapshots = metadata.get("snapshots", [])

        if self._rewrite_all:
            snapshot_ids_to_rewrite = {s["snapshot-id"] for s in snapshots}
        else:
            # Only rewrite current snapshot
            snapshot_ids_to_rewrite = {current_snapshot_id} if current_snapshot_id != -1 else set()

        # ── Rewrite table location ───────────────────────────────────────
        original_location = metadata["location"]
        metadata["location"] = self._translator.translate(original_location)
        log.info(f"Table location: {original_location} → {metadata['location']}")

        # ── Rewrite each snapshot's manifest-list ────────────────────────
        for snapshot in snapshots:
            snap_id = snapshot["snapshot-id"]
            manifest_list_uri = snapshot["manifest-list"]

            if snap_id not in snapshot_ids_to_rewrite:
                # Still translate the manifest-list path so metadata.json is
                # internally consistent, but don't rewrite manifest contents.
                snapshot["manifest-list"] = self._translator.translate(manifest_list_uri)
                continue

            try:
                new_manifest_list_uri = self._rewrite_manifest_list(
                    manifest_list_uri, stats
                )
                snapshot["manifest-list"] = new_manifest_list_uri
                stats.manifest_lists_rewritten += 1
            except Exception as e:
                msg = f"Error rewriting manifest-list for snapshot {snap_id}: {e}"
                log.error(msg)
                stats.errors.append(msg)
                # Still translate the path so metadata is parseable
                snapshot["manifest-list"] = self._translator.translate(manifest_list_uri)

        # ── Rewrite metadata-log and previous-metadata paths ─────────────
        for log_entry in metadata.get("metadata-log", []):
            if "metadata-file" in log_entry:
                log_entry["metadata-file"] = self._translator.translate(
                    log_entry["metadata-file"], strict=False
                )

        # ── Write rewritten metadata.json to target ──────────────────────
        target_metadata_uri = self._translator.translate(source_metadata_uri)
        metadata_bytes = json.dumps(metadata, indent=2, default=str).encode("utf-8")
        self._target.write_bytes(target_metadata_uri, metadata_bytes)
        stats.metadata_files_rewritten += 1
        log.info(f"Wrote target metadata: {target_metadata_uri}")

        # ── Write version-hint.text (atomic pointer) ─────────────────────
        self._write_version_hint(source_metadata_uri, target_metadata_uri)

        log.info(
            f"Rewrite complete: {stats.metadata_files_rewritten} metadata, "
            f"{stats.manifest_lists_rewritten} manifest-lists, "
            f"{stats.manifests_rewritten} manifests, "
            f"{stats.data_file_paths_translated} data file paths translated"
        )
        return stats

    def _rewrite_manifest_list(self, source_uri: str, stats: RewriteStats) -> str:
        """
        Read a manifest-list Avro, rewrite all manifest_path entries,
        then rewrite each referenced manifest.  Write results to target.
        """
        log.debug(f"Rewriting manifest-list: {source_uri}")
        raw_bytes = self._source.read_bytes(source_uri)
        reader = fastavro.reader(io.BytesIO(raw_bytes))
        schema = reader.writer_schema
        records = list(reader)

        for record in records:
            original_manifest_path = record["manifest_path"]

            # Rewrite the manifest itself
            try:
                new_manifest_path = self._rewrite_manifest(original_manifest_path, stats)
                record["manifest_path"] = new_manifest_path
            except Exception as e:
                log.warning(f"Failed to rewrite manifest {original_manifest_path}: {e}")
                record["manifest_path"] = self._translator.translate(original_manifest_path)

        # Write rewritten manifest-list to target
        target_uri = self._translator.translate(source_uri)
        out_buffer = io.BytesIO()
        fastavro.writer(out_buffer, schema, records)
        self._target.write_bytes(target_uri, out_buffer.getvalue())

        return target_uri

    def _rewrite_manifest(self, source_uri: str, stats: RewriteStats) -> str:
        """
        Read a manifest Avro, rewrite all data_file.file_path entries.
        Write to target.
        """
        log.debug(f"Rewriting manifest: {source_uri}")
        raw_bytes = self._source.read_bytes(source_uri)
        reader = fastavro.reader(io.BytesIO(raw_bytes))
        schema = reader.writer_schema

        # Capture the Avro metadata (Iceberg stores format-version, schema,
        # partition-spec, etc. as Avro file-level metadata)
        avro_metadata = dict(reader.metadata) if hasattr(reader, "metadata") else {}

        records = list(reader)

        for record in records:
            # Manifest entries have a 'data_file' struct (Iceberg v2)
            # or the fields are top-level (Iceberg v1).
            data_file = record.get("data_file", record)

            if "file_path" in data_file:
                original = data_file["file_path"]
                data_file["file_path"] = self._translator.translate(original)
                stats.data_file_paths_translated += 1

        # Write rewritten manifest to target
        target_uri = self._translator.translate(source_uri)
        out_buffer = io.BytesIO()
        fastavro.writer(out_buffer, schema, records, metadata=avro_metadata)
        self._target.write_bytes(target_uri, out_buffer.getvalue())

        stats.manifests_rewritten += 1
        return target_uri

    def _write_version_hint(self, source_metadata_uri: str, target_metadata_uri: str):
        """
        Write version-hint.text to the target table's metadata directory.

        This file contains a single integer — the version number of the current
        metadata file.  Iceberg Hadoop catalog reads this to find the latest
        metadata.json without listing the metadata directory.

        Written LAST so the target table only becomes readable once all
        manifests and data files are in place.
        """
        # Extract version number from filename: v3.metadata.json → 3
        import re
        filename = target_metadata_uri.rsplit("/", 1)[-1]
        match = re.match(r"v(\d+)\.metadata\.json", filename)
        if not match:
            log.warning(f"Cannot extract version from metadata filename: {filename}")
            return

        version = match.group(1)

        # version-hint.text lives in the same metadata/ directory
        metadata_dir = target_metadata_uri.rsplit("/", 1)[0]
        hint_uri = f"{metadata_dir}/version-hint.text"

        self._target.write_text(hint_uri, version)
        log.info(f"Wrote version-hint.text: {hint_uri} → version {version}")


def find_latest_metadata(storage: StorageBackend, table_root: str) -> str:
    """
    Find the latest metadata.json for an Iceberg table by reading
    version-hint.text or scanning the metadata directory.

    Args:
        storage:    StorageBackend to read from.
        table_root: Table root URI (e.g. s3://warehouse/iceberg/gold/top_customers/)

    Returns:
        Full URI to the latest metadata.json.
    """
    metadata_dir = table_root.rstrip("/") + "/metadata/"

    # Try version-hint.text first (fast path)
    hint_uri = metadata_dir + "version-hint.text"
    try:
        version = storage.read_text(hint_uri).strip()
        metadata_uri = f"{metadata_dir}v{version}.metadata.json"
        if storage.exists(metadata_uri):
            return metadata_uri
    except Exception:
        pass

    # Fallback: scan for the highest-versioned metadata file
    import re
    highest_version = -1
    highest_uri = ""

    for file_info in storage.list_objects(metadata_dir):
        match = re.search(r"v(\d+)\.metadata\.json$", file_info.uri)
        if match:
            v = int(match.group(1))
            if v > highest_version:
                highest_version = v
                highest_uri = file_info.uri

    if highest_version < 0:
        raise FileNotFoundError(
            f"No Iceberg metadata found in {metadata_dir}. "
            f"Is this a valid Iceberg table?"
        )

    return highest_uri
