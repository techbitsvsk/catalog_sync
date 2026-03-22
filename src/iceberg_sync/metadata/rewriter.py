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
            ("abfss://iceberg@acct.dfs.core.windows.net/", "s3a://warehouse/"),
        ])

        rewriter = MetadataRewriter(
            translator=translator,
            source_storage=adls_backend,
            target_storage=minio_backend,
            # write_version_hint=False when Nessie (or any REST catalog) is the
            # target catalog — the catalog server tracks the current metadata
            # pointer, not version-hint.text on disk.
            write_version_hint=False,
        )

        stats = rewriter.rewrite_table(
            source_metadata_uri="abfss://iceberg@acct.../gold/top_customers/metadata/00003-abc.metadata.json"
        )
    """

    def __init__(
        self,
        translator: PathTranslator,
        source_storage: StorageBackend,
        target_storage: StorageBackend,
        rewrite_all_snapshots: bool = False,
        write_version_hint: bool = True,
    ):
        self._translator = translator
        self._source = source_storage
        self._target = target_storage
        self._rewrite_all = rewrite_all_snapshots
        self._write_version_hint_flag = write_version_hint

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
        # Capture original URIs BEFORE translating — needed to read the source
        # files when we rewrite historical metadata below.
        historical_metadata_uris = [
            entry["metadata-file"]
            for entry in metadata.get("metadata-log", [])
            if "metadata-file" in entry
        ]
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

        # ── Rewrite historical metadata files referenced in metadata-log ────
        # Iceberg 1.4+ reads these to reconstruct lastAddedSchemaId.  Without
        # them on the target the table appears schema-less to Spark.
        for hist_uri in historical_metadata_uris:
            self._rewrite_historical_metadata(hist_uri, stats)

        # ── Write version-hint.text (atomic pointer) ─────────────────────
        self._write_version_hint(target_metadata_uri)

        log.info(
            f"Rewrite complete: {stats.metadata_files_rewritten} metadata, "
            f"{stats.manifest_lists_rewritten} manifest-lists, "
            f"{stats.manifests_rewritten} manifests, "
            f"{stats.data_file_paths_translated} data file paths translated"
        )
        return stats

    def _rewrite_historical_metadata(self, source_uri: str, stats: RewriteStats) -> None:
        """
        Translate paths in a historical metadata.json and write it to the target.

        Historical metadata files are referenced in metadata-log.  Iceberg 1.4+
        reads them during table load to reconstruct lastAddedSchemaId.  We only
        translate URI fields — we do NOT re-process manifests (the raw Avro files
        were already copied by the data-file sync step).

        Skips silently if the file already exists on the target or cannot be read
        from the source (e.g. retention-deleted on the source).
        """
        target_uri = self._translator.translate(source_uri, strict=False)
        if target_uri == source_uri:
            log.debug(f"Cannot translate historical metadata URI, skipping: {source_uri}")
            return

        try:
            if self._target.exists(target_uri):
                log.debug(f"Historical metadata already on target: {target_uri}")
                return
        except Exception:
            pass

        try:
            raw = self._source.read_bytes(source_uri)
        except Exception as e:
            log.debug(f"Cannot read historical metadata {source_uri}: {e} — skipping")
            return

        try:
            hist_meta = json.loads(raw)
        except Exception as e:
            log.debug(f"Cannot parse historical metadata {source_uri}: {e} — skipping")
            return

        if "location" in hist_meta:
            hist_meta["location"] = self._translator.translate(
                hist_meta["location"], strict=False
            )
        for snap in hist_meta.get("snapshots", []):
            if "manifest-list" in snap:
                snap["manifest-list"] = self._translator.translate(
                    snap["manifest-list"], strict=False
                )
        for entry in hist_meta.get("metadata-log", []):
            if "metadata-file" in entry:
                entry["metadata-file"] = self._translator.translate(
                    entry["metadata-file"], strict=False
                )

        self._target.write_bytes(
            target_uri,
            json.dumps(hist_meta, indent=2, default=str).encode("utf-8"),
        )
        stats.metadata_files_rewritten += 1
        log.debug(f"Wrote historical metadata: {target_uri}")

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

    def _write_version_hint(self, target_metadata_uri: str) -> None:
        """
        Optionally write version-hint.text for Hadoop/filesystem catalog targets.

        version-hint.text is a Hadoop catalog convention — it contains a single
        integer that points to the current v{N}.metadata.json without requiring
        a directory listing.  It does NOT exist in tables managed by REST catalogs
        (Nessie, Glue, Polaris, Unity Catalog, Azure Fabric).

        Skip this entirely when self._write_version_hint is False, which should
        be the case whenever a REST catalog (e.g. Nessie) is the authoritative
        pointer — writing the file would be misleading and wasteful.

        Handles both metadata naming schemes:
          v{N}.metadata.json             → writes N
          {NNNNN}-{UUID}.metadata.json   → writes the 5-digit sequence number
                                           (stripped of leading zeros)
        """
        if not self._write_version_hint_flag:
            log.debug("Skipping version-hint.text — REST catalog target, pointer lives in catalog")
            return

        import re
        filename = target_metadata_uri.rsplit("/", 1)[-1]

        # Modern Iceberg naming: 00003-550e8400-....metadata.json → "3"
        modern = re.match(r"^(\d{5})-[0-9a-f\-]{36}\.metadata\.json$", filename, re.IGNORECASE)
        if modern:
            version = str(int(modern.group(1)))  # strip leading zeros
        else:
            # Hadoop catalog naming: v3.metadata.json → "3"
            legacy = re.match(r"^v(\d+)\.metadata\.json$", filename)
            if legacy:
                version = legacy.group(1)
            else:
                log.warning(
                    f"Cannot determine sequence number from metadata filename '{filename}'. "
                    f"version-hint.text will not be written. "
                    f"If this table is managed by a REST catalog this is expected and harmless."
                )
                return

        metadata_dir = target_metadata_uri.rsplit("/", 1)[0]
        hint_uri = f"{metadata_dir}/version-hint.text"
        self._target.write_text(hint_uri, version)
        log.info(f"Wrote version-hint.text: {hint_uri} → {version}")


def find_latest_metadata(storage: StorageBackend, table_root: str) -> str:
    """
    Find the latest metadata.json for an Iceberg table.

    Handles all Iceberg catalog naming conventions:

    1. Hadoop/filesystem catalog (legacy HadoopCatalog):
         version-hint.text contains "3"  →  v3.metadata.json
         Fast path — a single read, no directory scan needed.

    2. Modern Iceberg writers (SparkCatalog, REST catalog, Nessie, Polaris,
       Unity Catalog, AWS Glue, Azure Fabric):
         00000-550e8400-e29b-41d4-a716-446655440000.metadata.json
         00001-6ba7b810-9dad-11d1-80b4-00c04fd430c8.metadata.json
         The 5-digit prefix is the write-sequence counter; the highest wins.

    3. Unrecognised / mixed naming — falls back to reading last-updated-ms
       from each file to break ties.

    Note: when the source table is managed by a REST catalog the authoritative
    current-metadata pointer lives in the catalog server, not on disk.  For the
    highest fidelity, supply metadata_location obtained from the catalog directly
    and bypass this function entirely.
    """
    import json
    import re

    metadata_dir = table_root.rstrip("/") + "/metadata/"

    # ── 1. Fast path: Hadoop catalog version-hint.text ───────────────────────
    hint_uri = metadata_dir + "version-hint.text"
    try:
        raw = storage.read_text(hint_uri).strip()
        if raw.isdigit():
            candidate = f"{metadata_dir}v{raw}.metadata.json"
            if storage.exists(candidate):
                log.debug(f"Hadoop catalog: version-hint.text → {candidate}")
                return candidate
            log.debug(
                f"version-hint.text points to missing file {candidate}, "
                f"falling back to directory scan"
            )
    except Exception:
        pass  # version-hint.text absent — not a Hadoop catalog table

    # ── 2. General scan: all .metadata.json files ────────────────────────────
    all_meta = [
        fi for fi in storage.list_objects(metadata_dir)
        if fi.uri.endswith(".metadata.json")
    ]

    if not all_meta:
        raise FileNotFoundError(
            f"No Iceberg metadata found in {metadata_dir}. "
            f"Is this a valid Iceberg table root?  "
            f"If this table is managed by a REST catalog (Nessie, Glue, Polaris, "
            f"Azure Fabric) supply metadata_location from the catalog directly "
            f"instead of relying on filesystem discovery."
        )

    # ── 3. Rank by naming-scheme sequence number ──────────────────────────────
    # Modern:  00003-<uuid>.metadata.json  → seq 3
    # Legacy:  v3.metadata.json            → seq 3
    _MODERN = re.compile(
        r"/(\d{5})-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        r"\.metadata\.json$",
        re.IGNORECASE,
    )
    _LEGACY = re.compile(r"/v(\d+)\.metadata\.json$")

    def _seq(uri: str) -> int:
        m = _MODERN.search(uri)
        if m:
            return int(m.group(1))
        m = _LEGACY.search(uri)
        if m:
            return int(m.group(1))
        return -1

    ranked = sorted(all_meta, key=lambda fi: _seq(fi.uri), reverse=True)
    top_seq = _seq(ranked[0].uri)

    if top_seq < 0:
        # Unrecognised naming — read last-updated-ms from every file
        log.warning(
            f"Metadata files in {metadata_dir} use unrecognised naming "
            f"(neither v{{N}}.metadata.json nor {{NNNNN}}-{{UUID}}.metadata.json). "
            f"Reading last-updated-ms from each file to determine the latest."
        )
        best_uri, best_ts = "", -1
        for fi in all_meta:
            try:
                ts = json.loads(storage.read_bytes(fi.uri)).get("last-updated-ms", 0)
                if ts > best_ts:
                    best_ts, best_uri = ts, fi.uri
            except Exception:
                continue
        if not best_uri:
            raise FileNotFoundError(f"Could not determine latest metadata in {metadata_dir}")
        return best_uri

    # Multiple files at the top sequence number (shouldn't happen, but defensive)
    top_group = [fi for fi in ranked if _seq(fi.uri) == top_seq]
    if len(top_group) == 1:
        log.debug(f"Latest metadata (seq={top_seq}): {top_group[0].uri}")
        return top_group[0].uri

    log.debug(f"{len(top_group)} files at seq={top_seq}, reading last-updated-ms to break tie")
    best_uri, best_ts = "", -1
    for fi in top_group:
        try:
            ts = json.loads(storage.read_bytes(fi.uri)).get("last-updated-ms", 0)
            if ts > best_ts:
                best_ts, best_uri = ts, fi.uri
        except Exception:
            continue
    return best_uri or top_group[0].uri
