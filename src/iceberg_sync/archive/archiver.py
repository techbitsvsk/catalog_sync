"""
archive/archiver.py — Periodic archival of Iceberg snapshots to cold storage.

Workflow
────────
  1. Discover latest metadata.json on the primary (source) storage.
  2. Evaluate each snapshot against the retention policy
     (snapshot_manager.decide_snapshots).
  3. For each snapshot marked "archive":
       a. Walk its manifest chain (partition_scanner).
       b. Copy manifest-list, manifests, and data files to archive storage.
       c. Translate all URIs from source root to archive root.
  4. Write / update .archive-manifest.json in archive storage.
  5. If delete_after_archive=True, rewrite primary metadata.json to remove
     the archived snapshots (metadata_editor.expire_snapshots_in_metadata)
     and update version-hint.text / Nessie.
"""

from __future__ import annotations

import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import fastavro
import orjson

from iceberg_sync.archive.archive_index import ArchiveIndex, ArchivedSnapshotEntry
from iceberg_sync.archive.config import ArchiveJobConfig
from iceberg_sync.archive.metadata_editor import expire_snapshots_in_metadata
from iceberg_sync.archive.partition_scanner import scan_snapshot
from iceberg_sync.archive.snapshot_manager import (
    decide_snapshots,
    snapshots_to_archive,
)
from iceberg_sync.storage.base import StorageBackend
from iceberg_sync.storage.factory import create_storage

log = logging.getLogger(__name__)


@dataclass
class ArchiveResult:
    table: str
    snapshots_archived: int = 0
    snapshots_skipped: int = 0
    files_copied: int = 0
    bytes_copied: int = 0
    dry_run: bool = True
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.errors


class IcebergArchiver:
    """
    Archives Iceberg snapshots from primary storage to cold/archive storage.

    Instantiate via IcebergArchiver.from_config(cfg) or directly by supplying
    storage backends.
    """

    def __init__(
        self,
        source_storage: StorageBackend,
        archive_storage: StorageBackend,
        source_root: str,
        archive_root: str,
        older_than: str = "30d",
        min_snapshots_to_keep: int = 1,
        delete_after_archive: bool = True,
        parallelism: int = 8,
        nessie_catalog: Optional[Any] = None,  # NessieCatalog instance, optional
    ) -> None:
        self._source = source_storage
        self._archive = archive_storage
        self._source_root = source_root.rstrip("/")
        self._archive_root = archive_root.rstrip("/")
        self._older_than = older_than
        self._min_keep = min_snapshots_to_keep
        self._delete_after = delete_after_archive
        self._parallelism = parallelism
        self._nessie = nessie_catalog

    @classmethod
    def from_config(cls, cfg: ArchiveJobConfig) -> "IcebergArchiver":
        source_kwargs = cfg.source_s3.to_storage_kwargs() or cfg.source_adls.to_storage_kwargs()
        archive_kwargs = cfg.archive_s3.to_storage_kwargs() or cfg.archive_adls.to_storage_kwargs()

        source_storage = create_storage(cfg.source_root, **source_kwargs)
        archive_storage = create_storage(cfg.archive_root, **archive_kwargs)

        nessie = None
        if cfg.catalog.nessie_uri:
            from iceberg_sync.catalog.nessie import NessieCatalog
            from iceberg_sync.auth.oauth_client import OAuthClient

            auth = None
            if cfg.catalog.oauth_url:
                auth = OAuthClient(
                    token_url=cfg.catalog.oauth_url,
                    client_id=cfg.catalog.oauth_client_id or "",
                    client_secret=cfg.catalog.oauth_client_secret or "",
                )
            nessie = NessieCatalog(
                base_url=cfg.catalog.nessie_uri,
                ref=cfg.catalog.nessie_ref,
                oauth_client=auth,
            )

        return cls(
            source_storage=source_storage,
            archive_storage=archive_storage,
            source_root=cfg.source_root,
            archive_root=cfg.archive_root,
            older_than=cfg.older_than,
            min_snapshots_to_keep=cfg.min_snapshots_to_keep,
            delete_after_archive=cfg.delete_after_archive,
            parallelism=cfg.transfer.parallelism,
            nessie_catalog=nessie,
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def archive_table(self, table: str, *, dry_run: bool = True) -> ArchiveResult:
        result = ArchiveResult(table=table, dry_run=dry_run)
        table_source_root = f"{self._source_root}/{table}"

        try:
            metadata = self._load_metadata(table_source_root)
        except Exception as exc:
            result.errors.append(f"Cannot load metadata for {table}: {exc}")
            return result

        # Load existing archive index to skip already-archived snapshots
        index = ArchiveIndex.load_or_new(
            self._archive, self._archive_root, table, self._source_root
        )
        already_archived: Set[int] = set(index.snapshot_ids())

        decisions = decide_snapshots(
            metadata,
            self._older_than,
            self._min_keep,
            already_archived_ids=already_archived,
        )
        to_archive = snapshots_to_archive(decisions)

        if not to_archive:
            log.info("No snapshots to archive for %s.", table)
            return result

        log.info(
            "%s: %d snapshot(s) to archive (dry_run=%s).",
            table, len(to_archive), dry_run,
        )

        snapshots_done: List[int] = []

        for decision in to_archive:
            snap_id = decision.snapshot_id
            log.info("  Archiving snapshot %d (%s) ...", snap_id, decision.timestamp_iso)

            try:
                files_copied, bytes_copied = self._archive_snapshot(
                    metadata=metadata,
                    table=table,
                    snapshot_id=snap_id,
                    dry_run=dry_run,
                )
                result.files_copied += files_copied
                result.bytes_copied += bytes_copied
                result.snapshots_archived += 1
                snapshots_done.append(snap_id)

                if not dry_run:
                    # Update archive index
                    snap_raw = next(
                        s for s in metadata.get("snapshots", [])
                        if s.get("snapshot-id") == snap_id
                    )
                    summary = snap_raw.get("summary", {})
                    index.snapshots.append(ArchivedSnapshotEntry(
                        snapshot_id=snap_id,
                        timestamp_ms=decision.timestamp_ms,
                        operation=decision.operation,
                        added_data_files=int(summary.get("added-data-files", 0)),
                        added_records=int(summary.get("added-records", 0)),
                        data_files_count=files_copied,
                        size_bytes=bytes_copied,
                    ))

            except Exception as exc:
                log.error("Failed to archive snapshot %d: %s", snap_id, exc)
                result.errors.append(f"snapshot {snap_id}: {exc}")
                result.snapshots_skipped += 1

        if dry_run:
            return result

        # Persist archive index
        index.last_archived_at = datetime.now(timezone.utc).isoformat()
        index.schema = metadata.get("schemas", [{}])[0] if metadata.get("schemas") else None
        index.partition_spec = metadata.get("partition-specs")
        index.save(self._archive)

        # Expire archived snapshots from primary if requested
        if self._delete_after and snapshots_done:
            try:
                new_meta_uri = expire_snapshots_in_metadata(
                    metadata=metadata,
                    snapshot_ids_to_remove=set(snapshots_done),
                    table_root=table_source_root,
                    storage=self._source,
                )
                self._commit_metadata(table, table_source_root, new_meta_uri)
            except Exception as exc:
                result.errors.append(f"Expire-from-primary failed: {exc}")

        return result

    def archive_namespace(self, namespace: str, *, dry_run: bool = True) -> List[ArchiveResult]:
        ns_root = f"{self._source_root}/{namespace}"
        results: List[ArchiveResult] = []

        for fi in self._source.list_objects(ns_root):
            if fi.relative_path.endswith("metadata/version-hint.text"):
                # Derive table path: namespace/tablename
                parts = fi.relative_path.split("/")
                if len(parts) >= 3:
                    table = f"{namespace}/{parts[-3]}"
                    results.append(self.archive_table(table, dry_run=dry_run))

        return results

    # ── Private helpers ───────────────────────────────────────────────────────

    def _load_metadata(self, table_root: str) -> dict:
        version_hint_uri = f"{table_root}/metadata/version-hint.text"
        version = int(self._source.read_text(version_hint_uri).strip())
        meta_uri = f"{table_root}/metadata/v{version:05d}.metadata.json"
        return orjson.loads(self._source.read_bytes(meta_uri))

    def _archive_snapshot(
        self,
        metadata: dict,
        table: str,
        snapshot_id: int,
        dry_run: bool,
    ) -> tuple[int, int]:
        """Copy all files for snapshot_id to archive storage. Returns (files, bytes)."""
        files = scan_snapshot(
            storage=self._source,
            metadata=metadata,
            snapshot_id=snapshot_id,
            requested_partitions=[],  # all partitions
        )

        files_copied = 0
        bytes_copied = 0

        def _copy_file(scanned_file) -> int:
            archive_uri = scanned_file.file_path.replace(
                self._source_root, self._archive_root, 1
            )
            if dry_run:
                return scanned_file.file_size_bytes
            self._archive.copy_from(self._source, scanned_file.file_path, archive_uri)
            return scanned_file.file_size_bytes

        with ThreadPoolExecutor(max_workers=self._parallelism) as pool:
            futures = {pool.submit(_copy_file, f): f for f in files}
            for future in as_completed(futures):
                try:
                    bytes_copied += future.result()
                    files_copied += 1
                except Exception as exc:
                    log.warning("File copy failed: %s", exc)

        # Also archive the metadata files for this snapshot
        if not dry_run:
            self._archive_metadata_files(metadata, table, snapshot_id)

        return files_copied, bytes_copied

    def _archive_metadata_files(
        self, metadata: dict, table: str, snapshot_id: int
    ) -> None:
        """Copy the manifest-list and manifests for a snapshot to archive storage."""
        snap = next(
            (s for s in metadata.get("snapshots", []) if s.get("snapshot-id") == snapshot_id),
            None,
        )
        if snap is None:
            return

        ml_uri = snap.get("manifest-list", "")
        if ml_uri:
            archive_uri = ml_uri.replace(self._source_root, self._archive_root, 1)
            self._archive.copy_from(self._source, ml_uri, archive_uri)

            # Read manifest-list to copy individual manifests
            try:
                ml_data = self._source.read_bytes(ml_uri)
                for entry in fastavro.reader(io.BytesIO(ml_data)):
                    mpath = entry.get("manifest_path", "")
                    if mpath:
                        m_archive = mpath.replace(self._source_root, self._archive_root, 1)
                        self._archive.copy_from(self._source, mpath, m_archive)
            except Exception as exc:
                log.warning("Could not copy manifests for snapshot %d: %s", snapshot_id, exc)

    def _commit_metadata(self, table: str, table_root: str, new_meta_uri: str) -> None:
        """Update version-hint.text or Nessie after expiring snapshots."""
        if self._nessie:
            try:
                self._nessie.update(table, new_meta_uri)
                return
            except Exception as exc:
                log.warning("Nessie update failed, falling back to version-hint: %s", exc)

        # Extract version number from filename
        filename = new_meta_uri.split("/")[-1]
        version = int(filename.split(".")[0].lstrip("v"))
        hint_uri = f"{table_root}/metadata/version-hint.text"
        self._source.write_text(hint_uri, str(version))
