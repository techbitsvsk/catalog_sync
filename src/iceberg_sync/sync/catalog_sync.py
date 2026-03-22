"""
sync/catalog_sync.py — Orchestrates full Iceberg table sync across clouds.

This is the main entry point for syncing an Iceberg table.  The sync process:

    1. Discover:  Find the latest metadata.json on the source.
                  Either via find_latest_metadata() (filesystem scan, default)
                  or via a caller-supplied metadata_location (REST catalog hybrid).
    2. Diff:      Read the current snapshot's manifest chain to discover the
                  exact set of data files the snapshot references, then compare
                  against the target.  Only genuinely missing files are copied.
    3. Copy:      Transfer missing data files from source to target.
    4. Rewrite:   Rewrite metadata chain (metadata.json → manifest-lists →
                  manifests) with translated storage paths.
    5. Commit:    Write version-hint.text on target for Hadoop catalog targets.
                  Skipped automatically when the target catalog is a REST catalog.

The ordering guarantees consistency: data files are in place before metadata
references them.  If the sync fails mid-copy the target retains its previous
valid state — the metadata pointer is never advanced until all referenced files
are confirmed present.

Design decisions
────────────────
•  Manifest-based diff: data file paths come from the manifest chain, not from a
   directory listing.  This handles tables with custom write.data.path and
   correctly includes Iceberg v2 delete files alongside data files.
•  Abort on copy failure: if any file copy fails the metadata rewrite is skipped
   entirely.  The target keeps its previous valid state.
•  Thread-safe counters: a lock protects result.files_copied / bytes_copied when
   max_parallel_copies > 1.
•  Orphan cleanup: cleanup_orphan_files() removes files on the target that are no
   longer referenced by any snapshot — safe to call after a successful sync.
•  Dry-run mode: shows what would be copied without doing it.
•  The sync is table-scoped: sync one table at a time.  To sync a namespace,
   iterate over tables (see CatalogSync.sync_namespace).
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from iceberg_sync.metadata.reader import read_all_snapshot_files, read_snapshot_data_files
from iceberg_sync.metadata.rewriter import MetadataRewriter, RewriteStats, find_latest_metadata
from iceberg_sync.path_translator import PathTranslator
from iceberg_sync.storage.base import FileInfo, StorageBackend

log = logging.getLogger(__name__)


@dataclass
class SyncResult:
    """Full result of a table sync operation."""
    table: str
    source_metadata_uri: str
    target_metadata_uri: str
    files_copied: int = 0
    files_skipped: int = 0
    bytes_copied: int = 0
    rewrite_stats: Optional[RewriteStats] = None
    duration_seconds: float = 0.0
    dry_run: bool = False
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


@dataclass
class CleanupResult:
    """Result of an orphan file cleanup operation."""
    table: str
    files_scanned: int = 0
    files_referenced: int = 0
    orphans_found: int = 0
    orphans_bytes: int = 0
    files_deleted: int = 0
    bytes_deleted: int = 0
    dry_run: bool = False
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


class CatalogSync:
    """
    Syncs Iceberg tables between cloud storage backends.

    Usage:
        from iceberg_sync.path_translator import aws_to_azure
        from iceberg_sync.storage import create_storage

        translator = PathTranslator([
            ("s3://my-warehouse/iceberg/", "abfss://iceberg@account.dfs.core.windows.net/iceberg/"),
        ])

        sync = CatalogSync(
            translator=translator,
            source_storage=create_storage("s3", region_name="eu-west-2"),
            target_storage=create_storage("abfss", storage_account_name="account"),
        )

        # Sync a single table
        result = sync.sync_table("s3://my-warehouse/iceberg/gold/top_customers/")

        # Sync all tables in a namespace
        results = sync.sync_namespace("s3://my-warehouse/iceberg/gold/")

        # Clean up orphan files on the target after sync
        cleanup = sync.cleanup_orphan_files("s3://my-warehouse/iceberg/gold/top_customers/")
    """

    def __init__(
        self,
        translator: PathTranslator,
        source_storage: StorageBackend,
        target_storage: StorageBackend,
        max_parallel_copies: int = 4,
        rewrite_all_snapshots: bool = False,
        write_version_hint: bool = True,
    ):
        self._translator = translator
        self._source = source_storage
        self._target = target_storage
        self._max_parallel = max_parallel_copies
        self._rewrite_all_snapshots = rewrite_all_snapshots
        self._write_version_hint = write_version_hint

    def sync_table(
        self,
        table_root: str,
        *,
        dry_run: bool = False,
        metadata_location: Optional[str] = None,
    ) -> SyncResult:
        """
        Sync a single Iceberg table from source to target.

        Args:
            table_root:         Table root URI on source
                                (e.g. s3://warehouse/iceberg/gold/top_customers/)
            dry_run:            If True, report what would be done without copying.
            metadata_location:  Explicit URI to the source metadata.json.
                                When supplied, filesystem discovery is skipped
                                entirely — useful when the caller has obtained the
                                authoritative pointer from a REST catalog endpoint
                                (Azure Fabric Iceberg API, AWS Glue, etc.).
                                When omitted, find_latest_metadata() is used.

        Returns:
            SyncResult with detailed counters.
        """
        start = time.monotonic()
        result = SyncResult(
            table=table_root,
            source_metadata_uri="",
            target_metadata_uri="",
            dry_run=dry_run,
        )

        try:
            # ── Step 1: Discover latest metadata ─────────────────────────────
            if metadata_location:
                metadata_uri = metadata_location
                log.info(f"Using supplied metadata location: {metadata_uri}")
            else:
                log.info(f"Discovering metadata for {table_root}")
                metadata_uri = find_latest_metadata(self._source, table_root)
                log.info(f"Latest metadata: {metadata_uri}")

            result.source_metadata_uri = metadata_uri
            result.target_metadata_uri = self._translator.translate(metadata_uri)

            # ── Step 2: Read current snapshot's file set from manifests ───────
            # This is the ground truth for which files need to exist on the
            # target.  It handles custom data paths and Iceberg v2 delete files.
            log.info("Reading current snapshot file set from manifests...")
            raw_metadata = self._source.read_bytes(metadata_uri)
            metadata = json.loads(raw_metadata)

            source_files = read_snapshot_data_files(
                self._source, metadata, table_root
            )
            log.info(f"Current snapshot references {len(source_files)} files")

            # ── Step 3: Diff against target ───────────────────────────────────
            # Translate each source URI to its target counterpart and check
            # existence.  One list call on the target then a set lookup.
            target_table_root = self._translator.translate(table_root)
            target_existing: Set[str] = set()
            try:
                for fi in self._target.list_objects(target_table_root):
                    target_existing.add(fi.uri)
            except Exception:
                pass  # Target may not exist yet — all files will be copied

            files_to_copy: Dict[str, FileInfo] = {}
            for rel, fi in source_files.items():
                target_uri = self._translator.translate(fi.uri)
                if target_uri not in target_existing:
                    files_to_copy[rel] = fi

            result.files_skipped = len(source_files) - len(files_to_copy)
            log.info(
                f"Files: {len(source_files)} in snapshot, "
                f"{len(files_to_copy)} to copy, "
                f"{result.files_skipped} already synced"
            )

            if dry_run:
                result.files_copied = len(files_to_copy)
                result.bytes_copied = sum(fi.size_bytes for fi in files_to_copy.values())
                log.info(
                    f"DRY RUN: would copy {result.files_copied} files "
                    f"({result.bytes_copied / 1024 / 1024:.1f} MB)"
                )
                result.duration_seconds = time.monotonic() - start
                return result

            # ── Step 4: Copy missing data / delete files ──────────────────────
            if files_to_copy:
                log.info(f"Copying {len(files_to_copy)} files...")
                self._copy_files_by_uri(files_to_copy, result)

            # ── Abort if any copy failed ──────────────────────────────────────
            # Writing metadata that references files which failed to copy would
            # leave the target in a broken state.  Stop here and preserve the
            # previous valid state on the target.
            if result.errors:
                log.error(
                    f"{len(result.errors)} copy error(s) — aborting metadata rewrite "
                    f"to preserve target consistency. Fix errors and re-run."
                )
                result.duration_seconds = round(time.monotonic() - start, 3)
                return result

            # ── Step 5: Sync raw metadata files (manifests not yet rewritten) ─
            # Copy any metadata directory files that are missing on the target.
            # These are raw Avro manifests for historical snapshots.  The current
            # snapshot's manifests will be overwritten by the rewriter below.
            source_meta_prefix = table_root.rstrip("/") + "/metadata/"
            target_meta_prefix = self._translator.translate(source_meta_prefix)

            source_meta_files = {
                fi.relative_path: fi
                for fi in self._source.list_objects(source_meta_prefix)
            }
            target_meta_files: Set[str] = set()
            try:
                target_meta_files = {
                    fi.relative_path
                    for fi in self._target.list_objects(target_meta_prefix)
                }
            except Exception:
                pass

            meta_to_copy = {
                rel: fi for rel, fi in source_meta_files.items()
                if rel not in target_meta_files
                and not rel.endswith(".metadata.json")   # rewritten below
                and rel != "version-hint.text"           # written last
            }

            if meta_to_copy:
                log.info(f"Copying {len(meta_to_copy)} raw metadata files...")
                self._copy_files_by_prefix(meta_to_copy, source_meta_prefix, target_meta_prefix, result)

            # Abort again if raw metadata copy had errors
            if result.errors:
                log.error("Metadata file copy errors — aborting rewrite.")
                result.duration_seconds = round(time.monotonic() - start, 3)
                return result

            # ── Step 6: Rewrite metadata chain ───────────────────────────────
            log.info("Rewriting metadata with translated paths...")
            rewriter = MetadataRewriter(
                translator=self._translator,
                source_storage=self._source,
                target_storage=self._target,
                rewrite_all_snapshots=self._rewrite_all_snapshots,
                write_version_hint=self._write_version_hint,
            )
            result.rewrite_stats = rewriter.rewrite_table(metadata_uri)

        except Exception as e:
            log.error(f"Sync failed for {table_root}: {e}")
            result.errors.append(str(e))

        result.duration_seconds = round(time.monotonic() - start, 3)
        return result

    def sync_namespace(
        self,
        namespace_root: str,
        *,
        dry_run: bool = False,
    ) -> List[SyncResult]:
        """
        Discover and sync all Iceberg tables under a namespace root.

        Iceberg Hadoop catalog stores each table as a subdirectory with a
        metadata/ subdirectory inside.  We scan for metadata/ directories
        to discover tables.

        Args:
            namespace_root: URI prefix for the namespace
                (e.g. s3://warehouse/iceberg/gold/)
        """
        log.info(f"Discovering tables under {namespace_root}")

        table_roots: Set[str] = set()
        for fi in self._source.list_objects(namespace_root):
            if "/metadata/" in fi.uri and fi.uri.endswith(".metadata.json"):
                table_root = fi.uri.split("/metadata/")[0] + "/"
                table_roots.add(table_root)

        log.info(f"Found {len(table_roots)} tables: {sorted(table_roots)}")

        results = []
        for table_root in sorted(table_roots):
            log.info(f"Syncing table: {table_root}")
            result = self.sync_table(table_root, dry_run=dry_run)
            results.append(result)

            if result.success:
                paths = result.rewrite_stats.data_file_paths_translated if result.rewrite_stats else 0
                log.info(
                    f"  ✓ {table_root}: {result.files_copied} files copied, "
                    f"{paths} paths translated"
                )
            else:
                log.error(f"  ✗ {table_root}: {result.errors}")

        return results

    def cleanup_orphan_files(
        self,
        table_root: str,
        *,
        dry_run: bool = False,
    ) -> CleanupResult:
        """
        Remove files on the target that are not referenced by any snapshot.

        Safe to call after a successful sync.  Reads the target metadata chain,
        builds the complete set of referenced files across all snapshots, then
        deletes anything on the target storage that is not in that set.

        Files in the metadata/ directory are always preserved.

        Args:
            table_root: Table root URI on the **source** (will be translated).
            dry_run:    If True, log what would be deleted without deleting.

        Returns:
            CleanupResult with counters.
        """
        target_table_root = self._translator.translate(table_root)
        result = CleanupResult(table=target_table_root, dry_run=dry_run)

        try:
            # Find latest metadata on the target
            target_metadata_uri = find_latest_metadata(self._target, target_table_root)
            raw = self._target.read_bytes(target_metadata_uri)
            metadata = json.loads(raw)

            # Collect ALL files referenced by ALL snapshots
            # (preserve time-travel data, not just current snapshot)
            log.info(f"Building referenced file set from all snapshots...")
            referenced = read_all_snapshot_files(self._target, metadata, target_table_root)
            referenced_uris: Set[str] = {fi.uri for fi in referenced.values()}

            # Always keep every file in the metadata/ directory
            target_meta_prefix = target_table_root.rstrip("/") + "/metadata/"
            for fi in self._target.list_objects(target_meta_prefix):
                referenced_uris.add(fi.uri)

            # List everything on the target under the table root
            all_files = list(self._target.list_objects(target_table_root))
            result.files_scanned = len(all_files)
            result.files_referenced = len(referenced_uris)

            orphans = [fi for fi in all_files if fi.uri not in referenced_uris]
            result.orphans_found = len(orphans)
            result.orphans_bytes = sum(fi.size_bytes for fi in orphans)

            log.info(
                f"Cleanup scan: {result.files_scanned} total files, "
                f"{result.files_referenced} referenced, "
                f"{result.orphans_found} orphans "
                f"({result.orphans_bytes / 1024 / 1024:.1f} MB)"
            )

            if dry_run:
                for fi in orphans:
                    log.info(f"DRY RUN: would delete orphan {fi.uri}")
                return result

            for fi in orphans:
                try:
                    self._target.delete(fi.uri)
                    result.files_deleted += 1
                    result.bytes_deleted += fi.size_bytes
                    log.info(f"Deleted orphan: {fi.uri}")
                except Exception as e:
                    msg = f"Failed to delete {fi.uri}: {e}"
                    log.warning(msg)
                    result.errors.append(msg)

            log.info(
                f"Cleanup complete: deleted {result.files_deleted} files "
                f"({result.bytes_deleted / 1024 / 1024:.1f} MB)"
            )

        except Exception as e:
            log.error(f"Cleanup failed for {target_table_root}: {e}")
            result.errors.append(str(e))

        return result

    # ── Internal copy helpers ─────────────────────────────────────────────────

    def _copy_files_by_uri(
        self,
        files: Dict[str, FileInfo],
        result: SyncResult,
    ) -> None:
        """
        Copy files where target URI is derived by translating the source URI.

        Used for data files / delete files discovered via manifest reading.
        Thread-safe counter updates via a lock.
        """
        lock = threading.Lock()

        def _copy_one(rel: str, fi: FileInfo) -> int:
            target_uri = self._translator.translate(fi.uri)
            self._target.copy_from(self._source, fi.uri, target_uri)
            return fi.size_bytes

        if self._max_parallel <= 1:
            for rel, fi in files.items():
                try:
                    size = _copy_one(rel, fi)
                    result.files_copied += 1
                    result.bytes_copied += size
                except Exception as e:
                    result.errors.append(f"Copy failed for {fi.uri}: {e}")
        else:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_parallel
            ) as executor:
                futures = {
                    executor.submit(_copy_one, rel, fi): (rel, fi)
                    for rel, fi in files.items()
                }
                for future in concurrent.futures.as_completed(futures):
                    rel, fi = futures[future]
                    try:
                        size = future.result()
                        with lock:
                            result.files_copied += 1
                            result.bytes_copied += size
                    except Exception as e:
                        result.errors.append(f"Copy failed for {fi.uri}: {e}")

    def _copy_files_by_prefix(
        self,
        files: Dict[str, FileInfo],
        source_prefix: str,
        target_prefix: str,
        result: SyncResult,
    ) -> None:
        """
        Copy files where target URI is constructed as target_prefix + relative_path.

        Used for raw metadata directory files (manifests, stats files) where the
        relative path is already known from a listing.
        """
        lock = threading.Lock()

        def _copy_one(rel_path: str, fi: FileInfo) -> int:
            target_uri = target_prefix + rel_path
            self._target.copy_from(self._source, fi.uri, target_uri)
            return fi.size_bytes

        if self._max_parallel <= 1:
            for rel_path, fi in files.items():
                try:
                    size = _copy_one(rel_path, fi)
                    with lock:
                        result.files_copied += 1
                        result.bytes_copied += size
                except Exception as e:
                    result.errors.append(f"Copy failed for {rel_path}: {e}")
        else:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_parallel
            ) as executor:
                futures = {
                    executor.submit(_copy_one, rel, fi): rel
                    for rel, fi in files.items()
                }
                for future in concurrent.futures.as_completed(futures):
                    rel = futures[future]
                    try:
                        size = future.result()
                        with lock:
                            result.files_copied += 1
                            result.bytes_copied += size
                    except Exception as e:
                        result.errors.append(f"Copy failed for {rel}: {e}")
