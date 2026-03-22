"""
sync/catalog_sync.py — Orchestrates full Iceberg table sync across clouds.

This is the main entry point for syncing an Iceberg table.  The sync process:

    1. Discover:  Find the latest metadata.json on the source.
    2. Diff:      Compare data files between source and target to find
                  files that need copying (incremental — only new files).
    3. Copy:      Transfer missing data files from source to target.
    4. Rewrite:   Rewrite metadata chain (metadata.json → manifest-lists →
                  manifests) with translated storage paths.
    5. Commit:    Write version-hint.text on target (atomic pointer).

The ordering guarantees consistency: data files are in place before
metadata references them.  If the sync fails mid-copy, the target still
has its previous valid state.

Design decisions
────────────────
•  Incremental by default: only new data files are copied.  Iceberg's
   immutability makes diffing trivial — if a file exists at the target
   with the same relative path, it's identical (Iceberg never mutates files).
•  Parallelism is opt-in via max_parallel_copies.  For cross-cloud transfers
   where bandwidth is the bottleneck, parallel copies help significantly.
•  Dry-run mode: shows what would be copied without doing it.
•  The sync is table-scoped: you sync one table at a time.  To sync an
   entire database/namespace, iterate over tables (see CatalogSync.sync_namespace).
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

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


class CatalogSync:
    """
    Syncs Iceberg tables between cloud storage backends.

    Usage:
        from iceberg_sync.path_translator import aws_to_azure
        from iceberg_sync.storage import create_storage

        translator = aws_to_azure(
            s3_warehouse="s3://my-warehouse/iceberg/",
            adls_warehouse="abfss://iceberg@account.dfs.core.windows.net/iceberg/",
        )

        sync = CatalogSync(
            translator=translator,
            source_storage=create_storage("s3", region_name="eu-west-2"),
            target_storage=create_storage("abfss", storage_account_name="account"),
        )

        # Sync a single table
        result = sync.sync_table("s3://my-warehouse/iceberg/gold/top_customers/")

        # Sync all tables in a namespace
        results = sync.sync_namespace("s3://my-warehouse/iceberg/gold/")
    """

    def __init__(
        self,
        translator: PathTranslator,
        source_storage: StorageBackend,
        target_storage: StorageBackend,
        max_parallel_copies: int = 4,
        rewrite_all_snapshots: bool = False,
    ):
        self._translator = translator
        self._source = source_storage
        self._target = target_storage
        self._max_parallel = max_parallel_copies
        self._rewrite_all_snapshots = rewrite_all_snapshots

    def sync_table(
        self,
        table_root: str,
        *,
        dry_run: bool = False,
    ) -> SyncResult:
        """
        Sync a single Iceberg table from source to target.

        Args:
            table_root: Table root URI on source
                        (e.g. s3://warehouse/iceberg/gold/top_customers/)
            dry_run:    If True, report what would be done without copying.

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
            # ── Step 1: Discover latest metadata ─────────────────────────
            log.info(f"Discovering metadata for {table_root}")
            metadata_uri = find_latest_metadata(self._source, table_root)
            result.source_metadata_uri = metadata_uri
            result.target_metadata_uri = self._translator.translate(metadata_uri)
            log.info(f"Latest metadata: {metadata_uri}")

            # ── Step 2: Diff data files ──────────────────────────────────
            log.info("Scanning source data files...")
            source_data_prefix = table_root.rstrip("/") + "/data/"
            source_files = {
                fi.relative_path: fi
                for fi in self._source.list_objects(source_data_prefix)
            }

            target_data_prefix = self._translator.translate(source_data_prefix)
            log.info("Scanning target data files...")
            target_files: Set[str] = set()
            try:
                target_files = {
                    fi.relative_path
                    for fi in self._target.list_objects(target_data_prefix)
                }
            except Exception:
                # Target prefix might not exist yet
                pass

            files_to_copy = {
                rel_path: fi
                for rel_path, fi in source_files.items()
                if rel_path not in target_files
            }

            result.files_skipped = len(source_files) - len(files_to_copy)
            log.info(
                f"Data files: {len(source_files)} source, "
                f"{len(target_files)} target, "
                f"{len(files_to_copy)} to copy, "
                f"{result.files_skipped} already synced"
            )

            if dry_run:
                result.files_copied = len(files_to_copy)
                result.bytes_copied = sum(fi.size_bytes for fi in files_to_copy.values())
                log.info(f"DRY RUN: would copy {result.files_copied} files "
                         f"({result.bytes_copied / 1024 / 1024:.1f} MB)")
                result.duration_seconds = time.monotonic() - start
                return result

            # ── Step 3: Copy data files ──────────────────────────────────
            if files_to_copy:
                log.info(f"Copying {len(files_to_copy)} data files...")
                self._copy_files(
                    files_to_copy, source_data_prefix, target_data_prefix, result
                )

            # ── Step 4: Copy metadata directory files (not metadata.json) ─
            # Iceberg also stores Avro manifests in the metadata/ directory.
            # We copy the raw source files first — the rewriter will then
            # read from source, rewrite, and overwrite on target.
            source_meta_prefix = table_root.rstrip("/") + "/metadata/"
            target_meta_prefix = self._translator.translate(source_meta_prefix)

            log.info("Syncing metadata directory files...")
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
                and not rel.endswith(".metadata.json")  # Will be rewritten
                and rel != "version-hint.text"          # Written last
            }

            if meta_to_copy:
                log.info(f"Copying {len(meta_to_copy)} raw metadata files...")
                self._copy_files(meta_to_copy, source_meta_prefix, target_meta_prefix, result)

            # ── Step 5: Rewrite metadata chain ───────────────────────────
            log.info("Rewriting metadata with translated paths...")
            rewriter = MetadataRewriter(
                translator=self._translator,
                source_storage=self._source,
                target_storage=self._target,
                rewrite_all_snapshots=self._rewrite_all_snapshots,
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

        # Find all metadata directories → each parent is a table root
        table_roots: Set[str] = set()
        for fi in self._source.list_objects(namespace_root):
            if "/metadata/v" in fi.uri and fi.uri.endswith(".metadata.json"):
                # Extract table root: everything before /metadata/
                table_root = fi.uri.split("/metadata/")[0] + "/"
                table_roots.add(table_root)

        log.info(f"Found {len(table_roots)} tables: {sorted(table_roots)}")

        results = []
        for table_root in sorted(table_roots):
            log.info(f"Syncing table: {table_root}")
            result = self.sync_table(table_root, dry_run=dry_run)
            results.append(result)

            if result.success:
                log.info(
                    f"  ✓ {table_root}: {result.files_copied} files copied, "
                    f"{result.rewrite_stats.data_file_paths_translated if result.rewrite_stats else 0} paths translated"
                )
            else:
                log.error(f"  ✗ {table_root}: {result.errors}")

        return results

    def _copy_files(
        self,
        files: Dict[str, FileInfo],
        source_prefix: str,
        target_prefix: str,
        result: SyncResult,
    ):
        """Copy files from source to target, optionally in parallel."""

        def _copy_one(rel_path: str, fi: FileInfo):
            target_uri = target_prefix + rel_path
            self._target.copy_from(self._source, fi.uri, target_uri)
            return fi.size_bytes

        if self._max_parallel <= 1:
            for rel_path, fi in files.items():
                size = _copy_one(rel_path, fi)
                result.files_copied += 1
                result.bytes_copied += size
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
                        result.files_copied += 1
                        result.bytes_copied += size
                    except Exception as e:
                        result.errors.append(f"Copy failed for {rel}: {e}")
