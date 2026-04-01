"""
archive/restorer.py — Restore a full table or specific partitions from cold storage.

Workflow
────────
  Step 1  list_snapshots()  — inspect the archive index; show available restore points.
  Step 2  plan()            — dry-run: scan partitions, detect conflicts, print plan.
  Step 3  execute(plan)     — copy files + reconstruct metadata + commit pointer.

The plan object is the contract between steps 2 and 3: what you see printed
is exactly what execute() will do — no extra decisions are made at execution time.
"""

from __future__ import annotations

import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import fastavro
import orjson

from iceberg_sync.archive.archive_index import ArchiveIndex, ArchivedSnapshotEntry
from iceberg_sync.archive.config import RestoreJobConfig
from iceberg_sync.archive.metadata_editor import (
    splice_manifests,
    write_table_metadata,
)
from iceberg_sync.archive.partition_scanner import ScannedFile, scan_snapshot
from iceberg_sync.archive.restore_planner import RestorePlan, RestorePlanner
from iceberg_sync.storage.base import StorageBackend
from iceberg_sync.storage.factory import create_storage

log = logging.getLogger(__name__)


@dataclass
class RestoreResult:
    table: str
    restore_as: Optional[str]
    mode: str
    snapshot_id: int
    files_copied: int = 0
    bytes_copied: int = 0
    new_metadata_uri: str = ""
    dry_run: bool = True
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.errors

    @property
    def effective_table(self) -> str:
        return self.restore_as or self.table


class IcebergRestorer:
    """
    Restores Iceberg table data from cold/archive storage back to the primary catalog.

    Typical usage:
        cfg = RestoreJobConfig.from_yaml("restore.yaml")
        restorer = IcebergRestorer.from_config(cfg)

        plan = restorer.plan()      # always review first
        plan.print()

        result = restorer.execute(plan)
    """

    def __init__(
        self,
        archive_storage: StorageBackend,
        target_storage: StorageBackend,
        config: RestoreJobConfig,
        nessie_catalog: Optional[Any] = None,
    ) -> None:
        self._archive = archive_storage
        self._target = target_storage
        self._cfg = config
        self._nessie = nessie_catalog

    @classmethod
    def from_config(cls, cfg: RestoreJobConfig) -> "IcebergRestorer":
        archive_kwargs = cfg.archive_s3.to_storage_kwargs() or cfg.archive_adls.to_storage_kwargs()
        target_kwargs = cfg.target_s3.to_storage_kwargs() or cfg.target_adls.to_storage_kwargs()

        archive_storage = create_storage(cfg.archive_root, **archive_kwargs)
        target_storage = create_storage(cfg.target_root, **target_kwargs)

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
            archive_storage=archive_storage,
            target_storage=target_storage,
            config=cfg,
            nessie_catalog=nessie,
        )

    # ── Step 1: List available snapshots ─────────────────────────────────────

    def list_snapshots(self) -> List[ArchivedSnapshotEntry]:
        """
        Return all snapshots available in the archive index for this table.
        Prints a formatted table to the console.
        """
        from rich.console import Console
        from rich.table import Table
        from rich import box

        index = ArchiveIndex.load(self._archive, self._cfg.archive_root, self._cfg.table)

        c = Console()
        t = Table(
            "Snapshot ID", "Timestamp (UTC)", "Operation", "Files", "Size",
            box=box.SIMPLE_HEAVY, header_style="bold cyan",
            title=f"Archived snapshots — {self._cfg.table}",
        )

        def _fmt_bytes(n: int) -> str:
            for unit in ("B", "KB", "MB", "GB", "TB"):
                if n < 1024:
                    return f"{n:.1f} {unit}"
                n //= 1024
            return f"{n} PB"

        for snap in sorted(index.snapshots, key=lambda s: s.timestamp_ms, reverse=True):
            t.add_row(
                str(snap.snapshot_id),
                snap.timestamp_iso,
                snap.operation,
                str(snap.data_files_count),
                _fmt_bytes(snap.size_bytes),
            )

        c.print(t)
        return index.snapshots

    # ── Step 2: Build plan (dry-run) ──────────────────────────────────────────

    def plan(self) -> RestorePlan:
        """
        Analyse the archive and build a RestorePlan.

        This method never writes anything.  Call plan.print() to display
        the plan, then pass it to execute() to apply.
        """
        cfg = self._cfg

        # Load archive index + resolve target snapshot
        index = ArchiveIndex.load(self._archive, cfg.archive_root, cfg.table)
        entry = index.find_snapshot(
            snapshot_id=cfg.snapshot_id,
            as_of_iso=cfg.as_of,
        )
        if entry is None:
            raise ValueError(
                f"No archived snapshot found for table '{cfg.table}' "
                f"(as_of={cfg.as_of!r}, snapshot_id={cfg.snapshot_id!r}). "
                "Run list_snapshots() to see what is available."
            )

        # Load archived metadata.json to access the manifest chain
        archived_metadata = self._load_archived_metadata(cfg.table)

        # Scan partitions
        files = scan_snapshot(
            storage=self._archive,
            metadata=archived_metadata,
            snapshot_id=entry.snapshot_id,
            requested_partitions=cfg.partitions,
        )

        if not files:
            raise ValueError(
                f"No files found for snapshot {entry.snapshot_id} "
                f"matching partitions {cfg.partitions!r}. "
                "Check that the partition values are correct."
            )

        # Build plan
        planner = RestorePlanner(
            archive_storage=self._archive,
            target_storage=self._target,
            archive_root=cfg.archive_root,
            target_root=cfg.target_root,
        )

        plan = planner.build_plan(
            table=cfg.table,
            restore_as=cfg.restore_as,
            snapshot_id=entry.snapshot_id,
            snapshot_timestamp_iso=entry.timestamp_iso,
            requested_partitions=cfg.partitions,
            files_to_copy=files,
            mode=cfg.mode,
            conflict_strategy=cfg.conflict_strategy,
        )

        plan.print()
        return plan

    # ── Step 3: Execute plan ──────────────────────────────────────────────────

    def execute(self, plan: RestorePlan) -> RestoreResult:
        """
        Execute a RestorePlan produced by plan().

        Raises RuntimeError if the plan has unresolved conflicts with
        conflict_strategy=fail.
        """
        cfg = self._cfg
        result = RestoreResult(
            table=plan.table,
            restore_as=plan.restore_as,
            mode=plan.mode,
            snapshot_id=plan.snapshot_id,
        )

        # Block on unresolved conflicts
        if plan.has_conflicts and cfg.conflict_strategy == "fail":
            raise RuntimeError(
                f"Restore blocked: {len(plan.conflicts)} partition conflict(s). "
                "Set conflict_strategy=overwrite or conflict_strategy=skip to proceed."
            )

        archived_metadata = self._load_archived_metadata(cfg.table)
        effective_table = plan.effective_table
        target_table_root = f"{cfg.target_root.rstrip('/')}/{effective_table}"

        # ── Copy data files: archive → target ──────────────────────────────

        files_to_copy = plan.files_to_copy
        if cfg.conflict_strategy == "skip":
            files_to_copy = [
                f for f in files_to_copy
                if not self._target.exists(
                    f.file_path.replace(cfg.archive_root, cfg.target_root, 1)
                )
            ]

        def _copy(f: ScannedFile) -> int:
            target_uri = f.file_path.replace(cfg.archive_root, cfg.target_root, 1)
            self._target.copy_from(self._archive, f.file_path, target_uri)
            return f.file_size_bytes

        with ThreadPoolExecutor(max_workers=cfg.transfer.parallelism) as pool:
            futures = {pool.submit(_copy, f): f for f in files_to_copy}
            for future in as_completed(futures):
                try:
                    result.bytes_copied += future.result()
                    result.files_copied += 1
                except Exception as exc:
                    f = futures[future]
                    log.error("Failed to copy %s: %s", f.file_path, exc)
                    result.errors.append(str(exc))

        if result.errors:
            raise RuntimeError(
                f"Restore aborted: {len(result.errors)} file copy failure(s). "
                "No metadata has been written — primary table is unchanged."
            )

        # ── Copy + rewrite manifest files ──────────────────────────────────

        restored_manifest_uris = self._copy_manifests(
            archived_metadata=archived_metadata,
            snapshot_id=plan.snapshot_id,
            archive_root=cfg.archive_root,
            target_table_root=target_table_root,
        )

        # ── Reconstruct metadata ────────────────────────────────────────────

        if plan.mode == "new_table" or (plan.mode == "replace" and plan.is_full_table):
            new_snap_id = int(time.time() * 1000) % (2**31)
            new_manifest_list_uri = self._copy_manifest_list(
                archived_metadata=archived_metadata,
                snapshot_id=plan.snapshot_id,
                archive_root=cfg.archive_root,
                target_table_root=target_table_root,
            )
            new_meta_uri = write_table_metadata(
                storage=self._target,
                archived_metadata=archived_metadata,
                target_table_root=target_table_root,
                new_manifest_list_uri=new_manifest_list_uri,
                new_snapshot_id=new_snap_id,
            )

        else:
            # Partial partition restore into live table
            current_metadata = self._load_target_metadata(effective_table)
            new_meta_uri = splice_manifests(
                target_storage=self._target,
                current_metadata=current_metadata,
                current_table_root=target_table_root,
                restored_manifest_uris=restored_manifest_uris,
                restored_partitions=cfg.partitions,
                mode=plan.mode,
                archive_root=cfg.archive_root,
                target_root=cfg.target_root,
            )

        result.new_metadata_uri = new_meta_uri

        # ── Commit catalog pointer ──────────────────────────────────────────
        self._commit_metadata(effective_table, target_table_root, new_meta_uri)

        log.info(
            "Restore complete: %d files, %d bytes → %s",
            result.files_copied, result.bytes_copied, new_meta_uri,
        )
        return result

    # ── Private helpers ───────────────────────────────────────────────────────

    def _load_archived_metadata(self, table: str) -> dict:
        table_archive_root = f"{self._cfg.archive_root.rstrip('/')}/{table}"
        # Find the latest versioned metadata.json in the archive
        latest_uri: Optional[str] = None
        latest_ver = -1
        for fi in self._archive.list_objects(f"{table_archive_root}/metadata/"):
            name = fi.uri.split("/")[-1]
            if name.endswith(".metadata.json") and name.startswith("v"):
                try:
                    ver = int(name.split(".")[0].lstrip("v"))
                    if ver > latest_ver:
                        latest_ver = ver
                        latest_uri = fi.uri
                except ValueError:
                    pass

        if latest_uri is None:
            raise FileNotFoundError(
                f"No metadata.json found in archive for table '{table}' at "
                f"{table_archive_root}/metadata/"
            )
        return orjson.loads(self._archive.read_bytes(latest_uri))

    def _load_target_metadata(self, table: str) -> dict:
        table_root = f"{self._cfg.target_root.rstrip('/')}/{table}"
        hint_uri = f"{table_root}/metadata/version-hint.text"
        version = int(self._target.read_text(hint_uri).strip())
        meta_uri = f"{table_root}/metadata/v{version:05d}.metadata.json"
        return orjson.loads(self._target.read_bytes(meta_uri))

    def _copy_manifests(
        self,
        archived_metadata: dict,
        snapshot_id: int,
        archive_root: str,
        target_table_root: str,
    ) -> List[str]:
        """Copy manifest Avro files for a snapshot; return their new target URIs."""
        snap = next(
            (s for s in archived_metadata.get("snapshots", [])
             if s.get("snapshot-id") == snapshot_id),
            None,
        )
        if snap is None:
            return []

        ml_uri = snap.get("manifest-list", "")
        if not ml_uri:
            return []

        try:
            ml_data = self._archive.read_bytes(ml_uri)
            entries = list(fastavro.reader(io.BytesIO(ml_data)))
        except Exception as exc:
            log.warning("Cannot read manifest-list %s: %s", ml_uri, exc)
            return []

        target_uris: List[str] = []
        for entry in entries:
            mpath: str = entry.get("manifest_path", "")
            if mpath:
                target_uri = mpath.replace(archive_root, self._cfg.target_root, 1)
                self._target.copy_from(self._archive, mpath, target_uri)
                target_uris.append(target_uri)

        return target_uris

    def _copy_manifest_list(
        self,
        archived_metadata: dict,
        snapshot_id: int,
        archive_root: str,
        target_table_root: str,
    ) -> str:
        """Copy the manifest-list Avro for a snapshot; return its new target URI."""
        snap = next(
            (s for s in archived_metadata.get("snapshots", [])
             if s.get("snapshot-id") == snapshot_id),
            None,
        )
        if snap is None:
            raise ValueError(f"Snapshot {snapshot_id} not in archived metadata.")

        ml_uri = snap.get("manifest-list", "")
        target_ml_uri = ml_uri.replace(archive_root, self._cfg.target_root, 1)
        self._target.copy_from(self._archive, ml_uri, target_ml_uri)
        return target_ml_uri

    def _commit_metadata(self, table: str, table_root: str, new_meta_uri: str) -> None:
        if self._nessie:
            try:
                self._nessie.update(table, new_meta_uri)
                return
            except Exception as exc:
                log.warning("Nessie update failed, falling back to version-hint: %s", exc)

        filename = new_meta_uri.split("/")[-1]
        version = int(filename.split(".")[0].lstrip("v"))
        hint_uri = f"{table_root}/metadata/version-hint.text"
        self._target.write_text(hint_uri, str(version))
