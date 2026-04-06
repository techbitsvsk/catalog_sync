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
from iceberg_sync.archive.config import RestoreJobConfig, _storage_kwargs
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
        archive_storage = create_storage(cfg.archive_root, **_storage_kwargs(cfg.archive_root, cfg.archive_s3, cfg.archive_adls))
        target_storage = create_storage(cfg.target_root, **_storage_kwargs(cfg.target_root, cfg.target_s3, cfg.target_adls))

        nessie = None
        if cfg.catalog.nessie_uri:
            from iceberg_sync.catalog.nessie import NessieCatalog
            from iceberg_sync.auth.oauth_client import OAuthClient

            auth = None
            if cfg.catalog.oauth_url:
                server_url = cfg.catalog.oauth_url
                if server_url.endswith("/token"):
                    server_url = server_url[: -len("/token")]
                auth = OAuthClient(
                    server_url=server_url,
                    client_id=cfg.catalog.oauth_client_id or "",
                    client_secret=cfg.catalog.oauth_client_secret or "",
                )
            nessie = NessieCatalog(
                uri=cfg.catalog.nessie_uri,
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

        from iceberg_sync.archive._utils import fmt_bytes as _fmt_bytes

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
        archived_metadata = self._load_archived_metadata(cfg.table, entry.snapshot_id)

        # Build a URI rewriter so scan_snapshot can resolve manifest paths that
        # still contain the original source URI base (e.g. s3a://warehouse/…)
        # to their archive equivalents (e.g. abfss://archive@…/iceberg-cold/…).
        path_rewriter = self._make_path_rewriter(index.source_root)

        # Scan partitions
        files = scan_snapshot(
            storage=self._archive,
            metadata=archived_metadata,
            snapshot_id=entry.snapshot_id,
            requested_partitions=cfg.partitions,
            path_rewriter=path_rewriter,
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

        index = ArchiveIndex.load(self._archive, cfg.archive_root, cfg.table)
        path_rewriter = self._make_path_rewriter(index.source_root)

        archived_metadata = self._load_archived_metadata(cfg.table, plan.snapshot_id)
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
            path_rewriter=path_rewriter,
        )

        # ── Reconstruct metadata ────────────────────────────────────────────

        if plan.mode == "new_table" or (plan.mode == "replace" and plan.is_full_table):
            new_snap_id = int(time.time() * 1000)   # 64-bit epoch-ms; no modulo
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

    def _make_path_rewriter(self, source_root: str):
        """
        Return a callable that translates a source-storage URI to its archive
        equivalent.

        The archiver strips the source bucket prefix (scheme://netloc) and
        prepends archive_root when it copies files, so we do the same here.
        For example:
            s3a://warehouse/tpch/orders_uuid/metadata/abc.avro
            → abfss://archive@account.dfs.core.windows.net/iceberg-cold/tpch/orders_uuid/metadata/abc.avro
        """
        from urllib.parse import urlparse
        p = urlparse(source_root)
        source_base = f"{p.scheme}://{p.netloc}"  # e.g. "s3a://warehouse"
        archive_root = self._cfg.archive_root.rstrip("/")

        def _rewrite(uri: str) -> str:
            if uri.startswith(archive_root):
                return uri  # already an archive URI
            if uri.startswith(source_base):
                return archive_root + uri[len(source_base):]
            return uri  # unknown scheme — return as-is

        return _rewrite

    def _load_archived_metadata(self, table: str, snapshot_id: Optional[int] = None) -> dict:
        table_archive_root = f"{self._cfg.archive_root.rstrip('/')}/{table}"
        metadata_prefix = f"{table_archive_root}/metadata/"

        # Collect all .metadata.json files — handles both Hadoop-style (v1.metadata.json)
        # and Nessie-style (UUID.metadata.json) naming conventions.
        candidates: List[str] = []
        for fi in self._archive.list_objects(metadata_prefix):
            if fi.uri.endswith(".metadata.json"):
                candidates.append(fi.uri)

        if not candidates:
            # Nessie stores tables at a UUID-suffixed path (e.g. orders_<uuid>/)
            # rather than the logical table name (orders/).  Search the parent
            # namespace directory for any subdirectory that starts with the table
            # leaf name and contains metadata files.
            namespace, table_name = (table.rsplit("/", 1) if "/" in table else ("", table))
            ns_prefix = f"{self._cfg.archive_root.rstrip('/')}/{namespace}/" if namespace else f"{self._cfg.archive_root.rstrip('/')}/"
            for fi in self._archive.list_objects(ns_prefix):
                if not fi.uri.endswith(".metadata.json"):
                    continue
                # e.g. …/tpch/orders_<uuid>/metadata/abc.metadata.json
                parts = fi.uri[len(ns_prefix):].split("/")
                if parts and parts[0].startswith(table_name) and "metadata" in parts:
                    candidates.append(fi.uri)

        if not candidates:
            raise FileNotFoundError(
                f"No metadata.json found in archive for table '{table}' at "
                f"{metadata_prefix}"
            )

        # Prefer Hadoop-style versioned files (v<N>.metadata.json) — pick highest N.
        versioned = {}
        for uri in candidates:
            name = uri.split("/")[-1]
            if name.startswith("v"):
                try:
                    ver = int(name.split(".")[0].lstrip("v"))
                    versioned[ver] = uri
                except ValueError:
                    pass
        if versioned:
            return orjson.loads(self._archive.read_bytes(versioned[max(versioned)]))

        # UUID-named files (Nessie): each metadata file typically covers one snapshot.
        # Read all candidates and build a map; prefer the file that contains the
        # target snapshot_id (exact match), then fall back to highest last-updated-ms.
        parsed: List[tuple] = []  # (last-updated-ms, uri, meta_dict)
        for uri in candidates:
            try:
                meta = orjson.loads(self._archive.read_bytes(uri))
                ts = meta.get("last-updated-ms", 0)
                parsed.append((ts, uri, meta))
            except Exception:
                pass

        if not parsed:
            raise FileNotFoundError(
                f"No readable metadata.json found in archive for table '{table}' at "
                f"{metadata_prefix}"
            )

        parsed.sort(key=lambda x: x[0], reverse=True)

        # If we know the target snapshot, prefer the file that contains it.
        if snapshot_id is not None:
            for _ts, _uri, meta in parsed:
                snaps = meta.get("snapshots", [])
                if any(s.get("snapshot-id") == snapshot_id for s in snaps):
                    log.debug("Using metadata %s (contains snapshot %s)", _uri, snapshot_id)
                    return meta

            # No single file contains the snapshot — try merging all metadata files
            # (handles Nessie one-snapshot-per-file where the most-recent file is archived).
            import copy
            base_meta = copy.deepcopy(parsed[0][2])
            merged_snaps: Dict[int, Any] = {
                s["snapshot-id"]: s for s in base_meta.get("snapshots", [])
            }
            for _ts, _uri, meta in parsed[1:]:
                for s in meta.get("snapshots", []):
                    sid = s.get("snapshot-id")
                    if sid is not None and sid not in merged_snaps:
                        merged_snaps[sid] = s
            base_meta["snapshots"] = list(merged_snaps.values())

            if any(s.get("snapshot-id") == snapshot_id for s in base_meta["snapshots"]):
                return base_meta

            # Last resort: Iceberg names manifest-list files snap-{snapshot_id}-{n}.avro.
            # The archiver copies these to the archive even though the metadata file
            # doesn't reference them.  Scan the archived metadata directories for the file
            # and inject a synthetic snapshot entry so the rest of the restore can proceed.
            meta_dirs: set = {uri.rsplit("/", 1)[0] + "/" for uri in candidates}
            for meta_dir in meta_dirs:
                try:
                    for fi in self._archive.list_objects(meta_dir):
                        fname = fi.uri.split("/")[-1]
                        if fname.startswith(f"snap-{snapshot_id}-") and fname.endswith(".avro"):
                            log.info(
                                "Snapshot %s not in any metadata file; "
                                "found manifest-list by scan: %s",
                                snapshot_id, fi.uri,
                            )
                            base_meta.setdefault("snapshots", []).append({
                                "snapshot-id": snapshot_id,
                                "timestamp-ms": 0,
                                "manifest-list": fi.uri,
                                "summary": {"operation": "append"},
                            })
                            return base_meta
                except Exception as exc:
                    log.debug("Manifest-list scan in %s failed: %s", meta_dir, exc)

            log.warning(
                "Snapshot %s not found in archived metadata and no matching "
                "snap-<id>-*.avro file found; restore may fail.",
                snapshot_id,
            )
            return base_meta

        # No target snapshot_id provided — return the most-recently-updated metadata.
        return parsed[0][2]

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
        path_rewriter=None,
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
                archive_mpath = path_rewriter(mpath) if path_rewriter else mpath
                target_uri = archive_mpath.replace(archive_root, self._cfg.target_root, 1)
                self._target.copy_from(self._archive, archive_mpath, target_uri)
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
                namespace, table_name = table.rsplit("/", 1)
                self._nessie.update_table(namespace, table_name, new_meta_uri)
                return
            except Exception as exc:
                log.warning("Nessie update failed, falling back to version-hint: %s", exc)

        filename = new_meta_uri.split("/")[-1]
        version = int(filename.split(".")[0].lstrip("v"))
        hint_uri = f"{table_root}/metadata/version-hint.text"
        self._target.write_text(hint_uri, str(version))
