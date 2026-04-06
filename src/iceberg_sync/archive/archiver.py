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
from iceberg_sync.archive.config import ArchiveJobConfig, _storage_kwargs
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


_DATE_FORMATS = [("%Y-%m-%d", 10), ("%Y-%m", 7)]


def _cob_dates_from_manifests(
    metadata: dict,
    storage: "StorageBackend",
) -> Dict[int, int]:
    """
    For each snapshot, read its first manifest FILE (not manifest list) and
    extract the partition field value of the first data file entry.  If any
    partition field value parses as YYYY-MM or YYYY-MM-DD, return it as
    epoch-ms so decide_snapshots evaluates retention against the data's
    business date rather than its write time.

    Reading the manifest file (not the manifest list) is more reliable because
    PyIceberg does not always populate the lower_bound summaries in the
    manifest list, whereas the partition record in each manifest entry always
    contains the actual partition values written by the writer.

    Returns an empty dict if the table is unpartitioned or the partition
    values cannot be decoded — callers fall back to snapshot timestamp-ms.
    """
    result: Dict[int, int] = {}
    for snap in metadata.get("snapshots", []):
        snap_id: int = snap.get("snapshot-id", 0)
        ml_uri: str = snap.get("manifest-list", "")
        if not ml_uri:
            continue
        try:
            ml_raw = storage.read_bytes(ml_uri)
            ml_entries = list(fastavro.reader(io.BytesIO(ml_raw)))
            if not ml_entries:
                continue

            # Read the first manifest file to get actual partition values.
            manifest_path: str = ml_entries[0].get("manifest_path", "")
            if not manifest_path:
                continue

            m_raw = storage.read_bytes(manifest_path)
            for record in fastavro.reader(io.BytesIO(m_raw)):
                df = record.get("data_file") or record
                partition: dict = dict(df.get("partition") or {})
                if not partition:
                    continue
                # Check each partition field value for a parseable date.
                for val in partition.values():
                    if val is None:
                        continue
                    val_str = val.decode("utf-8") if isinstance(val, (bytes, bytearray)) else str(val)
                    for fmt, expected_len in _DATE_FORMATS:
                        try:
                            dt = datetime.strptime(val_str[:expected_len], fmt).replace(
                                tzinfo=timezone.utc
                            )
                            result[snap_id] = int(dt.timestamp() * 1000)
                            break
                        except ValueError:
                            continue
                    if snap_id in result:
                        break
                if snap_id in result:
                    break

        except Exception as exc:
            # Non-fatal: fall back to write timestamp for this snapshot.
            log.warning("COB date read failed for snapshot %d: %s", snap_id, exc)
            continue
    return result


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
        catalog_rest_uri: Optional[str] = None,
        catalog_rest_session: Optional[Any] = None,
        catalog_rest_prefix: Optional[str] = None,  # e.g. "main" from /v1/config
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
        self._catalog_rest_uri = (catalog_rest_uri or "").rstrip("/") or None
        self._catalog_rest_session = catalog_rest_session
        self._catalog_rest_prefix = catalog_rest_prefix or ""
        # Derive the bucket-level base (scheme://netloc) for URI translation so
        # that files stored outside the configured source_root subtree (e.g. when
        # the catalog stores tables at a path that doesn't match source_root) are
        # still translated correctly to their archive equivalents.
        from urllib.parse import urlparse as _urlparse
        _p = _urlparse(source_root)
        self._source_base = f"{_p.scheme}://{_p.netloc}"

    @classmethod
    def from_config(cls, cfg: ArchiveJobConfig) -> "IcebergArchiver":
        source_storage = create_storage(cfg.source_root, **_storage_kwargs(cfg.source_root, cfg.source_s3, cfg.source_adls))
        archive_storage = create_storage(cfg.archive_root, **_storage_kwargs(cfg.archive_root, cfg.archive_s3, cfg.archive_adls))

        # Build OAuth client (shared by Nessie and REST catalog sessions)
        auth = None
        if cfg.catalog.oauth_url:
            from iceberg_sync.auth.oauth_client import OAuthClient
            server_url = cfg.catalog.oauth_url
            if server_url.endswith("/token"):
                server_url = server_url[: -len("/token")]
            auth = OAuthClient(
                server_url=server_url,
                client_id=cfg.catalog.oauth_client_id or "",
                client_secret=cfg.catalog.oauth_client_secret or "",
            )

        nessie = None
        if cfg.catalog.nessie_uri:
            from iceberg_sync.catalog.nessie import NessieCatalog
            nessie = NessieCatalog(
                uri=cfg.catalog.nessie_uri,
                ref=cfg.catalog.nessie_ref,
                oauth_client=auth,
            )

        # Build an HTTP session for the Iceberg REST catalog (catalog-gateway)
        catalog_rest_session = None
        catalog_rest_uri = cfg.catalog.catalog_rest_uri or None
        if catalog_rest_uri:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            from iceberg_sync.catalog.nessie import _OAuthTokenAuth
            session = requests.Session()
            retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 502, 503, 504])
            session.mount("http://", HTTPAdapter(max_retries=retry))
            session.mount("https://", HTTPAdapter(max_retries=retry))
            if auth:
                session.auth = _OAuthTokenAuth(auth)
            catalog_rest_session = session

        # Fetch the REST catalog prefix from /v1/config (e.g. "main" for Nessie-backed catalogs)
        catalog_rest_prefix = ""
        if catalog_rest_uri and catalog_rest_session:
            try:
                r = catalog_rest_session.get(f"{catalog_rest_uri}/v1/config", timeout=5)
                if r.ok:
                    catalog_rest_prefix = r.json().get("defaults", {}).get("prefix", "")
                    log.debug("REST catalog prefix from /v1/config: %r", catalog_rest_prefix)
            except Exception as exc:
                log.debug("Could not fetch REST catalog config: %s", exc)

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
            catalog_rest_uri=catalog_rest_uri,
            catalog_rest_session=catalog_rest_session,
            catalog_rest_prefix=catalog_rest_prefix,
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def archive_table(self, table: str, *, dry_run: bool = True) -> ArchiveResult:
        result = ArchiveResult(table=table, dry_run=dry_run)
        table_source_root = f"{self._source_root}/{table}"

        try:
            metadata, src_meta_uri = self._load_metadata(table_source_root)
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
            effective_timestamps=_cob_dates_from_manifests(
                metadata, self._source
            ),
        )
        to_archive = snapshots_to_archive(decisions)

        if not to_archive:
            log.info("No snapshots to archive for %s.", table)
            return result

        log.info(
            "%s: %d snapshot(s) to archive (dry_run=%s).",
            table, len(to_archive), dry_run,
        )

        # Dry-run plan: just count snapshots without scanning files.
        if dry_run:
            result.snapshots_archived = len(to_archive)
            return result

        snapshots_done: List[int] = []
        # Shared set so files referenced by multiple snapshots are copied only once
        already_copied: Set[str] = set()

        for decision in to_archive:
            snap_id = decision.snapshot_id
            log.info("  Archiving snapshot %d (%s) ...", snap_id, decision.timestamp_iso)

            try:
                files_copied, bytes_copied = self._archive_snapshot(
                    metadata=metadata,
                    table=table,
                    snapshot_id=snap_id,
                    dry_run=dry_run,
                    already_copied=already_copied,
                )
                result.files_copied += files_copied
                result.bytes_copied += bytes_copied
                result.snapshots_archived += 1
                snapshots_done.append(snap_id)

                if not dry_run:
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

        # Copy the primary metadata.json to archive so restorer can read it
        arc_meta_uri = self._translate_uri(src_meta_uri)
        try:
            self._archive.copy_from(self._source, src_meta_uri, arc_meta_uri)
            log.debug("Copied metadata.json to archive → %s", arc_meta_uri)
        except Exception as exc:
            result.errors.append(f"Failed to copy metadata.json to archive: {exc}")
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

    def _translate_uri(self, uri: str) -> str:
        """
        Translate a source URI to its archive equivalent.

        Strips the bucket-level prefix (scheme://netloc) from *uri* so that
        files stored at any path within the same bucket — including paths that
        don't start with source_root — are still translated correctly.
        """
        rel = uri[len(self._source_base):]  # e.g. /tpch/orders_uuid/data/f.parquet
        return self._archive_root + rel

    def _locate_metadata_uri(self, table_root: str) -> str:
        """
        Return the current metadata.json URI for a table using whichever
        discovery method is available, in order of reliability:

        1. Nessie catalog  — authoritative when accessible.
        2. version-hint.text — written by Hadoop-style catalogs.
        3. Directory scan — lists all .metadata.json files under the metadata/
           prefix and returns the one with the highest version number.  This
           works for any catalog that writes versioned metadata files, including
           REST catalogs backed by Nessie or JDBC.
        """
        # 0. Iceberg REST catalog (catalog-gateway) — spec-correct, most reliable
        if self._catalog_rest_uri and self._catalog_rest_session:
            try:
                rel = table_root[len(self._source_root):].strip("/")
                namespace, table_name = rel.rsplit("/", 1)
                prefix_seg = f"{self._catalog_rest_prefix}/" if self._catalog_rest_prefix else ""
                url = f"{self._catalog_rest_uri}/v1/{prefix_seg}namespaces/{namespace}/tables/{table_name}"
                r = self._catalog_rest_session.get(url, timeout=10)
                if r.ok:
                    loc = r.json().get("metadata-location")
                    if loc:
                        log.debug("Metadata location from REST catalog: %s", loc)
                        return loc
            except Exception as exc:
                log.debug("REST catalog metadata lookup skipped: %s", exc)

        # 1. Nessie
        if self._nessie:
            try:
                rel = table_root[len(self._source_root):].strip("/")
                namespace, table_name = rel.rsplit("/", 1)
                uri = self._nessie.get_metadata_location(namespace, table_name)
                if uri:
                    return uri
            except Exception as exc:
                log.debug("Nessie metadata lookup skipped: %s", exc)

        # 2. version-hint.text (Hadoop catalog)
        try:
            hint_uri = f"{table_root}/metadata/version-hint.text"
            version = int(self._source.read_text(hint_uri).strip())
            return f"{table_root}/metadata/v{version:05d}.metadata.json"
        except Exception:
            pass

        # 3. Scan metadata directory for highest-versioned .metadata.json
        prefix = f"{table_root}/metadata/"
        best_version = -1
        best_uri: str | None = None
        fallback_uri: str | None = None  # for UUID-named files
        try:
            for fi in self._source.list_objects(prefix):
                if not fi.uri.endswith(".metadata.json"):
                    continue
                fname = fi.uri.split("/")[-1]
                stem = fname.split(".")[0]
                if stem.startswith("v"):
                    try:
                        v = int(stem[1:])
                        if v > best_version:
                            best_version, best_uri = v, fi.uri
                        continue
                    except ValueError:
                        pass
                # UUID-named file — keep as last resort
                if fallback_uri is None or fi.uri > fallback_uri:
                    fallback_uri = fi.uri
        except Exception as exc:
            raise FileNotFoundError(
                f"Cannot locate metadata for {table_root}: metadata scan failed — {exc}"
            ) from exc

        result = best_uri or fallback_uri
        if result is None:
            raise FileNotFoundError(
                f"Cannot locate metadata for {table_root}: "
                "no version-hint.text, Nessie is unreachable, "
                "and no .metadata.json files found in metadata/"
            )
        return result

    def _enrich_with_historical_snapshots(
        self, current_meta: dict, meta_uri: str
    ) -> dict:
        """
        Scan all .metadata.json files in the same directory as *meta_uri* and
        collect snapshots that are absent from *current_meta*.

        Nessie-backed REST catalogs write one independent metadata file per
        commit (no snapshot chaining inside the file).  Walking the directory
        reconstructs the full snapshot timeline without touching the Nessie
        commit log or requiring any special catalog access.
        """
        dir_uri = meta_uri.rsplit("/", 1)[0] + "/"
        existing_ids: set = {s.get("snapshot-id") for s in current_meta.get("snapshots", [])}
        additional: list = []

        try:
            for fi in self._source.list_objects(dir_uri):
                if not fi.uri.endswith(".metadata.json") or fi.uri == meta_uri:
                    continue
                try:
                    other = orjson.loads(self._source.read_bytes(fi.uri))
                except Exception:
                    continue
                for snap in other.get("snapshots", []):
                    sid = snap.get("snapshot-id")
                    if sid and sid not in existing_ids:
                        existing_ids.add(sid)
                        additional.append(snap)
        except Exception as exc:
            log.debug("Historical snapshot scan failed: %s", exc)
            return current_meta

        if not additional:
            return current_meta

        import copy
        enriched = copy.deepcopy(current_meta)
        enriched["snapshots"].extend(additional)
        log.info(
            "Enriched metadata with %d historical snapshot(s) from metadata directory "
            "(single-snapshot-per-file catalog detected).",
            len(additional),
        )
        return enriched

    def _load_metadata(self, table_root: str) -> tuple[dict, str]:
        """Return (metadata dict, metadata_uri)."""
        meta_uri = self._locate_metadata_uri(table_root)
        meta = orjson.loads(self._source.read_bytes(meta_uri))

        # Catalogs that write one snapshot per metadata file (e.g. Nessie REST)
        # produce a current metadata with only 1 snapshot.  Recover the full
        # snapshot history by scanning the metadata directory.
        if len(meta.get("snapshots", [])) <= 1:
            meta = self._enrich_with_historical_snapshots(meta, meta_uri)

        return meta, meta_uri

    def _archive_snapshot(
        self,
        metadata: dict,
        table: str,
        snapshot_id: int,
        dry_run: bool,
        already_copied: Set[str],
    ) -> tuple[int, int]:
        """
        Copy all files for snapshot_id to archive storage.

        *already_copied* is a shared set mutated in place — files already
        transferred for a previous snapshot in the same run are skipped so
        that data files shared across snapshots are not copied twice.

        Returns (files_copied, bytes_copied) for this snapshot only.
        """
        files = scan_snapshot(
            storage=self._source,
            metadata=metadata,
            snapshot_id=snapshot_id,
            requested_partitions=[],  # all partitions
        )

        # Deduplicate against files already transferred in this archive run
        new_files = [f for f in files if f.file_path not in already_copied]

        files_copied = 0
        bytes_copied = 0

        def _copy_file(scanned_file) -> int:
            archive_uri = self._translate_uri(scanned_file.file_path)
            if dry_run:
                return scanned_file.file_size_bytes
            self._archive.copy_from(self._source, scanned_file.file_path, archive_uri)
            return scanned_file.file_size_bytes

        with ThreadPoolExecutor(max_workers=self._parallelism) as pool:
            futures = {pool.submit(_copy_file, f): f for f in new_files}
            for future in as_completed(futures):
                f = futures[future]
                try:
                    bytes_copied += future.result()
                    files_copied += 1
                    already_copied.add(f.file_path)
                except Exception as exc:
                    log.warning("File copy failed for %s: %s", f.file_path, exc)

        # Archive the Avro metadata files for this snapshot
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
            archive_uri = self._translate_uri(ml_uri)
            self._archive.copy_from(self._source, ml_uri, archive_uri)

            # Read manifest-list to copy individual manifests
            try:
                ml_data = self._source.read_bytes(ml_uri)
                for entry in fastavro.reader(io.BytesIO(ml_data)):
                    mpath = entry.get("manifest_path", "")
                    if mpath:
                        m_archive = self._translate_uri(mpath)
                        self._archive.copy_from(self._source, mpath, m_archive)
            except Exception as exc:
                log.warning("Could not copy manifests for snapshot %d: %s", snapshot_id, exc)

    def _commit_metadata(self, table: str, table_root: str, new_meta_uri: str) -> None:
        """Update version-hint.text or Nessie after expiring snapshots."""
        if self._nessie:
            try:
                namespace, table_name = table.rsplit("/", 1)
                self._nessie.update_table(namespace, table_name, new_meta_uri)
                return
            except Exception as exc:
                log.warning("Nessie update failed, falling back to version-hint: %s", exc)

        # Extract version number from filename
        filename = new_meta_uri.split("/")[-1]
        version = int(filename.split(".")[0].lstrip("v"))
        hint_uri = f"{table_root}/metadata/version-hint.text"
        self._source.write_text(hint_uri, str(version))
