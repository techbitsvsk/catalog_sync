"""
tests/test_archive.py — Unit tests for the iceberg_sync.archive module.

All tests use MemoryStorageBackend — no cloud credentials or network required.

Coverage:
  snapshot_manager  — retention policy decisions
  partition_scanner — manifest Avro parsing + partition filtering
  archive_index     — index persistence and snapshot lookup
  archiver          — end-to-end archive run (copy + expire + index)
  metadata_editor   — write_table_metadata, splice_manifests, expire_snapshots
  restorer          — plan() dry-run + execute() full-table and partition restore
  _utils            — fmt_bytes correctness
"""

from __future__ import annotations

import io
import json
import time
from typing import Any, Dict, List

import fastavro
import orjson
import pytest

from iceberg_sync.storage.memory import MemoryStorageBackend
from iceberg_sync.archive._utils import fmt_bytes
from iceberg_sync.archive.archive_index import ArchiveIndex, ArchivedSnapshotEntry
from iceberg_sync.archive.snapshot_manager import decide_snapshots, parse_duration_ms
from iceberg_sync.archive.partition_scanner import scan_snapshot, ScannedFile
from iceberg_sync.archive.metadata_editor import (
    expire_snapshots_in_metadata,
    write_table_metadata,
    splice_manifests,
)
from iceberg_sync.archive.archiver import IcebergArchiver, ArchiveResult
from iceberg_sync.archive.restorer import IcebergRestorer

# ──────────────────────────────────────────────────────────────────────────────
# Avro helpers
# ──────────────────────────────────────────────────────────────────────────────

_MANIFEST_LIST_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        {"name": "added_snapshot_id", "type": ["null", "long"], "default": None},
        {"name": "added_data_files_count", "type": ["null", "int"], "default": None},
        {"name": "existing_data_files_count", "type": ["null", "int"], "default": None},
        {"name": "deleted_data_files_count", "type": ["null", "int"], "default": None},
        {"name": "partitions", "type": {"type": "array", "items": "string"}, "default": []},
    ],
}

_MANIFEST_SCHEMA = {
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int"},
        {"name": "data_file", "type": {
            "type": "record",
            "name": "r2",
            "fields": [
                {"name": "file_path", "type": "string"},
                {"name": "file_size_in_bytes", "type": "long"},
                {"name": "content", "type": "int"},
                {"name": "record_count", "type": "long"},
                {"name": "partition", "type": {
                    "type": "record",
                    "name": "r102",
                    "fields": [
                        {"name": "year", "type": ["null", "int"], "default": None},
                        {"name": "month", "type": ["null", "int"], "default": None},
                    ],
                }},
            ],
        }},
    ],
}


def _avro_bytes(schema: dict, records: list) -> bytes:
    buf = io.BytesIO()
    fastavro.writer(buf, fastavro.parse_schema(schema), records)
    return buf.getvalue()


def _manifest_list_bytes(entries: list) -> bytes:
    return _avro_bytes(_MANIFEST_LIST_SCHEMA, entries)


def _manifest_bytes(entries: list) -> bytes:
    return _avro_bytes(_MANIFEST_SCHEMA, entries)


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures — minimal Iceberg table seeded in MemoryStorageBackend
# ──────────────────────────────────────────────────────────────────────────────

SOURCE_ROOT = "mem://source/iceberg"
ARCHIVE_ROOT = "mem://archive/iceberg"
TARGET_ROOT = "mem://target/iceberg"
TABLE = "gold/orders"


def _seed_table(storage: MemoryStorageBackend, root: str, table: str) -> dict:
    """
    Write a minimal 2-snapshot Iceberg table into *storage*.

    Snapshot 100 — old (timestamp = 1 ms)
    Snapshot 200 — current (timestamp = now)

    Returns the metadata dict.
    """
    table_root = f"{root}/{table}"

    # Data files
    data_old = b"parquet-old-data"
    data_new = b"parquet-new-data"
    old_data_uri = f"{table_root}/data/year=2024/month=01/old.parquet"
    new_data_uri = f"{table_root}/data/year=2025/month=11/new.parquet"
    storage.put(old_data_uri, data_old)
    storage.put(new_data_uri, data_new)

    # Manifest for snapshot 100
    manifest_100_content = _manifest_bytes([{
        "status": 1,
        "data_file": {
            "file_path": old_data_uri,
            "file_size_in_bytes": len(data_old),
            "content": 0,
            "record_count": 10,
            "partition": {"year": 2024, "month": 1},
        },
    }])
    manifest_100_uri = f"{table_root}/metadata/manifest-100.avro"
    storage.put(manifest_100_uri, manifest_100_content)

    # Manifest-list for snapshot 100
    ml_100_content = _manifest_list_bytes([{
        "manifest_path": manifest_100_uri,
        "manifest_length": len(manifest_100_content),
        "partition_spec_id": 0,
        "added_snapshot_id": 100,
        "added_data_files_count": 1,
        "existing_data_files_count": 0,
        "deleted_data_files_count": 0,
        "partitions": [],
    }])
    ml_100_uri = f"{table_root}/metadata/snap-100-ml.avro"
    storage.put(ml_100_uri, ml_100_content)

    # Manifest for snapshot 200
    manifest_200_content = _manifest_bytes([{
        "status": 1,
        "data_file": {
            "file_path": new_data_uri,
            "file_size_in_bytes": len(data_new),
            "content": 0,
            "record_count": 20,
            "partition": {"year": 2025, "month": 11},
        },
    }])
    manifest_200_uri = f"{table_root}/metadata/manifest-200.avro"
    storage.put(manifest_200_uri, manifest_200_content)

    # Manifest-list for snapshot 200
    ml_200_content = _manifest_list_bytes([{
        "manifest_path": manifest_200_uri,
        "manifest_length": len(manifest_200_content),
        "partition_spec_id": 0,
        "added_snapshot_id": 200,
        "added_data_files_count": 1,
        "existing_data_files_count": 0,
        "deleted_data_files_count": 0,
        "partitions": [],
    }])
    ml_200_uri = f"{table_root}/metadata/snap-200-ml.avro"
    storage.put(ml_200_uri, ml_200_content)

    now_ms = int(time.time() * 1000)
    metadata = {
        "format-version": 2,
        "table-uuid": "test-uuid",
        "location": table_root,
        "last-updated-ms": now_ms,
        "last-sequence-number": 2,
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "fields": []}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": [
            {"name": "year", "transform": "year", "source-id": 1, "field-id": 1000},
            {"name": "month", "transform": "month", "source-id": 1, "field-id": 1001},
        ]}],
        "current-snapshot-id": 200,
        "snapshots": [
            {
                "snapshot-id": 100,
                "timestamp-ms": 1,   # very old — eligible for archive
                "manifest-list": ml_100_uri,
                "summary": {"operation": "append", "added-data-files": "1"},
            },
            {
                "snapshot-id": 200,
                "timestamp-ms": now_ms,
                "manifest-list": ml_200_uri,
                "summary": {"operation": "append", "added-data-files": "1"},
            },
        ],
        "snapshot-log": [
            {"snapshot-id": 100, "timestamp-ms": 1},
            {"snapshot-id": 200, "timestamp-ms": now_ms},
        ],
        "metadata-log": [],
    }

    meta_uri = f"{table_root}/metadata/v00001.metadata.json"
    storage.put(meta_uri, orjson.dumps(metadata))
    storage.put(f"{table_root}/metadata/version-hint.text", b"1")
    return metadata


# ──────────────────────────────────────────────────────────────────────────────
# snapshot_manager tests
# ──────────────────────────────────────────────────────────────────────────────

class TestParseDuration:
    def test_days(self):
        assert parse_duration_ms("7d") == 7 * 86_400_000

    def test_hours(self):
        assert parse_duration_ms("24h") == 24 * 3_600_000

    def test_minutes(self):
        assert parse_duration_ms("30m") == 30 * 60_000

    def test_milliseconds(self):
        assert parse_duration_ms("500ms") == 500

    def test_weeks(self):
        assert parse_duration_ms("2w") == 2 * 604_800_000

    def test_invalid(self):
        with pytest.raises(ValueError, match="Cannot parse duration"):
            parse_duration_ms("badvalue")


class TestDecideSnapshots:
    def _meta(self, snaps: list, current_id: int) -> dict:
        return {"current-snapshot-id": current_id, "snapshots": snaps}

    def test_current_always_kept(self):
        meta = self._meta([
            {"snapshot-id": 1, "timestamp-ms": 1, "summary": {}},
        ], current_id=1)
        decisions = decide_snapshots(meta, "1ms", min_snapshots_to_keep=0, now_ms=9999999999999)
        assert decisions[0].keep is True
        assert decisions[0].reason == "current"

    def test_old_snapshot_marked_archive(self):
        meta = self._meta([
            {"snapshot-id": 1, "timestamp-ms": 1, "summary": {}},   # current
            {"snapshot-id": 2, "timestamp-ms": 2, "summary": {}},   # old
        ], current_id=1)
        decisions = decide_snapshots(meta, "1ms", min_snapshots_to_keep=0, now_ms=9999999999999)
        archive_decisions = [d for d in decisions if d.reason == "archive"]
        assert len(archive_decisions) == 1
        assert archive_decisions[0].snapshot_id == 2

    def test_min_keep_respected(self):
        meta = self._meta([
            {"snapshot-id": 1, "timestamp-ms": 9999999999998, "summary": {}},  # current
            {"snapshot-id": 2, "timestamp-ms": 2, "summary": {}},              # old
            {"snapshot-id": 3, "timestamp-ms": 1, "summary": {}},              # older
        ], current_id=1)
        # min_keep=2 means keep 2 total; current is 1, so keep 1 more
        decisions = decide_snapshots(meta, "1ms", min_snapshots_to_keep=2, now_ms=9999999999999)
        kept = [d for d in decisions if d.keep]
        assert len(kept) == 2

    def test_already_archived_skipped(self):
        meta = self._meta([
            {"snapshot-id": 1, "timestamp-ms": 9999999999998, "summary": {}},
            {"snapshot-id": 2, "timestamp-ms": 1, "summary": {}},
        ], current_id=1)
        decisions = decide_snapshots(
            meta, "1ms", min_snapshots_to_keep=0,
            now_ms=9999999999999, already_archived_ids={2},
        )
        snap2 = next(d for d in decisions if d.snapshot_id == 2)
        assert snap2.reason == "already_archived"
        assert snap2.keep is False

    def test_no_snapshots_returns_empty(self):
        assert decide_snapshots({"snapshots": []}, "7d") == []


# ──────────────────────────────────────────────────────────────────────────────
# partition_scanner tests
# ──────────────────────────────────────────────────────────────────────────────

class TestPartitionScanner:
    def setup_method(self):
        self.storage = MemoryStorageBackend()
        self.metadata = _seed_table(self.storage, SOURCE_ROOT, TABLE)

    def test_full_table_scan_returns_all_files(self):
        files = scan_snapshot(self.storage, self.metadata, 200, [])
        assert len(files) == 1
        assert files[0].partition == {"year": 2025, "month": 11}

    def test_partition_filter_exact_match(self):
        files = scan_snapshot(
            self.storage, self.metadata, 100,
            [{"year": 2024, "month": 1}],
        )
        assert len(files) == 1
        assert files[0].partition["year"] == 2024

    def test_partition_filter_partial_key(self):
        # seed a snapshot with two months under 2025
        storage = MemoryStorageBackend()
        table_root = f"{SOURCE_ROOT}/{TABLE}"

        def _mk_manifest(uri, year, month, file_uri):
            content = _manifest_bytes([{
                "status": 1,
                "data_file": {
                    "file_path": file_uri,
                    "file_size_in_bytes": 100,
                    "content": 0,
                    "record_count": 5,
                    "partition": {"year": year, "month": month},
                },
            }])
            storage.put(uri, content)
            return content

        m1 = f"{table_root}/metadata/m1.avro"
        m2 = f"{table_root}/metadata/m2.avro"
        f1 = f"{table_root}/data/f1.parquet"
        f2 = f"{table_root}/data/f2.parquet"
        storage.put(f1, b"x")
        storage.put(f2, b"x")
        c1 = _mk_manifest(m1, 2025, 10, f1)
        c2 = _mk_manifest(m2, 2025, 11, f2)

        ml = _manifest_list_bytes([
            {"manifest_path": m1, "manifest_length": len(c1), "partition_spec_id": 0,
             "added_snapshot_id": 300, "added_data_files_count": 1,
             "existing_data_files_count": 0, "deleted_data_files_count": 0, "partitions": []},
            {"manifest_path": m2, "manifest_length": len(c2), "partition_spec_id": 0,
             "added_snapshot_id": 300, "added_data_files_count": 1,
             "existing_data_files_count": 0, "deleted_data_files_count": 0, "partitions": []},
        ])
        ml_uri = f"{table_root}/metadata/ml-300.avro"
        storage.put(ml_uri, ml)

        meta = {
            "current-snapshot-id": 300,
            "snapshots": [{"snapshot-id": 300, "timestamp-ms": 1, "manifest-list": ml_uri, "summary": {}}],
        }

        # Partial key {"year": 2025} should match both months
        files = scan_snapshot(storage, meta, 300, [{"year": 2025}])
        assert len(files) == 2

    def test_no_match_returns_empty(self):
        files = scan_snapshot(
            self.storage, self.metadata, 200,
            [{"year": 1999, "month": 1}],
        )
        assert files == []

    def test_missing_snapshot_raises(self):
        with pytest.raises(ValueError, match="not found"):
            scan_snapshot(self.storage, self.metadata, 9999, [])


# ──────────────────────────────────────────────────────────────────────────────
# archive_index tests
# ──────────────────────────────────────────────────────────────────────────────

class TestArchiveIndex:
    def setup_method(self):
        self.storage = MemoryStorageBackend()

    def _make_index(self) -> ArchiveIndex:
        idx = ArchiveIndex(
            source_root=SOURCE_ROOT, table=TABLE, archive_root=ARCHIVE_ROOT,
        )
        idx.snapshots = [
            ArchivedSnapshotEntry(
                snapshot_id=100, timestamp_ms=1_000_000, operation="append",
                data_files_count=5, size_bytes=1024,
            ),
            ArchivedSnapshotEntry(
                snapshot_id=200, timestamp_ms=2_000_000, operation="append",
                data_files_count=8, size_bytes=2048,
            ),
        ]
        return idx

    def test_save_and_load_roundtrip(self):
        idx = self._make_index()
        idx.save(self.storage)

        loaded = ArchiveIndex.load(self.storage, ARCHIVE_ROOT, TABLE)
        assert len(loaded.snapshots) == 2
        assert loaded.snapshots[0].snapshot_id == 100
        assert loaded.source_root == SOURCE_ROOT

    def test_load_or_new_returns_empty_when_missing(self):
        idx = ArchiveIndex.load_or_new(self.storage, ARCHIVE_ROOT, TABLE, SOURCE_ROOT)
        assert idx.snapshots == []

    def test_find_snapshot_by_id(self):
        idx = self._make_index()
        found = idx.find_snapshot(snapshot_id=100)
        assert found is not None
        assert found.snapshot_id == 100

    def test_find_snapshot_missing_returns_none(self):
        idx = self._make_index()
        assert idx.find_snapshot(snapshot_id=9999) is None

    def test_find_snapshot_as_of(self):
        idx = self._make_index()
        # as_of = 1970-01-01T00:00:01 (1000 ms) should return snap 100 (ts=1000ms)
        found = idx.find_snapshot(as_of_iso="1970-01-01T00:00:01")
        assert found is not None
        assert found.snapshot_id == 100

    def test_find_snapshot_latest_when_no_criteria(self):
        idx = self._make_index()
        found = idx.find_snapshot()
        assert found.snapshot_id == 200   # most recent


# ──────────────────────────────────────────────────────────────────────────────
# metadata_editor tests
# ──────────────────────────────────────────────────────────────────────────────

class TestExpireSnapshots:
    def setup_method(self):
        self.storage = MemoryStorageBackend()
        self.metadata = _seed_table(self.storage, SOURCE_ROOT, TABLE)

    def test_removes_expired_snapshot(self):
        table_root = f"{SOURCE_ROOT}/{TABLE}"
        new_uri = expire_snapshots_in_metadata(
            metadata=self.metadata,
            snapshot_ids_to_remove={100},
            table_root=table_root,
            storage=self.storage,
        )
        written = orjson.loads(self.storage.read_bytes(new_uri))
        ids = [s["snapshot-id"] for s in written["snapshots"]]
        assert 100 not in ids
        assert 200 in ids

    def test_snapshot_log_pruned(self):
        table_root = f"{SOURCE_ROOT}/{TABLE}"
        new_uri = expire_snapshots_in_metadata(
            metadata=self.metadata,
            snapshot_ids_to_remove={100},
            table_root=table_root,
            storage=self.storage,
        )
        written = orjson.loads(self.storage.read_bytes(new_uri))
        log_ids = [e["snapshot-id"] for e in written.get("snapshot-log", [])]
        assert 100 not in log_ids

    def test_new_version_written(self):
        table_root = f"{SOURCE_ROOT}/{TABLE}"
        new_uri = expire_snapshots_in_metadata(
            metadata=self.metadata,
            snapshot_ids_to_remove={100},
            table_root=table_root,
            storage=self.storage,
        )
        assert new_uri.endswith(".metadata.json")
        assert self.storage.exists(new_uri)


class TestWriteTableMetadata:
    def setup_method(self):
        self.storage = MemoryStorageBackend()
        self.metadata = _seed_table(self.storage, SOURCE_ROOT, TABLE)

    def test_writes_single_snapshot(self):
        table_root = f"{TARGET_ROOT}/{TABLE}"
        new_uri = write_table_metadata(
            storage=self.storage,
            archived_metadata=self.metadata,
            target_table_root=table_root,
            new_manifest_list_uri=f"{table_root}/metadata/ml-new.avro",
            new_snapshot_id=999,
        )
        written = orjson.loads(self.storage.read_bytes(new_uri))
        assert len(written["snapshots"]) == 1
        assert written["current-snapshot-id"] == 999
        assert written["location"] == table_root

    def test_location_updated_to_target(self):
        table_root = f"{TARGET_ROOT}/{TABLE}"
        new_uri = write_table_metadata(
            storage=self.storage,
            archived_metadata=self.metadata,
            target_table_root=table_root,
            new_manifest_list_uri=f"{table_root}/metadata/ml-new.avro",
            new_snapshot_id=999,
        )
        written = orjson.loads(self.storage.read_bytes(new_uri))
        assert written["location"].startswith(TARGET_ROOT)


# ──────────────────────────────────────────────────────────────────────────────
# IcebergArchiver end-to-end tests
# ──────────────────────────────────────────────────────────────────────────────

class TestIcebergArchiver:
    def setup_method(self):
        self.source = MemoryStorageBackend()
        self.archive = MemoryStorageBackend()
        _seed_table(self.source, SOURCE_ROOT, TABLE)

    def _archiver(self, delete_after=True) -> IcebergArchiver:
        return IcebergArchiver(
            source_storage=self.source,
            archive_storage=self.archive,
            source_root=SOURCE_ROOT,
            archive_root=ARCHIVE_ROOT,
            older_than="1ms",           # everything older than 1 ms is eligible
            min_snapshots_to_keep=1,    # always keep current
            delete_after_archive=delete_after,
            parallelism=1,
        )

    def test_dry_run_copies_nothing(self):
        result = self._archiver().archive_table(TABLE, dry_run=True)
        assert result.success
        assert result.snapshots_archived == 1   # snap 100 eligible
        assert self.archive.keys() == []        # nothing written

    def test_archive_copies_data_files(self):
        result = self._archiver().archive_table(TABLE, dry_run=False)
        assert result.success
        assert result.snapshots_archived == 1
        assert result.files_copied == 1
        # Old data file should exist in archive
        old_file = f"{ARCHIVE_ROOT}/{TABLE}/data/year=2024/month=01/old.parquet"
        assert self.archive.exists(old_file)

    def test_archive_copies_metadata_json(self):
        self._archiver().archive_table(TABLE, dry_run=False)
        # At least one versioned metadata.json should be in archive
        meta_files = [k for k in self.archive.keys() if k.endswith(".metadata.json")]
        assert len(meta_files) >= 1

    def test_archive_writes_index(self):
        self._archiver().archive_table(TABLE, dry_run=False)
        index_uri = f"{ARCHIVE_ROOT}/{TABLE}/.archive-manifest.json"
        assert self.archive.exists(index_uri)
        idx = ArchiveIndex.load(self.archive, ARCHIVE_ROOT, TABLE)
        assert len(idx.snapshots) == 1
        assert idx.snapshots[0].snapshot_id == 100

    def test_delete_after_archive_expires_from_primary(self):
        self._archiver(delete_after=True).archive_table(TABLE, dry_run=False)
        # Primary should now have a new metadata.json with snap 100 removed
        table_root = f"{SOURCE_ROOT}/{TABLE}"
        version = int(self.source.read_text(f"{table_root}/metadata/version-hint.text").strip())
        meta_uri = f"{table_root}/metadata/v{version:05d}.metadata.json"
        meta = orjson.loads(self.source.read_bytes(meta_uri))
        ids = [s["snapshot-id"] for s in meta["snapshots"]]
        assert 100 not in ids
        assert 200 in ids

    def test_no_delete_after_archive_leaves_primary_intact(self):
        self._archiver(delete_after=False).archive_table(TABLE, dry_run=False)
        table_root = f"{SOURCE_ROOT}/{TABLE}"
        # version-hint.text still points at original version 1
        version = int(self.source.read_text(f"{table_root}/metadata/version-hint.text").strip())
        assert version == 1

    def test_already_archived_not_re_archived(self):
        # Run twice; second run should archive nothing new
        archiver = self._archiver()
        archiver.archive_table(TABLE, dry_run=False)
        result2 = archiver.archive_table(TABLE, dry_run=False)
        assert result2.snapshots_archived == 0

    def test_shared_files_not_copied_twice(self):
        """Two snapshots sharing a data file should result in only one copy."""
        # Add a second snapshot that references the same data file
        source = self.source
        table_root = f"{SOURCE_ROOT}/{TABLE}"
        existing_file = f"{table_root}/data/year=2024/month=01/old.parquet"

        manifest_300_content = _manifest_bytes([{
            "status": 1,
            "data_file": {
                "file_path": existing_file,  # same file as snap 100
                "file_size_in_bytes": 16,
                "content": 0,
                "record_count": 10,
                "partition": {"year": 2024, "month": 1},
            },
        }])
        m300_uri = f"{table_root}/metadata/manifest-300.avro"
        source.put(m300_uri, manifest_300_content)

        ml_300 = _manifest_list_bytes([{
            "manifest_path": m300_uri,
            "manifest_length": len(manifest_300_content),
            "partition_spec_id": 0,
            "added_snapshot_id": 300,
            "added_data_files_count": 1,
            "existing_data_files_count": 0,
            "deleted_data_files_count": 0,
            "partitions": [],
        }])
        ml_300_uri = f"{table_root}/metadata/snap-300-ml.avro"
        source.put(ml_300_uri, ml_300)

        # Add snap 300 (also very old) to metadata and re-write
        meta = orjson.loads(source.read_bytes(f"{table_root}/metadata/v00001.metadata.json"))
        meta["snapshots"].append({
            "snapshot-id": 300,
            "timestamp-ms": 2,
            "manifest-list": ml_300_uri,
            "summary": {"operation": "append"},
        })
        source.put(f"{table_root}/metadata/v00001.metadata.json", orjson.dumps(meta))

        archiver = IcebergArchiver(
            source_storage=source,
            archive_storage=self.archive,
            source_root=SOURCE_ROOT,
            archive_root=ARCHIVE_ROOT,
            older_than="1ms",
            min_snapshots_to_keep=1,
            delete_after_archive=False,
            parallelism=1,
        )
        result = archiver.archive_table(TABLE, dry_run=False)
        assert result.success
        # files_copied should be 1 (the shared file), not 2
        assert result.files_copied == 1


# ──────────────────────────────────────────────────────────────────────────────
# IcebergRestorer tests
# ──────────────────────────────────────────────────────────────────────────────

def _setup_archive_and_target() -> tuple[MemoryStorageBackend, MemoryStorageBackend]:
    """
    Archive snap 100 to cold storage, return (archive_storage, target_storage).
    Target has only the current snapshot (200) with its data.
    """
    source = MemoryStorageBackend()
    archive = MemoryStorageBackend()
    _seed_table(source, SOURCE_ROOT, TABLE)

    archiver = IcebergArchiver(
        source_storage=source,
        archive_storage=archive,
        source_root=SOURCE_ROOT,
        archive_root=ARCHIVE_ROOT,
        older_than="1ms",
        min_snapshots_to_keep=1,
        delete_after_archive=False,
        parallelism=1,
    )
    archiver.archive_table(TABLE, dry_run=False)

    # Target starts as a copy of source (only current snap 200 visible)
    target = MemoryStorageBackend()
    _seed_table(target, TARGET_ROOT, TABLE)

    return archive, target


def _make_restorer(
    archive: MemoryStorageBackend,
    target: MemoryStorageBackend,
    partitions=None,
    mode="replace",
    snapshot_id=100,
) -> IcebergRestorer:
    from iceberg_sync.archive.config import RestoreJobConfig, S3Config, ADLSConfig, CatalogConfig, TransferConfig

    cfg = RestoreJobConfig(
        archive_root=ARCHIVE_ROOT,
        table=TABLE,
        target_root=TARGET_ROOT,
        snapshot_id=snapshot_id,
        partitions=partitions or [],
        mode=mode,
        conflict_strategy="overwrite",
    )

    restorer = IcebergRestorer(
        archive_storage=archive,
        target_storage=target,
        config=cfg,
    )
    return restorer


class TestIcebergRestorerPlan:
    def setup_method(self):
        self.archive, self.target = _setup_archive_and_target()

    def test_plan_returns_files(self):
        restorer = _make_restorer(self.archive, self.target)
        plan = restorer.plan()
        assert plan.total_files >= 1
        assert plan.snapshot_id == 100

    def test_plan_full_table_is_full(self):
        restorer = _make_restorer(self.archive, self.target, partitions=[])
        plan = restorer.plan()
        assert plan.is_full_table is True

    def test_plan_partition_is_not_full(self):
        restorer = _make_restorer(
            self.archive, self.target, partitions=[{"year": 2024, "month": 1}]
        )
        plan = restorer.plan()
        assert plan.is_full_table is False

    def test_plan_missing_snapshot_raises(self):
        restorer = _make_restorer(self.archive, self.target, snapshot_id=9999)
        with pytest.raises(ValueError, match="No archived snapshot found"):
            restorer.plan()

    def test_plan_no_matching_partition_raises(self):
        restorer = _make_restorer(
            self.archive, self.target, partitions=[{"year": 1900}]
        )
        with pytest.raises(ValueError, match="No files found"):
            restorer.plan()


class TestIcebergRestorerExecute:
    def setup_method(self):
        self.archive, self.target = _setup_archive_and_target()

    def test_full_table_restore_copies_files(self):
        restorer = _make_restorer(self.archive, self.target, mode="new_table")
        plan = restorer.plan()
        result = restorer.execute(plan)
        assert result.success
        assert result.files_copied >= 1

    def test_full_table_restore_writes_metadata(self):
        restorer = _make_restorer(self.archive, self.target, mode="new_table")
        plan = restorer.plan()
        result = restorer.execute(plan)
        assert result.new_metadata_uri != ""
        assert self.target.exists(result.new_metadata_uri)

    def test_partition_restore_copies_matching_files(self):
        restorer = _make_restorer(
            self.archive, self.target,
            partitions=[{"year": 2024, "month": 1}],
            mode="replace",
        )
        plan = restorer.plan()
        result = restorer.execute(plan)
        assert result.success
        restored_file = f"{TARGET_ROOT}/{TABLE}/data/year=2024/month=01/old.parquet"
        assert self.target.exists(restored_file)

    def test_restore_version_hint_updated(self):
        restorer = _make_restorer(self.archive, self.target, mode="new_table")
        plan = restorer.plan()
        result = restorer.execute(plan)
        # version-hint.text should point to the new metadata version
        table_root = f"{TARGET_ROOT}/{TABLE}"
        hint = self.target.read_text(f"{table_root}/metadata/version-hint.text").strip()
        filename = result.new_metadata_uri.split("/")[-1]
        expected_ver = filename.split(".")[0].lstrip("v")
        assert hint == expected_ver


# ──────────────────────────────────────────────────────────────────────────────
# _utils tests
# ──────────────────────────────────────────────────────────────────────────────

class TestFmtBytes:
    def test_bytes(self):
        assert fmt_bytes(512) == "512.0 B"

    def test_kilobytes(self):
        assert fmt_bytes(2048) == "2.0 KB"

    def test_megabytes_decimal(self):
        # 1.5 MB = 1572864 bytes
        assert fmt_bytes(1_572_864) == "1.5 MB"

    def test_gigabytes(self):
        assert fmt_bytes(2 * 1024 ** 3) == "2.0 GB"

    def test_zero(self):
        assert fmt_bytes(0) == "0.0 B"


# ──────────────────────────────────────────────────────────────────────────────
# config.py — storage_kwargs_for_uri tests
# ──────────────────────────────────────────────────────────────────────────────

class TestStorageKwargsForUri:
    def test_s3_uri_returns_s3_kwargs(self):
        from iceberg_sync.archive.config import storage_kwargs_for_uri, S3Config, ADLSConfig

        s3 = S3Config(region="us-east-1", access_key="key", secret_key="secret")
        adls = ADLSConfig(account_name="account", account_key="accountkey")

        kwargs = storage_kwargs_for_uri("s3://my-bucket/path", s3, adls)
        assert "aws_access_key_id" in kwargs
        assert "storage_account_name" not in kwargs

    def test_adls_uri_returns_adls_kwargs(self):
        from iceberg_sync.archive.config import storage_kwargs_for_uri, S3Config, ADLSConfig

        s3 = S3Config(region="us-east-1", access_key="key", secret_key="secret")
        adls = ADLSConfig(account_name="account", account_key="accountkey")

        kwargs = storage_kwargs_for_uri("abfss://container@account.dfs.core.windows.net/", s3, adls)
        assert "storage_account_name" in kwargs
        assert "aws_access_key_id" not in kwargs

    def test_unknown_scheme_returns_empty(self):
        from iceberg_sync.archive.config import storage_kwargs_for_uri, S3Config, ADLSConfig

        kwargs = storage_kwargs_for_uri("gs://bucket/path", S3Config(), ADLSConfig())
        assert kwargs == {}
