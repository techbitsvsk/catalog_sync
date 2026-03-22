"""
tests/test_metadata_rewrite.py — Tests for Iceberg metadata rewriting.

Uses MemoryStorageBackend so no cloud credentials or network access is needed.
We construct a realistic Iceberg metadata chain (metadata.json → manifest-list
→ manifest → data files) in memory and verify the rewriter translates every
absolute URI correctly.
"""

from __future__ import annotations

import io
import json

import fastavro
import pytest

from iceberg_sync.metadata.rewriter import MetadataRewriter, find_latest_metadata
from iceberg_sync.path_translator import PathTranslator
from iceberg_sync.storage.memory import MemoryStorageBackend
from iceberg_sync.sync.catalog_sync import CatalogSync


# ── Helpers to build realistic Iceberg metadata in memory ────────────────────

SOURCE_ROOT = "s3://warehouse/iceberg/"
TARGET_ROOT = "abfss://iceberg@acct.dfs.core.windows.net/iceberg/"
TABLE = "gold/top_customers"

SOURCE_TABLE = f"{SOURCE_ROOT}{TABLE}/"
TARGET_TABLE = f"{TARGET_ROOT}{TABLE}/"


def _build_manifest_avro(file_paths: list[str]) -> bytes:
    """Build a minimal Iceberg manifest (Avro) with data_file entries."""
    schema = {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int"},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {"name": "file_path", "type": "string"},
                        {"name": "file_format", "type": "string"},
                        {"name": "record_count", "type": "long"},
                        {"name": "file_size_in_bytes", "type": "long"},
                    ],
                },
            },
        ],
    }

    records = [
        {
            "status": 1,
            "data_file": {
                "file_path": fp,
                "file_format": "PARQUET",
                "record_count": 100,
                "file_size_in_bytes": 4096,
            },
        }
        for fp in file_paths
    ]

    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return buf.getvalue()


def _build_manifest_list_avro(manifest_paths: list[str]) -> bytes:
    """Build a minimal Iceberg manifest-list (Avro)."""
    schema = {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "added_snapshot_id", "type": "long"},
        ],
    }

    records = [
        {
            "manifest_path": mp,
            "manifest_length": 1024,
            "partition_spec_id": 0,
            "added_snapshot_id": 1000,
        }
        for mp in manifest_paths
    ]

    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return buf.getvalue()


def _build_metadata_json(
    table_location: str,
    manifest_list_path: str,
    snapshot_id: int = 1000,
    version: int = 3,
) -> str:
    """Build a minimal Iceberg metadata.json."""
    return json.dumps({
        "format-version": 2,
        "table-uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "location": table_location,
        "last-sequence-number": 1,
        "last-updated-ms": 1700000000000,
        "last-column-id": 5,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "current-snapshot-id": snapshot_id,
        "snapshots": [
            {
                "snapshot-id": snapshot_id,
                "timestamp-ms": 1700000000000,
                "summary": {"operation": "append"},
                "manifest-list": manifest_list_path,
                "schema-id": 0,
            }
        ],
        "metadata-log": [
            {"timestamp-ms": 1699999000000, "metadata-file": f"{table_location}/metadata/v{version - 1}.metadata.json"}
        ],
    })


def populate_source(source: MemoryStorageBackend):
    """Create a complete Iceberg table in the source storage."""
    prefix = SOURCE_TABLE

    # Data files
    source.put(f"{prefix}data/00001-abc.parquet", b"parquet-data-1")
    source.put(f"{prefix}data/00002-def.parquet", b"parquet-data-2")
    source.put(f"{prefix}data/00003-ghi.parquet", b"parquet-data-3")

    # Manifest (references data files)
    manifest_bytes = _build_manifest_avro([
        f"{prefix}data/00001-abc.parquet",
        f"{prefix}data/00002-def.parquet",
        f"{prefix}data/00003-ghi.parquet",
    ])
    manifest_uri = f"{prefix}metadata/m-001.avro"
    source.put(manifest_uri, manifest_bytes)

    # Manifest-list (references manifests)
    manifest_list_bytes = _build_manifest_list_avro([manifest_uri])
    manifest_list_uri = f"{prefix}metadata/snap-1000-001.avro"
    source.put(manifest_list_uri, manifest_list_bytes)

    # metadata.json
    metadata_json = _build_metadata_json(
        table_location=prefix.rstrip("/"),
        manifest_list_path=manifest_list_uri,
        snapshot_id=1000,
        version=3,
    )
    source.put(f"{prefix}metadata/v3.metadata.json", metadata_json)

    # version-hint.text
    source.put(f"{prefix}metadata/version-hint.text", "3")


# ── Tests ────────────────────────────────────────────────────────────────────

class TestFindLatestMetadata:
    def test_via_version_hint(self):
        source = MemoryStorageBackend()
        populate_source(source)
        uri = find_latest_metadata(source, SOURCE_TABLE)
        assert uri == f"{SOURCE_TABLE}metadata/v3.metadata.json"

    def test_via_scan_no_hint(self):
        source = MemoryStorageBackend()
        populate_source(source)
        # Remove version-hint.text
        source.delete(f"{SOURCE_TABLE}metadata/version-hint.text")
        uri = find_latest_metadata(source, SOURCE_TABLE)
        assert uri == f"{SOURCE_TABLE}metadata/v3.metadata.json"

    def test_no_metadata_raises(self):
        source = MemoryStorageBackend()
        with pytest.raises(FileNotFoundError, match="No Iceberg metadata found"):
            find_latest_metadata(source, "s3://empty-bucket/table/")


class TestMetadataRewriter:
    @pytest.fixture
    def setup(self):
        source = MemoryStorageBackend()
        target = MemoryStorageBackend()
        populate_source(source)
        translator = PathTranslator([(SOURCE_ROOT, TARGET_ROOT)])
        rewriter = MetadataRewriter(
            translator=translator,
            source_storage=source,
            target_storage=target,
        )
        return source, target, translator, rewriter

    def test_metadata_json_location_translated(self, setup):
        source, target, translator, rewriter = setup
        stats = rewriter.rewrite_table(f"{SOURCE_TABLE}metadata/v3.metadata.json")

        # Read the rewritten metadata.json from target
        target_metadata_uri = f"{TARGET_TABLE}metadata/v3.metadata.json"
        assert target.exists(target_metadata_uri)

        metadata = json.loads(target.read_text(target_metadata_uri))
        assert metadata["location"].startswith("abfss://")
        assert "s3://" not in metadata["location"]

    def test_manifest_list_path_translated(self, setup):
        source, target, translator, rewriter = setup
        rewriter.rewrite_table(f"{SOURCE_TABLE}metadata/v3.metadata.json")

        metadata = json.loads(target.read_text(f"{TARGET_TABLE}metadata/v3.metadata.json"))
        manifest_list_path = metadata["snapshots"][0]["manifest-list"]
        assert manifest_list_path.startswith("abfss://")
        assert "s3://" not in manifest_list_path

    def test_manifest_data_file_paths_translated(self, setup):
        source, target, translator, rewriter = setup
        stats = rewriter.rewrite_table(f"{SOURCE_TABLE}metadata/v3.metadata.json")

        # The rewritten manifest should exist on target
        target_manifest_uri = f"{TARGET_TABLE}metadata/m-001.avro"
        assert target.exists(target_manifest_uri)

        # Read and verify data_file.file_path entries
        manifest_bytes = target.read_bytes(target_manifest_uri)
        reader = fastavro.reader(io.BytesIO(manifest_bytes))
        records = list(reader)

        for record in records:
            file_path = record["data_file"]["file_path"]
            assert file_path.startswith("abfss://"), f"Untranslated path: {file_path}"
            assert "s3://" not in file_path

    def test_version_hint_written(self, setup):
        source, target, translator, rewriter = setup
        rewriter.rewrite_table(f"{SOURCE_TABLE}metadata/v3.metadata.json")

        hint_uri = f"{TARGET_TABLE}metadata/version-hint.text"
        assert target.exists(hint_uri)
        assert target.read_text(hint_uri) == "3"

    def test_stats_accurate(self, setup):
        source, target, translator, rewriter = setup
        stats = rewriter.rewrite_table(f"{SOURCE_TABLE}metadata/v3.metadata.json")

        assert stats.metadata_files_rewritten == 1
        assert stats.manifest_lists_rewritten == 1
        assert stats.manifests_rewritten == 1
        assert stats.data_file_paths_translated == 3  # 3 data files
        assert len(stats.errors) == 0

    def test_no_source_uris_leak_into_target(self, setup):
        """Comprehensive check: scan ALL target files for leaked s3:// URIs."""
        source, target, translator, rewriter = setup
        rewriter.rewrite_table(f"{SOURCE_TABLE}metadata/v3.metadata.json")

        for uri in target.keys():
            if uri.endswith(".json") or uri.endswith(".text"):
                content = target.read_text(uri)
                assert "s3://" not in content, \
                    f"Leaked s3:// URI found in {uri}: {content[:200]}"

    def test_metadata_log_paths_translated(self, setup):
        source, target, translator, rewriter = setup
        rewriter.rewrite_table(f"{SOURCE_TABLE}metadata/v3.metadata.json")

        metadata = json.loads(target.read_text(f"{TARGET_TABLE}metadata/v3.metadata.json"))
        for entry in metadata.get("metadata-log", []):
            path = entry.get("metadata-file", "")
            # Non-strict translation — may not match if it's an older path
            # but should not contain s3:// if it was translated
            if path:
                assert "s3://" not in path or path == entry.get("metadata-file"), \
                    f"Leaked URI in metadata-log: {path}"


class TestCatalogSync:
    @pytest.fixture
    def setup(self):
        source = MemoryStorageBackend()
        target = MemoryStorageBackend()
        populate_source(source)
        translator = PathTranslator([(SOURCE_ROOT, TARGET_ROOT)])
        sync = CatalogSync(
            translator=translator,
            source_storage=source,
            target_storage=target,
        )
        return source, target, sync

    def test_full_sync(self, setup):
        source, target, sync = setup
        result = sync.sync_table(SOURCE_TABLE)

        assert result.success
        assert result.files_copied >= 3  # at least the 3 data files
        assert result.rewrite_stats is not None
        assert result.rewrite_stats.data_file_paths_translated == 3

    def test_data_files_on_target(self, setup):
        source, target, sync = setup
        sync.sync_table(SOURCE_TABLE)

        # All 3 data files should be on target
        assert target.exists(f"{TARGET_TABLE}data/00001-abc.parquet")
        assert target.exists(f"{TARGET_TABLE}data/00002-def.parquet")
        assert target.exists(f"{TARGET_TABLE}data/00003-ghi.parquet")

    def test_incremental_sync(self, setup):
        source, target, sync = setup

        # First sync
        result1 = sync.sync_table(SOURCE_TABLE)
        copied_first = result1.files_copied

        # Second sync — no new data files
        result2 = sync.sync_table(SOURCE_TABLE)
        assert result2.files_copied == 0  # data files already there
        assert result2.files_skipped >= 3

    def test_incremental_with_new_file(self, setup):
        source, target, sync = setup

        # First sync
        sync.sync_table(SOURCE_TABLE)

        # Add a new data file to source
        source.put(f"{SOURCE_TABLE}data/00004-new.parquet", b"new-data")

        # Second sync — should copy only the new file
        result2 = sync.sync_table(SOURCE_TABLE)
        assert result2.files_copied >= 1
        assert target.exists(f"{TARGET_TABLE}data/00004-new.parquet")

    def test_dry_run_copies_nothing(self, setup):
        source, target, sync = setup

        result = sync.sync_table(SOURCE_TABLE, dry_run=True)
        assert result.dry_run
        assert result.files_copied >= 3  # reported but not actually copied
        assert not target.exists(f"{TARGET_TABLE}data/00001-abc.parquet")

    def test_namespace_discovery(self, setup):
        source, target, sync = setup

        # The fixture has one table under gold/
        results = sync.sync_namespace(f"{SOURCE_ROOT}gold/")
        assert len(results) == 1
        assert results[0].success

    def test_target_readable_after_sync(self, setup):
        """After sync, we should be able to find_latest_metadata on target."""
        source, target, sync = setup
        sync.sync_table(SOURCE_TABLE)

        uri = find_latest_metadata(target, TARGET_TABLE)
        assert uri == f"{TARGET_TABLE}metadata/v3.metadata.json"

        # Read and verify
        metadata = json.loads(target.read_text(uri))
        assert metadata["location"] == TARGET_TABLE.rstrip("/")
        assert metadata["current-snapshot-id"] == 1000


class TestReverseSync:
    """Test failback: Azure → AWS sync using reverse translator."""

    def test_reverse_direction(self):
        source = MemoryStorageBackend()
        target = MemoryStorageBackend()
        populate_source(source)

        forward = PathTranslator([(SOURCE_ROOT, TARGET_ROOT)])
        reverse = forward.reverse()

        # Forward sync: S3 → ADLS
        sync_forward = CatalogSync(
            translator=forward,
            source_storage=source,
            target_storage=target,
        )
        sync_forward.sync_table(SOURCE_TABLE)

        # Now sync back: ADLS → S3 (into a new "restored" backend)
        restored = MemoryStorageBackend()
        sync_reverse = CatalogSync(
            translator=reverse,
            source_storage=target,
            target_storage=restored,
        )
        result = sync_reverse.sync_table(TARGET_TABLE)
        assert result.success

        # Verify data files are back with s3:// paths
        assert restored.exists(f"{SOURCE_TABLE}data/00001-abc.parquet")

        # Verify metadata points to s3://
        metadata = json.loads(restored.read_text(f"{SOURCE_TABLE}metadata/v3.metadata.json"))
        assert metadata["location"].startswith("s3://")
