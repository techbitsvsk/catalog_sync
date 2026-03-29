"""
Seeds a minimal Iceberg table in MinIO at:
  s3a://warehouse/source/gold/orders/

This gives iceberg-sync a real source table to read, copy, and register in Nessie
(used by DEVELOPMENT.md section 8.7 — Run a Full Sync with Policy Enforcement).

The Parquet file contains stub bytes only — enough for iceberg-sync to copy the
metadata chain and rewrite URIs. For a PySpark test that actually queries rows,
see scripts/test_pyspark_nessie.py (section 8.14), which seeds real data via Spark.

Prerequisites:
  pip install boto3 fastavro

Run from the project root:
  python scripts/seed_test_data.py
"""
import io, json, uuid, time
import boto3
import fastavro

ENDPOINT     = "http://localhost:9000"
KEY          = "minioadmin"
SECRET       = "minioadmin"
BUCKET       = "warehouse"
TABLE_PREFIX = "source/gold/orders"

s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                  aws_access_key_id=KEY, aws_secret_access_key=SECRET)

snap_id       = int(time.time() * 1000)
data_file_uri = f"s3a://{BUCKET}/{TABLE_PREFIX}/data/00001.parquet"
manifest_uri  = f"s3a://{BUCKET}/{TABLE_PREFIX}/metadata/m-{snap_id}.avro"
manlist_uri   = f"s3a://{BUCKET}/{TABLE_PREFIX}/metadata/snap-{snap_id}-m-1.avro"
metadata_uri  = f"s3a://{BUCKET}/{TABLE_PREFIX}/metadata/v1.metadata.json"


def put(key, data):
    if isinstance(data, str):
        data = data.encode()
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)
    print(f"  PUT s3a://{BUCKET}/{key}")


# ── Parquet stub ─────────────────────────────────────────────────────────────
# iceberg-sync copies the bytes; the content only needs valid PAR1 magic bytes.
put(f"{TABLE_PREFIX}/data/00001.parquet", b"PAR1" + b"\x00" * 100 + b"PAR1")

# ── Manifest (Avro) ──────────────────────────────────────────────────────────
manifest_schema = fastavro.parse_schema({
    "type": "record", "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int"},
        {"name": "data_file", "type": {"type": "record", "name": "r2",
            "fields": [
                {"name": "file_path",          "type": "string"},
                {"name": "file_format",         "type": "string"},
                {"name": "record_count",        "type": "long"},
                {"name": "file_size_in_bytes",  "type": "long"},
            ]
        }}
    ]
})
manifest_records = [{
    "status": 1,
    "data_file": {
        "file_path":         data_file_uri,
        "file_format":       "PARQUET",
        "record_count":      1000,
        "file_size_in_bytes": 108,
    }
}]
buf = io.BytesIO()
fastavro.writer(buf, manifest_schema, manifest_records)
put(f"{TABLE_PREFIX}/metadata/m-{snap_id}.avro", buf.getvalue())

# ── Manifest list (Avro) ─────────────────────────────────────────────────────
manlist_schema = fastavro.parse_schema({
    "type": "record", "name": "manifest_file",
    "fields": [
        {"name": "manifest_path",              "type": "string"},
        {"name": "manifest_length",            "type": "long"},
        {"name": "partition_spec_id",          "type": "int"},
        {"name": "added_snapshot_id",          "type": "long"},
        {"name": "added_data_files_count",     "type": "int"},
        {"name": "existing_data_files_count",  "type": "int"},
        {"name": "deleted_data_files_count",   "type": "int"},
    ]
})
buf2 = io.BytesIO()
fastavro.writer(buf2, manlist_schema, [{
    "manifest_path":             manifest_uri,
    "manifest_length":           len(buf.getvalue()),
    "partition_spec_id":         0,
    "added_snapshot_id":         snap_id,
    "added_data_files_count":    1,
    "existing_data_files_count": 0,
    "deleted_data_files_count":  0,
}])
put(f"{TABLE_PREFIX}/metadata/snap-{snap_id}-m-1.avro", buf2.getvalue())

# ── metadata.json ────────────────────────────────────────────────────────────
metadata = {
    "format-version": 1,
    "table-uuid": str(uuid.uuid4()),
    "location": f"s3a://{BUCKET}/{TABLE_PREFIX}",
    "schema": {
        "type": "struct", "schema-id": 0,
        "fields": [{"id": 1, "name": "id", "type": "long", "required": True}],
    },
    "current-snapshot-id": snap_id,
    "snapshots": [{
        "snapshot-id":  snap_id,
        "timestamp-ms": int(time.time() * 1000),
        "summary":      {"operation": "append"},
        "manifest-list": manlist_uri,
    }],
    "metadata-log":    [],
    "partition-spec":  [],
    "partition-specs": [{"spec-id": 0, "fields": []}],
}
put(f"{TABLE_PREFIX}/metadata/v1.metadata.json", json.dumps(metadata))
put(f"{TABLE_PREFIX}/metadata/version-hint.text", "1")

print("\nTest table seeded at:")
print(f"  s3a://{BUCKET}/{TABLE_PREFIX}/")
print("\nNext step (DEVELOPMENT.md §8.7):")
print("  iceberg-sync table --source-root s3a://warehouse/source/ \\")
print("    --target-root s3a://warehouse/target/ --table gold/orders ...")
