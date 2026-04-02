"""
e2e/scripts/02_create_iceberg_table.py
========================================
Create the tpch/orders Iceberg table in the local Nessie catalog via the
catalog-gateway REST endpoint, then upload raw Parquet data to MinIO.

Prerequisites:
  pip install pyiceberg[s3,nessie] boto3 pyarrow

Services (start with `docker compose -f docker/docker-compose.yml up -d`):
  Catalog Gateway:  http://localhost:8083  (Iceberg REST Catalog)
  OAuth service:    http://localhost:8081
  MinIO S3:         http://localhost:9000  (minioadmin / minioadmin)

Usage:
  python e2e/scripts/02_create_iceberg_table.py [--data-dir e2e/data/orders]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

# ── Connection constants ────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "warehouse")

GATEWAY_URL      = os.getenv("GATEWAY_URL",      "http://localhost:8083")
OAUTH_URL        = os.getenv("OAUTH_URL",        "http://localhost:8081/oauth2/token")
OAUTH_CLIENT_ID  = os.getenv("OAUTH_CLIENT_ID",  "admin-client")
OAUTH_SECRET     = os.getenv("OAUTH_SECRET",     "admin-secret")

NAMESPACE = "tpch"
TABLE     = "orders"

# Iceberg table schema mirroring the generated Parquet files
ICEBERG_SCHEMA = Schema(
    NestedField(1,  "order_key",      LongType(),           required=True),
    NestedField(2,  "customer_key",   LongType()),
    NestedField(3,  "order_status",   StringType()),
    NestedField(4,  "total_price",    DecimalType(15, 2)),
    NestedField(5,  "order_date",     DateType()),
    NestedField(6,  "order_priority", StringType()),
    NestedField(7,  "clerk",          StringType()),
    NestedField(8,  "ship_priority",  IntegerType()),
    NestedField(9,  "comment",        StringType()),
    NestedField(10, "order_month",    StringType()),
)

# Partition by order_month (identity transform on the string YYYY-MM column)
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=10, field_id=1000, transform=IdentityTransform(), name="order_month"),
)


def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"  Bucket '{bucket}' already exists.")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"  Created bucket '{bucket}'.")


def ensure_cold_bucket(s3) -> None:
    """Create the archive bucket for cold storage."""
    try:
        s3.head_bucket(Bucket="archive") if False else None
        cold_bucket = "warehouse"   # use a prefix inside warehouse for simplicity
    except Exception:
        pass
    # For the E2E test we use warehouse/archive/ prefix — no separate bucket needed.


def get_catalog():
    """Return a PyIceberg catalog connected to the catalog-gateway REST endpoint."""
    return load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": GATEWAY_URL,
            "oauth2-server-uri": OAUTH_URL,
            "credential": f"{OAUTH_CLIENT_ID}:{OAUTH_SECRET}",
            "scope": "catalog:read catalog:write",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def create_table(catalog) -> object:
    """Create tpch namespace and orders table if they don't exist."""
    # Ensure namespace exists
    existing_ns = [ns[0] for ns in catalog.list_namespaces()]
    if NAMESPACE not in existing_ns:
        catalog.create_namespace(NAMESPACE)
        print(f"  Created namespace '{NAMESPACE}'.")
    else:
        print(f"  Namespace '{NAMESPACE}' already exists.")

    full_name = f"{NAMESPACE}.{TABLE}"
    if catalog.table_exists(full_name):
        print(f"  Table '{full_name}' already exists — loading.")
        return catalog.load_table(full_name)

    table = catalog.create_table(
        full_name,
        schema=ICEBERG_SCHEMA,
        partition_spec=PARTITION_SPEC,
        location=f"s3a://{MINIO_BUCKET}/iceberg/{NAMESPACE}/{TABLE}",
        properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
    )
    print(f"  Created table '{full_name}' at {table.location()}.")
    return table


def write_partitions(table, data_dir: Path) -> None:
    """Write each monthly Parquet partition to the Iceberg table."""
    part_dirs = sorted(data_dir.glob("order_month=*"))
    if not part_dirs:
        raise FileNotFoundError(
            f"No partition directories found under {data_dir}. "
            "Run 01_generate_tpch.py first."
        )

    print(f"\nWriting {len(part_dirs)} partitions …")
    for part_dir in part_dirs:
        month = part_dir.name.split("=", 1)[1]
        parquet_files = list(part_dir.glob("*.parquet"))
        if not parquet_files:
            continue

        arrow_tbl = pq.read_table(parquet_files[0])
        row_count = len(arrow_tbl)
        table.append(arrow_tbl)
        print(f"  order_month={month:8s}  {row_count:>10,} rows written")

    print(f"\nAll {len(part_dirs)} partitions loaded into {NAMESPACE}.{TABLE}.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Iceberg table and load partitions.")
    parser.add_argument("--data-dir", type=Path, default=Path("e2e/data/orders"),
                        help="Directory containing the generated Parquet partitions")
    args = parser.parse_args()

    print("=== Step 2: Create Iceberg Table ===\n")

    print("1. Ensuring MinIO buckets …")
    s3 = get_s3_client()
    ensure_bucket(s3, MINIO_BUCKET)

    print("\n2. Connecting to catalog-gateway …")
    catalog = get_catalog()
    print(f"   Namespaces: {[ns[0] for ns in catalog.list_namespaces()]}")

    print(f"\n3. Creating table '{NAMESPACE}.{TABLE}' …")
    table = create_table(catalog)

    print("\n4. Loading monthly partitions …")
    write_partitions(table, args.data_dir)

    print("\n=== Setup complete. Run the following to verify: ===")
    print(f"""
  python -c "
from pyiceberg.catalog import load_catalog
cat = load_catalog('rest', **{{
    'type': 'rest',
    'uri': '{GATEWAY_URL}',
    'oauth2-server-uri': '{OAUTH_URL}',
    'credential': '{OAUTH_CLIENT_ID}:{OAUTH_SECRET}',
}})
tbl = cat.load_table('{NAMESPACE}.{TABLE}')
df = tbl.scan().to_arrow()
print(df.group_by('order_month').aggregate([('order_key','count')]).sort_by('order_month'))
  "
""")


if __name__ == "__main__":
    main()
