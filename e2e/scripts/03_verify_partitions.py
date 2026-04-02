"""
e2e/scripts/03_verify_partitions.py
=====================================
Query the tpch/orders Iceberg table through the catalog-gateway and print
a row-count breakdown by partition (order_month).

Run after 02_create_iceberg_table.py to confirm all 10 partitions are present.

Usage:
  python e2e/scripts/03_verify_partitions.py
"""

from __future__ import annotations

import os

from pyiceberg.catalog import load_catalog

GATEWAY_URL     = os.getenv("GATEWAY_URL",     "http://localhost:8083")
OAUTH_URL       = os.getenv("OAUTH_URL",       "http://localhost:8081/oauth2/token")
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID", "admin-client")
OAUTH_SECRET    = os.getenv("OAUTH_SECRET",    "admin-secret")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY","minioadmin")


def main() -> None:
    print("=== Step 3: Verify Partitions ===\n")

    catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": GATEWAY_URL,
            "oauth2-server-uri": OAUTH_URL,
            "credential": f"{OAUTH_CLIENT_ID}:{OAUTH_SECRET}",
            "scope": "catalog:read catalog:write",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS,
            "s3.secret-access-key": MINIO_SECRET,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )

    table = catalog.load_table("tpch.orders")
    print(f"Table location : {table.location()}")
    print(f"Schema fields  : {[f.name for f in table.schema().fields]}")
    print(f"Partition spec : {table.spec()}")
    print()

    # Count snapshots in the table history
    snapshots = list(table.history())
    print(f"Snapshots in history: {len(snapshots)}")
    for snap in snapshots:
        print(f"  snapshot_id={snap.snapshot_id}  parent_id={snap.parent_snapshot_id}")
    print()

    # Scan and count rows by month
    print("Row counts by partition (order_month):")
    print("-" * 35)
    arrow_tbl = table.scan().to_arrow()

    import pyarrow.compute as pc
    months = arrow_tbl.column("order_month").unique().to_pylist()
    total = 0
    for month in sorted(months):
        mask = pc.equal(arrow_tbl.column("order_month"), month)
        count = pc.sum(mask.cast("int64")).as_py()
        total += count
        print(f"  order_month={month}   {count:>10,} rows")

    print("-" * 35)
    print(f"  TOTAL                  {total:>10,} rows")
    print()
    print(f"Partitions found: {len(months)}")


if __name__ == "__main__":
    main()
