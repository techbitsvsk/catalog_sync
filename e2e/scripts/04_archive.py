"""
e2e/scripts/04_archive.py
===========================
Archive the 5 oldest monthly partitions of tpch/orders to cold storage
using the IcebergArchiver Python API.

Strategy:
  - Table has snapshots for 2024-01 through 2024-10 (10 monthly append snapshots)
  - Keep the 5 most recent snapshots on primary (2024-06 → 2024-10)
  - Archive the 5 oldest (2024-01 → 2024-05)

Run:
  python e2e/scripts/04_archive.py [--dry-run]  (default: dry-run)
  python e2e/scripts/04_archive.py --execute    (actually archive)
"""

from __future__ import annotations

import argparse
import os

from iceberg_sync.archive.archiver import IcebergArchiver
from iceberg_sync.archive.config import (
    ArchiveJobConfig,
    CatalogConfig,
    S3Config,
    TransferConfig,
)

# ── Connection constants ────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
OAUTH_URL        = os.getenv("OAUTH_URL",        "http://localhost:8081/oauth2/token")


def build_config(dry_run: bool) -> ArchiveJobConfig:
    s3 = S3Config(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        region="us-east-1",
    )
    return ArchiveJobConfig(
        source_root="s3a://warehouse/iceberg",
        table="tpch/orders",
        archive_root="s3a://warehouse/archive",
        # Archive snapshots older than ~5 months so the 5 oldest monthly
        # snapshots are eligible. Adjust if your demo is run much later.
        older_than="150d",
        min_snapshots_to_keep=5,
        delete_after_archive=True,
        dry_run=dry_run,
        source_s3=s3,
        archive_s3=s3,   # same MinIO, different path prefix
        catalog=CatalogConfig(
            nessie_uri="http://localhost:19120",
            nessie_ref="main",
            oauth_url=OAUTH_URL,
            oauth_client_id="sync-service",
            oauth_client_secret="sync-secret",
        ),
        transfer=TransferConfig(parallelism=8),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive old tpch/orders partitions.")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--dry-run",  dest="dry_run", action="store_true",  default=True,
                     help="Show what would be archived (default)")
    grp.add_argument("--execute",  dest="dry_run", action="store_false",
                     help="Execute the archive (copy files and update metadata)")
    args = parser.parse_args()

    dry_run = args.dry_run
    mode_label = "DRY-RUN" if dry_run else "EXECUTE"

    print(f"=== Step 4: Archive Old Partitions [{mode_label}] ===\n")

    cfg = build_config(dry_run=dry_run)
    archiver = IcebergArchiver.from_config(cfg)

    result = archiver.archive_table("tpch/orders", dry_run=dry_run)

    print()
    print("─" * 50)
    print(f"Table             : {result.table}")
    print(f"Snapshots archived: {result.snapshots_archived}")
    print(f"Snapshots skipped : {result.snapshots_skipped}")
    print(f"Files copied      : {result.files_copied}")
    print(f"Bytes copied      : {result.bytes_copied:,}")
    print(f"Errors            : {len(result.errors)}")
    print(f"Success           : {result.success}")
    print("─" * 50)

    if result.errors:
        print("\nErrors:")
        for e in result.errors:
            print(f"  • {e}")

    if dry_run:
        print("\n[DRY RUN] No data was written.")
        print("Re-run with --execute to apply.\n")
    else:
        print("\nArchive complete. Run 05_restore.py to verify restore works.\n")


if __name__ == "__main__":
    main()
