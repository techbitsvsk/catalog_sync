"""
e2e/scripts/05_restore.py
===========================
Restore the March-2024 partition (order_month=2024-03) from archive
back into the live tpch/orders table using the IcebergRestorer Python API.

Workflow:
  1. list_snapshots()  — show what is available in the archive index
  2. plan()            — dry-run: show what will be restored
  3. execute(plan)     — copy files and rewrite metadata  (only with --confirm)

Run:
  python e2e/scripts/05_restore.py           (plan only)
  python e2e/scripts/05_restore.py --confirm (execute restore)
  python e2e/scripts/05_restore.py --list    (only list snapshots)
"""

from __future__ import annotations

import argparse
import os

from iceberg_sync.archive.config import (
    CatalogConfig,
    RestoreJobConfig,
    S3Config,
    TransferConfig,
)
from iceberg_sync.archive.restorer import IcebergRestorer

# ── Connection constants ────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
OAUTH_URL        = os.getenv("OAUTH_URL",        "http://localhost:8081/oauth2/token")


def build_config(*, partitions: list[dict], as_of: str | None) -> RestoreJobConfig:
    s3 = S3Config(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        region="us-east-1",
    )
    return RestoreJobConfig(
        archive_root="s3a://warehouse/archive",
        table="tpch/orders",
        target_root="s3a://warehouse/iceberg",
        as_of=as_of or "2024-04-01",
        partitions=partitions,
        mode="replace",
        conflict_strategy="skip",
        archive_s3=s3,
        target_s3=s3,
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
    parser = argparse.ArgumentParser(description="Restore an archived partition of tpch/orders.")
    parser.add_argument("--list",    action="store_true",
                        help="List available snapshots in the archive index and exit.")
    parser.add_argument("--confirm", action="store_true",
                        help="Execute the restore (default: plan/dry-run only).")
    parser.add_argument("--month",   default="2024-03",
                        help="Month partition to restore as YYYY-MM (default: 2024-03)")
    parser.add_argument("--as-of",   default=None,
                        help="Restore the snapshot as of this date (YYYY-MM-DD)")
    args = parser.parse_args()

    partitions = [{"order_month": args.month}]
    cfg = build_config(partitions=partitions, as_of=args.as_of)
    restorer = IcebergRestorer.from_config(cfg)

    # ── Step 1: List ─────────────────────────────────────────────────────────
    print("=== Available Archive Snapshots ===")
    restorer.list_snapshots()

    if args.list:
        return

    # ── Step 2: Plan (always) ─────────────────────────────────────────────────
    print(f"\n=== Restore Plan: order_month={args.month} ===")
    plan = restorer.plan()

    if not args.confirm:
        print("\n[DRY RUN] No data was written.")
        print("Re-run with --confirm to execute the restore.\n")
        return

    # ── Step 3: Execute ───────────────────────────────────────────────────────
    if plan.has_conflicts:
        print(f"\nConflicts detected ({len(plan.conflicts)} partition(s)). "
              "Proceeding with conflict_strategy=skip …")

    print("\n=== Executing Restore … ===")
    result = restorer.execute(plan)

    print()
    print("─" * 50)
    print(f"Table           : {result.effective_table}")
    print(f"Mode            : {result.mode}")
    print(f"Snapshot ID     : {result.snapshot_id}")
    print(f"Files copied    : {result.files_copied}")
    print(f"Bytes copied    : {result.bytes_copied:,}")
    print(f"New metadata    : {result.new_metadata_uri}")
    print(f"Success         : {result.success}")
    print("─" * 50)

    if not result.success:
        print("\nErrors:")
        for e in result.errors:
            print(f"  • {e}")

    print("\nRestore complete. "
          "Verify via 03_verify_partitions.py that the partition is visible again.\n")


if __name__ == "__main__":
    main()
