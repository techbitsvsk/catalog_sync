"""
e2e/scripts/iceberg_table.py
==============================
Unified Iceberg table setup, archive, and restore demo.
Works with both S3/MinIO and ADLS Gen2/Microsoft Fabric — backend is
selected at runtime; all workflow logic is identical.

Replaces:
  02_create_iceberg_table.py  (MinIO variant)
  07_adls_fabric.py           (ADLS/Fabric variant)

Usage
─────
  # MinIO / local Docker stack
  python e2e/scripts/iceberg_table.py --backend s3 create-table
  python e2e/scripts/iceberg_table.py --backend s3 archive --execute
  python e2e/scripts/iceberg_table.py --backend s3 restore --month 2024-03 --confirm
  python e2e/scripts/iceberg_table.py --backend s3 verify

  # ADLS Gen2 / Microsoft Fabric
  export AZURE_STORAGE_ACCOUNT=myfabricaccount
  export AZURE_STORAGE_KEY=<key>
  export FABRIC_WORKSPACE_ID=<ws-guid>
  export FABRIC_LAKEHOUSE_ID=<lh-guid>
  export ARCHIVE_ROOT="abfss://archive@myfabricaccount.dfs.core.windows.net/iceberg-cold"
  python e2e/scripts/iceberg_table.py --backend adls create-table
  python e2e/scripts/iceberg_table.py --backend adls archive --execute
  python e2e/scripts/iceberg_table.py --backend adls restore --month 2024-03 --confirm
  python e2e/scripts/iceberg_table.py --backend adls verify

  # Override any individual credential on the CLI
  python e2e/scripts/iceberg_table.py --backend s3 \\
      --s3-endpoint http://localhost:9000 \\
      --s3-access-key minioadmin \\
      --s3-secret-key minioadmin \\
      --gateway http://localhost:8083 \\
      --oauth-url http://localhost:8081/oauth2/token \\
      --oauth-client-id admin-client \\
      --oauth-secret admin-secret \\
      --warehouse s3a://warehouse/iceberg \\
      --archive-root s3a://warehouse/archive \\
      create-table

Prerequisites
─────────────
  pip install "iceberg-catalog-sync[archive]"
  pip install "pyiceberg[s3,nessie]"          # for S3 backend
  pip install "pyiceberg[adls]" "azure-storage-blob" "azure-identity"  # for ADLS
  pip install duckdb pyarrow

  S3/MinIO:  docker compose -f docker/docker-compose.yml up -d
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# ── Iceberg table schema (shared by both backends) ────────────────────────────

def _build_schema_and_spec():
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import (
        DateType, DecimalType, IntegerType, LongType, NestedField, StringType,
    )
    schema = Schema(
        NestedField(1,  "order_key",      LongType(),        required=True),
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
    spec = PartitionSpec(
        PartitionField(source_id=10, field_id=1000,
                       transform=IdentityTransform(), name="order_month"),
    )
    return schema, spec


# ── Build connection from CLI args ────────────────────────────────────────────

def build_connection(args) -> "IcebergConnection":  # noqa: F821
    """
    Construct an IcebergConnection from --backend + CLI credential flags.
    Falls back to environment variables for any flag not supplied.
    """
    # Add the e2e/scripts/ directory to sys.path so `connection` is importable
    # regardless of the working directory.
    sys.path.insert(0, str(Path(__file__).parent))
    from connection import IcebergConnectionBuilder

    if args.backend == "s3":
        builder = (
            IcebergConnectionBuilder()
            .s3(
                endpoint=   args.s3_endpoint  or os.getenv("MINIO_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL"),
                access_key= args.s3_access_key or os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID"),
                secret_key= args.s3_secret_key or os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"),
                region=     args.s3_region     or os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            )
            .warehouse(args.warehouse or os.getenv("WAREHOUSE_ROOT", "s3a://warehouse/iceberg"))
            .archive(  args.archive_root or os.getenv("ARCHIVE_ROOT", "s3a://warehouse/archive"))
        )

    elif args.backend == "adls":
        account = (args.adls_account or os.getenv("AZURE_STORAGE_ACCOUNT", ""))
        if not account:
            _die("ADLS backend requires --adls-account or AZURE_STORAGE_ACCOUNT env var.")
        builder = (
            IcebergConnectionBuilder()
            .adls(
                account_name=          account,
                account_key=           args.adls_key     or os.getenv("AZURE_STORAGE_KEY"),
                tenant_id=             args.adls_tenant  or os.getenv("AZURE_TENANT_ID"),
                client_id=             args.adls_client_id or os.getenv("AZURE_CLIENT_ID"),
                client_secret=         args.adls_client_secret or os.getenv("AZURE_CLIENT_SECRET"),
                use_default_credential=args.adls_default_credential,
            )
        )
        # Fabric shortcut: build OneLake warehouse root automatically
        ws = args.fabric_workspace or os.getenv("FABRIC_WORKSPACE_ID")
        lh = args.fabric_lakehouse or os.getenv("FABRIC_LAKEHOUSE_ID")
        if ws and lh:
            builder.fabric_lakehouse(workspace_id=ws, lakehouse_id=lh, account_name=account)
        elif args.warehouse:
            builder.warehouse(args.warehouse)
        else:
            _die(
                "ADLS backend: provide --fabric-workspace + --fabric-lakehouse "
                "(or set FABRIC_WORKSPACE_ID + FABRIC_LAKEHOUSE_ID) "
                "or --warehouse."
            )
        archive = args.archive_root or os.getenv("ARCHIVE_ROOT")
        if not archive:
            _die(
                "ADLS backend: provide --archive-root or set ARCHIVE_ROOT env var.\n"
                "  Example: abfss://archive@<account>.dfs.core.windows.net/iceberg-cold"
            )
        builder.archive(archive)

    else:
        _die(f"Unknown backend: {args.backend!r}. Use --backend s3 or --backend adls.")
        return  # unreachable — satisfies type checker

    # Catalog / auth (same for both backends)
    if args.gateway or args.backend == "s3":
        gateway = args.gateway or os.getenv("GATEWAY_URL", "http://localhost:8083")
        builder.catalog_gateway(gateway)

    nessie = args.nessie_uri or os.getenv("NESSIE_URI")
    if nessie:
        builder.nessie(nessie, ref=args.nessie_ref or os.getenv("NESSIE_REF", "main"))

    oauth_url = args.oauth_url or os.getenv("OAUTH_URL")
    if oauth_url:
        builder.oauth(
            oauth_url,
            client_id=     args.oauth_client_id or os.getenv("OAUTH_CLIENT_ID", "admin-client"),
            client_secret= args.oauth_secret     or os.getenv("OAUTH_CLIENT_SECRET", "admin-secret"),
        )

    return builder.build()


# ── Subcommand: create-table ──────────────────────────────────────────────────

def cmd_create_table(conn, args) -> None:
    """Create the tpch.orders Iceberg table and load Parquet partitions."""
    import pyarrow.parquet as pq

    data_dir = Path(args.data_dir)
    print(f"=== create-table [{conn.backend}] ===")
    print(f"  Warehouse  : {conn.warehouse_root}")

    schema, spec = _build_schema_and_spec()
    catalog = conn.get_catalog()

    # Ensure namespace
    ns_list = [ns[0] for ns in catalog.list_namespaces()]
    if "tpch" not in ns_list:
        catalog.create_namespace("tpch")
        print("  Created namespace 'tpch'")

    table_name = "tpch.orders"
    location   = f"{conn.warehouse_root}/tpch/orders"

    if catalog.table_exists(table_name):
        print(f"  Table '{table_name}' already exists — loading.")
        table = catalog.load_table(table_name)
    else:
        table = catalog.create_table(
            table_name,
            schema=schema,
            partition_spec=spec,
            location=location,
            properties={
                "write.format.default":             "parquet",
                "write.parquet.compression-codec":  "snappy",
            },
        )
        print(f"  Created '{table_name}' at {table.location()}")

    # Load partitions
    part_dirs = sorted(data_dir.glob("order_month=*"))
    if not part_dirs:
        print(f"\nNo Parquet files found under {data_dir}.")
        print("Run 01_generate_tpch.py first.\n")
        sys.exit(1)

    print(f"\nLoading {len(part_dirs)} partitions …")
    for part_dir in part_dirs:
        month = part_dir.name.split("=", 1)[1]
        files = list(part_dir.glob("*.parquet"))
        if not files:
            continue
        arrow_tbl = pq.read_table(files[0])
        table.append(arrow_tbl)
        print(f"  order_month={month}  {len(arrow_tbl):>10,} rows")

    print(f"\nAll partitions loaded into '{table_name}'.")
    if conn.backend == "adls":
        print(
            "Fabric SQL Analytics Endpoint will auto-discover the table "
            "(allow 1–2 minutes for OneLake metadata refresh)."
        )


# ── Subcommand: archive ───────────────────────────────────────────────────────

def cmd_archive(conn, args) -> None:
    """Archive old partitions to cold storage."""
    from iceberg_sync.archive.archiver import IcebergArchiver

    dry_run = not args.execute
    print(f"=== archive [{conn.backend}] [{'EXECUTE' if args.execute else 'DRY-RUN'}] ===")
    print(f"  Source  : {conn.warehouse_root}/tpch/orders")
    print(f"  Archive : {conn.archive_root}/tpch/orders")

    cfg = conn.make_archive_config(
        "tpch/orders",
        older_than=          args.older_than,
        min_snapshots_to_keep=args.min_keep,
        delete_after_archive= args.delete_after,
    )
    archiver = IcebergArchiver.from_config(cfg)
    result   = archiver.archive_table("tpch/orders", dry_run=dry_run)

    print()
    print(f"  Snapshots archived : {result.snapshots_archived}")
    print(f"  Files copied       : {result.files_copied}")
    print(f"  Bytes copied       : {result.bytes_copied:,}")
    print(f"  Success            : {result.success}")

    if result.errors:
        print("\nErrors:")
        for e in result.errors:
            print(f"  • {e}")

    if dry_run:
        print("\n[DRY-RUN] Re-run with --execute to apply.\n")


# ── Subcommand: restore ───────────────────────────────────────────────────────

def cmd_restore(conn, args) -> None:
    """Restore an archived partition back to the primary warehouse."""
    from iceberg_sync.archive.restorer import IcebergRestorer

    print(f"=== restore [{conn.backend}] [{'EXECUTE' if args.confirm else 'PLAN'}] ===")
    print(f"  Archive : {conn.archive_root}/tpch/orders")
    print(f"  Target  : {conn.warehouse_root}/tpch/orders")
    print(f"  Month   : {args.month}")

    cfg = conn.make_restore_config(
        "tpch/orders",
        partitions=[{"order_month": args.month}],
        as_of=args.as_of,
        mode=args.mode,
        conflict_strategy=args.conflict,
    )
    restorer = IcebergRestorer.from_config(cfg)

    print("\n--- Available Archive Snapshots ---")
    restorer.list_snapshots()

    print(f"\n--- Restore Plan ---")
    plan = restorer.plan()

    if not args.confirm:
        print("\n[PLAN ONLY] Add --confirm to execute.\n")
        return

    print("\nExecuting …")
    result = restorer.execute(plan)

    print()
    if result.success:
        print(f"  Files restored  : {result.files_copied}")
        print(f"  Bytes restored  : {result.bytes_copied:,}")
        print(f"  New metadata    : {result.new_metadata_uri}")
    else:
        print("Restore failed:")
        for e in result.errors:
            print(f"  • {e}")
        sys.exit(1)


# ── Subcommand: verify ────────────────────────────────────────────────────────

def cmd_verify(conn, args) -> None:
    """Verify the partition is visible after restore."""
    print(f"=== verify [{conn.backend}] ===")
    print(f"  Checking order_month={args.month} in {conn.warehouse_root}/tpch/orders")

    try:
        catalog   = conn.get_catalog()
        table     = catalog.load_table("tpch.orders")
        arrow_tbl = table.scan(
            row_filter=f"order_month = '{args.month}'"
        ).to_arrow()
        count = len(arrow_tbl)
        if count > 0:
            print(f"\n  order_month={args.month}  {count:,} rows ✓")
        else:
            print(f"\n  WARNING: 0 rows found for order_month={args.month}")
    except Exception as exc:
        print(f"\n  Read failed: {exc}")

    if conn.backend == "adls":
        print(
            "\n--- Fabric SQL Analytics Endpoint ---\n"
            f"  SELECT order_month, COUNT(*) AS rows\n"
            f"  FROM   tpch.orders\n"
            f"  WHERE  order_month = '{args.month}'\n"
            f"  GROUP BY order_month;\n"
        )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


# ── CLI definition ────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="iceberg_table.py",
        description="Iceberg table create / archive / restore — S3 and ADLS backends.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Global: backend ───────────────────────────────────────────────────────
    p.add_argument(
        "--backend", choices=["s3", "adls"], default="s3",
        help="Storage backend: s3 (MinIO/AWS) or adls (Azure/Fabric). Default: s3.",
    )

    # ── S3 flags ──────────────────────────────────────────────────────────────
    s3g = p.add_argument_group("S3 / MinIO credentials")
    s3g.add_argument("--s3-endpoint",    metavar="URL",    help="S3 endpoint (omit for AWS)")
    s3g.add_argument("--s3-access-key",  metavar="KEY")
    s3g.add_argument("--s3-secret-key",  metavar="SECRET")
    s3g.add_argument("--s3-region",      metavar="REGION", default="us-east-1")

    # ── ADLS flags ────────────────────────────────────────────────────────────
    ag = p.add_argument_group("ADLS Gen2 / Fabric credentials")
    ag.add_argument("--adls-account",         metavar="NAME",
                    help="Storage account name (or set AZURE_STORAGE_ACCOUNT)")
    ag.add_argument("--adls-key",             metavar="KEY",
                    help="Storage account key (or set AZURE_STORAGE_KEY)")
    ag.add_argument("--adls-tenant",          metavar="GUID",
                    help="Azure tenant ID for service principal auth")
    ag.add_argument("--adls-client-id",       metavar="GUID")
    ag.add_argument("--adls-client-secret",   metavar="SECRET")
    ag.add_argument("--adls-default-credential", action="store_true",
                    help="Use DefaultAzureCredential (managed identity / az login)")
    ag.add_argument("--fabric-workspace",     metavar="GUID",
                    help="Fabric workspace ID (or set FABRIC_WORKSPACE_ID)")
    ag.add_argument("--fabric-lakehouse",     metavar="GUID",
                    help="Fabric Lakehouse ID (or set FABRIC_LAKEHOUSE_ID)")

    # ── Catalog / auth ────────────────────────────────────────────────────────
    cg = p.add_argument_group("Catalog and auth")
    cg.add_argument("--gateway",          metavar="URL",
                    help="Catalog-gateway URL (default: http://localhost:8083)")
    cg.add_argument("--nessie-uri",       metavar="URL")
    cg.add_argument("--nessie-ref",       metavar="BRANCH", default="main")
    cg.add_argument("--oauth-url",        metavar="URL")
    cg.add_argument("--oauth-client-id",  metavar="ID")
    cg.add_argument("--oauth-secret",     metavar="SECRET")

    # ── Storage roots ─────────────────────────────────────────────────────────
    rg = p.add_argument_group("Storage roots")
    rg.add_argument("--warehouse",    metavar="URI",
                    help="Primary Iceberg warehouse root. "
                         "Auto-set by --fabric-workspace/lakehouse for ADLS.")
    rg.add_argument("--archive-root", metavar="URI",
                    help="Cold storage root URI (or set ARCHIVE_ROOT env var)")

    # ── Subcommands ───────────────────────────────────────────────────────────
    sub = p.add_subparsers(dest="cmd", required=True)

    # create-table
    ct = sub.add_parser("create-table", help="Create Iceberg table and load Parquet partitions")
    ct.add_argument("--data-dir", default="e2e/data/orders",
                    help="Directory with partitioned Parquet files (default: e2e/data/orders)")

    # archive
    ar = sub.add_parser("archive", help="Archive old snapshots to cold storage")
    ar.add_argument("--execute",     action="store_true",
                    help="Execute archive (default: dry-run)")
    ar.add_argument("--older-than",  default="150d",   metavar="DURATION",
                    help="Archive snapshots older than this (default: 150d)")
    ar.add_argument("--min-keep",    default=5,         type=int, metavar="N",
                    help="Min snapshots to keep on primary (default: 5)")
    ar.add_argument("--delete-after",action="store_true", default=True,
                    help="Expire archived snapshots from primary (default: true)")

    # restore
    rs = sub.add_parser("restore", help="Restore a partition from cold storage")
    rs.add_argument("--month",    default="2024-03",   metavar="YYYY-MM",
                    help="Partition (order_month) to restore (default: 2024-03)")
    rs.add_argument("--as-of",    default="2024-04-01",metavar="YYYY-MM-DD",
                    help="Restore snapshot as of this date (default: 2024-04-01)")
    rs.add_argument("--mode",     default="replace",
                    choices=["replace", "append", "new_table"])
    rs.add_argument("--conflict", default="skip",
                    choices=["fail", "skip", "overwrite"],
                    dest="conflict")
    rs.add_argument("--confirm",  action="store_true",
                    help="Execute the restore (default: plan only)")

    # verify
    vr = sub.add_parser("verify", help="Verify the restored partition is visible")
    vr.add_argument("--month", default="2024-03", metavar="YYYY-MM")

    return p


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    conn   = build_connection(args)

    print(f"\n{conn}\n")

    dispatch = {
        "create-table": cmd_create_table,
        "archive":      cmd_archive,
        "restore":      cmd_restore,
        "verify":       cmd_verify,
    }
    dispatch[args.cmd](conn, args)


if __name__ == "__main__":
    main()
