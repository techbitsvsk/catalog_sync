"""
examples/adls_to_nessie_poc.py
──────────────────────────────
End-to-end POC: Incremental sync from Azure ADLS Gen2 → on-prem Nessie + MinIO.

This script demonstrates:
  1. Source data lives in Azure ADLS Gen2 (Azure Fabric Lakehouse / Synapse)
  2. Files are copied incrementally to on-prem MinIO (S3-compatible)
  3. Iceberg metadata is rewritten with MinIO URIs
  4. Table is registered / updated in Nessie REST catalog
  5. Any Iceberg-aware engine on-prem can now query the table

Prerequisites
─────────────
  # Start on-prem stack
  docker compose -f docker/docker-compose.yml up -d

  # Install this package
  pip install -e ".[nessie]"

  # Azure auth (choose one):
  #   az login                    (interactive — for dev)
  #   set AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID  (service principal)

Usage
─────
  python examples/adls_to_nessie_poc.py \\
      --adls-account  mystorageacct \\
      --container     iceberg \\
      --table         gold/top_customers

  # Subsequent run (incremental — only new files copied):
  python examples/adls_to_nessie_poc.py \\
      --adls-account  mystorageacct \\
      --container     iceberg \\
      --table         gold/top_customers

Or via the CLI (equivalent):
  iceberg-sync table \\
      --source-root   "abfss://iceberg@mystorageacct.dfs.core.windows.net/" \\
      --target-root   "s3a://warehouse/" \\
      --table         gold/top_customers \\
      --target-endpoint http://localhost:9000 \\
      --target-access-key minioadmin \\
      --target-secret-key minioadmin \\
      --nessie-uri    http://localhost:19120
"""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync Iceberg table from ADLS → on-prem MinIO + Nessie"
    )
    p.add_argument("--adls-account", required=True,
                   help="Azure storage account name (e.g. mystorageacct)")
    p.add_argument("--container", default="iceberg",
                   help="ADLS container name (default: iceberg)")
    p.add_argument("--adls-key", default=None,
                   help="Storage account key. Leave blank to use DefaultAzureCredential.")
    p.add_argument("--table", required=True,
                   help="Table path: namespace/table_name  (e.g. gold/top_customers)")
    p.add_argument("--nessie-uri", default="http://localhost:19120",
                   help="Nessie base URL")
    p.add_argument("--nessie-ref", default="main",
                   help="Nessie branch (default: main)")
    p.add_argument("--minio-endpoint", default="http://localhost:9000",
                   help="MinIO S3 endpoint")
    p.add_argument("--minio-bucket", default="warehouse",
                   help="MinIO bucket (default: warehouse)")
    p.add_argument("--minio-access-key", default="minioadmin")
    p.add_argument("--minio-secret-key", default="minioadmin")
    p.add_argument("--parallel", type=int, default=8,
                   help="Parallel copy threads (default: 8)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would be synced without copying")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # ── Build storage URIs ────────────────────────────────────────────────────
    source_root = (
        f"abfss://{args.container}@{args.adls_account}.dfs.core.windows.net/"
    )
    target_root = f"s3a://{args.minio_bucket}/"

    # ── Parse namespace / table name ──────────────────────────────────────────
    parts = args.table.strip("/").split("/")
    if len(parts) < 2:
        log.error("--table must be in the form  namespace/table_name")
        return 1
    namespace = ".".join(parts[:-1])   # supports nested: gold.revenue → "gold.revenue"
    table_name = parts[-1]
    table_root = source_root + args.table.strip("/") + "/"

    log.info("=" * 60)
    log.info("Iceberg Catalog Sync — ADLS → On-prem Nessie POC")
    log.info("=" * 60)
    log.info(f"  Source (ADLS):     {table_root}")
    log.info(f"  Target (MinIO):    {target_root}{args.table.strip('/')}/")
    log.info(f"  Nessie:            {args.nessie_uri}  branch={args.nessie_ref}")
    log.info(f"  Namespace/Table:   {namespace}.{table_name}")
    if args.dry_run:
        log.info("  Mode:              DRY RUN")
    log.info("")

    # ── Step 1: Build storage backends ───────────────────────────────────────
    from iceberg_sync.path_translator import PathTranslator
    from iceberg_sync.storage import create_storage
    from iceberg_sync.sync import CatalogSync

    translator = PathTranslator([(source_root, target_root)])

    source_kwargs: dict = {"storage_account_name": args.adls_account}
    if args.adls_key:
        source_kwargs["storage_account_key"] = args.adls_key

    source_storage = create_storage(source_root, **source_kwargs)
    target_storage = create_storage(
        target_root,
        endpoint_url=args.minio_endpoint,
        aws_access_key_id=args.minio_access_key,
        aws_secret_access_key=args.minio_secret_key,
        region_name="us-east-1",
    )

    # ── Step 2: Incremental file sync + metadata rewrite ─────────────────────
    sync = CatalogSync(
        translator=translator,
        source_storage=source_storage,
        target_storage=target_storage,
        max_parallel_copies=args.parallel,
    )

    log.info("Step 1/2 — Syncing files and rewriting metadata…")
    result = sync.sync_table(table_root, dry_run=args.dry_run)

    log.info(f"  Files copied:   {result.files_copied}")
    log.info(f"  Files skipped:  {result.files_skipped}  (already on MinIO)")
    log.info(f"  Bytes copied:   {result.bytes_copied / 1024 / 1024:.2f} MB")
    log.info(f"  Duration:       {result.duration_seconds:.1f}s")
    if result.rewrite_stats:
        rs = result.rewrite_stats
        log.info(f"  Metadata files rewritten:  {rs.metadata_files_rewritten}")
        log.info(f"  Manifests rewritten:       {rs.manifests_rewritten}")
        log.info(f"  Data paths translated:     {rs.data_file_paths_translated}")

    if not result.success:
        log.error(f"  Sync failed: {result.errors}")
        return 1

    if args.dry_run:
        log.info("\nDRY RUN complete — no files or catalog entries were modified.")
        return 0

    # ── Step 3: Register / update in Nessie catalog ───────────────────────────
    from iceberg_sync.catalog.nessie import NessieCatalog

    log.info("")
    log.info("Step 2/2 — Registering in Nessie catalog…")
    log.info(f"  Metadata location: {result.target_metadata_uri}")

    nessie = NessieCatalog(uri=args.nessie_uri, ref=args.nessie_ref)

    if not nessie.ping():
        log.error(f"  Cannot reach Nessie at {args.nessie_uri}")
        log.error("  Is the stack running?  docker compose -f docker/docker-compose.yml up -d")
        return 1

    catalog_result = nessie.register_or_update(
        namespace=namespace,
        table=table_name,
        metadata_location=result.target_metadata_uri,
    )

    if catalog_result.get("skipped"):
        log.info(f"  {namespace}.{table_name} — already up-to-date in Nessie")
    else:
        log.info(f"  {namespace}.{table_name} — registered / updated in Nessie")

    log.info("")
    log.info("=" * 60)
    log.info("SUCCESS — table is now queryable via the Nessie catalog")
    log.info("=" * 60)
    log.info("")
    log.info("Spark connection snippet:")
    log.info(f'  spark.conf.set("spark.sql.catalog.nessie",')
    log.info(f'                 "org.apache.iceberg.spark.SparkCatalog")')
    log.info(f'  spark.conf.set("spark.sql.catalog.nessie.catalog-impl",')
    log.info(f'                 "org.apache.iceberg.nessie.NessieCatalog")')
    log.info(f'  spark.conf.set("spark.sql.catalog.nessie.uri",')
    log.info(f'                 "{args.nessie_uri}/iceberg")')
    log.info(f'  spark.conf.set("spark.sql.catalog.nessie.ref", "{args.nessie_ref}")')
    log.info(f'  spark.conf.set("spark.sql.catalog.nessie.warehouse",')
    log.info(f'                 "s3a://{args.minio_bucket}/")')
    log.info(f'  # MinIO S3 settings')
    log.info(f'  spark.conf.set("spark.hadoop.fs.s3a.endpoint", "{args.minio_endpoint}")')
    log.info(f'  spark.conf.set("spark.hadoop.fs.s3a.access.key", "{args.minio_access_key}")')
    log.info(f'  spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")')
    log.info(f'  spark.sql("SELECT * FROM nessie.{namespace}.{table_name} LIMIT 10").show()')
    log.info("")
    log.info("PyIceberg connection snippet:")
    log.info(f'  from pyiceberg.catalog.rest import RestCatalog')
    log.info(f'  catalog = RestCatalog("nessie", **{{')
    log.info(f'      "uri": "{args.nessie_uri}/iceberg",')
    log.info(f'      "ref": "{args.nessie_ref}",')
    log.info(f'  }})')
    log.info(f'  table = catalog.load_table(("{namespace}", "{table_name}"))')
    log.info(f'  df = table.scan().to_arrow().to_pandas()')

    return 0


if __name__ == "__main__":
    sys.exit(main())
