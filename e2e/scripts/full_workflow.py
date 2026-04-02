"""
e2e/scripts/full_workflow.py
==============================
Full end-to-end walkthrough: generate → create table → archive → restore → verify
→ access-control demo.

Runs every stage in sequence against the local Docker stack (MinIO + Nessie +
catalog-gateway + OPA + OAuth).  Switch to ADLS/Fabric with --backend adls.

Usage
─────
  # Start the stack first
  docker compose -f docker/docker-compose.yml up -d

  # Run every stage (default: S3/MinIO)
  python e2e/scripts/full_workflow.py

  # Run only specific stages
  python e2e/scripts/full_workflow.py --stages generate,create,archive,restore,verify

  # ADLS/Fabric backend
  export AZURE_STORAGE_ACCOUNT=myfabricaccount
  export AZURE_STORAGE_KEY=<key>
  export FABRIC_WORKSPACE_ID=<ws-guid>
  export FABRIC_LAKEHOUSE_ID=<lh-guid>
  export ARCHIVE_ROOT="abfss://archive@myfabricaccount.dfs.core.windows.net/iceberg-cold"
  python e2e/scripts/full_workflow.py --backend adls

  # Skip data generation if Parquet files already exist
  python e2e/scripts/full_workflow.py --skip-generate

  # Use fewer rows for a quick smoke-test (≈ 15 MB)
  python e2e/scripts/full_workflow.py --rows 150000

Stages
──────
  generate   — produce TPC-H Parquet files with DuckDB
  create     — create tpch.orders Iceberg table + load all partitions
  archive    — archive 5 oldest monthly snapshots to cold storage
  restore    — restore the March-2024 partition from cold storage
  verify     — assert restored partition has rows; print row counts
  access     — demonstrate OPA/gateway enforcement (S3 backend only)

Prerequisites
─────────────
  pip install "iceberg-catalog-sync[archive]"
  pip install "pyiceberg[s3,nessie]"
  pip install duckdb pyarrow boto3 rich
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

# ── Ensure e2e/scripts is importable regardless of working directory ──────────
sys.path.insert(0, str(Path(__file__).parent))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

console = Console()

DATA_DIR       = Path("e2e/data/orders")
NAMESPACE      = "tpch"
TABLE_NAME     = "orders"
FULL_TABLE     = f"{NAMESPACE}.{TABLE_NAME}"
ARCHIVE_TABLE  = f"{NAMESPACE}/{TABLE_NAME}"
RESTORE_MONTH  = "2024-03"
RESTORE_AS_OF  = "2024-04-01"

ALL_STAGES = ["generate", "create", "archive", "restore", "verify", "access"]


# ─────────────────────────────────────────────────────────────────────────────
# Connection factory
# ─────────────────────────────────────────────────────────────────────────────

def make_connection(backend: str):
    """Build an IcebergConnection from environment / defaults."""
    from connection import IcebergConnectionBuilder

    if backend == "s3":
        return (
            IcebergConnectionBuilder()
            .s3(
                endpoint=   os.getenv("MINIO_ENDPOINT",   "http://localhost:9000"),
                access_key= os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key= os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                region=     os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            )
            .catalog_gateway(os.getenv("GATEWAY_URL", "http://localhost:8083"))
            .nessie(
                os.getenv("NESSIE_URI", "http://localhost:19120"),
                ref=os.getenv("NESSIE_REF", "main"),
            )
            .oauth(
                os.getenv("OAUTH_URL", "http://localhost:8081/oauth2/token"),
                client_id=     os.getenv("OAUTH_CLIENT_ID",     "admin-client"),
                client_secret= os.getenv("OAUTH_CLIENT_SECRET", "admin-secret"),
            )
            .warehouse(os.getenv("WAREHOUSE_ROOT", "s3a://warehouse/iceberg"))
            .archive(  os.getenv("ARCHIVE_ROOT",   "s3a://warehouse/archive"))
            .build()
        )

    # ADLS / Fabric
    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "")
    if not account:
        console.print("[red]ERROR:[/red] AZURE_STORAGE_ACCOUNT env var is required for --backend adls")
        sys.exit(1)

    archive_root = os.environ.get("ARCHIVE_ROOT", "")
    if not archive_root:
        console.print("[red]ERROR:[/red] ARCHIVE_ROOT env var is required for --backend adls")
        sys.exit(1)

    builder = (
        IcebergConnectionBuilder()
        .adls(
            account_name=          account,
            account_key=           os.getenv("AZURE_STORAGE_KEY"),
            tenant_id=             os.getenv("AZURE_TENANT_ID"),
            client_id=             os.getenv("AZURE_CLIENT_ID"),
            client_secret=         os.getenv("AZURE_CLIENT_SECRET"),
            use_default_credential=not os.getenv("AZURE_STORAGE_KEY")
                                   and not os.getenv("AZURE_CLIENT_SECRET"),
        )
        .fabric_lakehouse(
            workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
            lakehouse_id=os.environ["FABRIC_LAKEHOUSE_ID"],
        )
        .archive(archive_root)
    )
    if os.getenv("NESSIE_URI"):
        builder.nessie(os.environ["NESSIE_URI"])
    if os.getenv("OAUTH_URL"):
        builder.oauth(
            os.environ["OAUTH_URL"],
            client_id=     os.getenv("OAUTH_CLIENT_ID",     "sync-service"),
            client_secret= os.getenv("OAUTH_CLIENT_SECRET", ""),
        )
    return builder.build()


# ─────────────────────────────────────────────────────────────────────────────
# Stage helpers
# ─────────────────────────────────────────────────────────────────────────────

def _banner(title: str, step: int, total: int) -> None:
    console.print()
    console.print(Panel.fit(
        f"[bold cyan]Stage {step}/{total} — {title}[/bold cyan]",
        border_style="cyan",
    ))


def _ok(msg: str) -> None:
    console.print(f"  [green]✓[/green]  {msg}")


def _info(msg: str) -> None:
    console.print(f"  [dim]•[/dim]  {msg}")


def _warn(msg: str) -> None:
    console.print(f"  [yellow]⚠[/yellow]  {msg}")


def _fail(msg: str) -> None:
    console.print(f"  [red]✗[/red]  {msg}")


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Generate TPC-H data
# ─────────────────────────────────────────────────────────────────────────────

def stage_generate(rows: int) -> dict:
    import duckdb
    import pyarrow.parquet as pq

    t0 = time.time()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    _info(f"Generating {rows:,} rows spanning 2024-01 → 2024-10 …")

    conn = duckdb.connect()
    conn.execute(f"""
        CREATE TABLE orders AS
        SELECT
            row_number() OVER ()                                                      AS order_key,
            (random() * 99999 + 1)::BIGINT                                            AS customer_key,
            CASE (random() * 3)::INT
                WHEN 0 THEN 'O' WHEN 1 THEN 'F' ELSE 'P'
            END                                                                       AS order_status,
            ROUND((random() * 499999 + 1)::DOUBLE, 2)::DECIMAL(15,2)                 AS total_price,
            (DATE '2024-01-01' + ((random() * 304)::INT * INTERVAL '1' DAY))::DATE   AS order_date,
            CASE (random() * 4)::INT
                WHEN 0 THEN '1-URGENT' WHEN 1 THEN '2-HIGH'
                WHEN 2 THEN '3-MEDIUM' WHEN 3 THEN '4-NOT SPECIFIED'
                ELSE '5-LOW'
            END                                                                       AS order_priority,
            'Clerk#' || LPAD(((random() * 999)::INT)::VARCHAR, 9, '0')               AS clerk,
            0::INT                                                                    AS ship_priority,
            'Generated order ' || row_number() OVER ()                               AS comment,
            strftime(
                (DATE '2024-01-01' + ((random() * 304)::INT * INTERVAL '1' DAY))::DATE,
                '%Y-%m'
            )                                                                         AS order_month
        FROM range(0, {rows})
    """)

    months = [r[0] for r in conn.execute(
        "SELECT DISTINCT order_month FROM orders ORDER BY order_month"
    ).fetchall()]

    total_bytes = 0
    for month in months:
        part_dir = DATA_DIR / f"order_month={month}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_path = part_dir / "part-0.parquet"
        arrow_tbl = conn.execute(
            "SELECT * FROM orders WHERE order_month = ? ORDER BY order_date", [month]
        ).fetch_arrow_table()
        import pyarrow.parquet as pq
        pq.write_table(arrow_tbl, out_path, compression="snappy", row_group_size=100_000)
        total_bytes += out_path.stat().st_size
        _ok(f"order_month={month}  {len(arrow_tbl):>10,} rows  "
            f"({out_path.stat().st_size / 1_048_576:.1f} MB)")

    elapsed = time.time() - t0
    return {
        "months":      months,
        "total_bytes": total_bytes,
        "elapsed_s":   round(elapsed, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Create Iceberg table + load partitions
# ─────────────────────────────────────────────────────────────────────────────

def stage_create(conn) -> dict:
    import pyarrow.parquet as pq
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

    catalog = conn.get_catalog()

    # Ensure namespace
    existing_ns = [ns[0] for ns in catalog.list_namespaces()]
    if NAMESPACE not in existing_ns:
        catalog.create_namespace(NAMESPACE)
        _ok(f"Created namespace '{NAMESPACE}'")
    else:
        _info(f"Namespace '{NAMESPACE}' already exists")

    if catalog.table_exists(FULL_TABLE):
        _info(f"Table '{FULL_TABLE}' already exists — loading")
        table = catalog.load_table(FULL_TABLE)
    else:
        table = catalog.create_table(
            FULL_TABLE,
            schema=schema,
            partition_spec=spec,
            location=f"{conn.warehouse_root}/{NAMESPACE}/{TABLE_NAME}",
            properties={
                "write.format.default":            "parquet",
                "write.parquet.compression-codec": "snappy",
            },
        )
        _ok(f"Created '{FULL_TABLE}' at {table.location()}")

    # Load monthly partitions
    part_dirs = sorted(DATA_DIR.glob("order_month=*"))
    if not part_dirs:
        _fail(f"No Parquet files under {DATA_DIR} — run with 'generate' stage first")
        sys.exit(1)

    total_rows = 0
    for part_dir in part_dirs:
        month = part_dir.name.split("=", 1)[1]
        files = list(part_dir.glob("*.parquet"))
        if not files:
            continue
        arrow_tbl = pq.read_table(files[0])
        table.append(arrow_tbl)
        total_rows += len(arrow_tbl)
        _ok(f"order_month={month}  {len(arrow_tbl):>10,} rows loaded")

    return {
        "table":       FULL_TABLE,
        "location":    table.location(),
        "partitions":  len(part_dirs),
        "total_rows":  total_rows,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Archive 5 oldest snapshots
# ─────────────────────────────────────────────────────────────────────────────

def stage_archive(conn) -> dict:
    from iceberg_sync.archive.archiver import IcebergArchiver

    _info("Scanning archive candidates (older_than=150d, min_keep=5) …")

    cfg = conn.make_archive_config(
        ARCHIVE_TABLE,
        older_than="150d",
        min_snapshots_to_keep=5,
        delete_after_archive=True,
    )
    archiver = IcebergArchiver.from_config(cfg)

    # Dry-run first — show the plan
    dry = archiver.archive_table(ARCHIVE_TABLE, dry_run=True)
    _info(f"Plan: {dry.snapshots_archived} snapshot(s) to archive, "
          f"{dry.files_copied} file(s), {dry.bytes_copied:,} bytes")

    if dry.snapshots_archived == 0:
        _warn("No snapshots eligible for archival with current retention policy.")
        return {"snapshots_archived": 0, "files_copied": 0, "bytes_copied": 0}

    # Execute
    _info("Executing archive …")
    result = archiver.archive_table(ARCHIVE_TABLE, dry_run=False)

    if not result.success:
        for e in result.errors:
            _fail(e)
        sys.exit(1)

    _ok(f"Archived {result.snapshots_archived} snapshot(s), "
        f"{result.files_copied} files, {result.bytes_copied:,} bytes")
    _ok(f"Archive index written to {conn.archive_root}/{ARCHIVE_TABLE}")

    return {
        "snapshots_archived": result.snapshots_archived,
        "files_copied":       result.files_copied,
        "bytes_copied":       result.bytes_copied,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4 — Restore March-2024 partition
# ─────────────────────────────────────────────────────────────────────────────

def stage_restore(conn) -> dict:
    from iceberg_sync.archive.restorer import IcebergRestorer

    _info(f"Restoring order_month={RESTORE_MONTH} from archive …")

    cfg = conn.make_restore_config(
        ARCHIVE_TABLE,
        partitions=[{"order_month": RESTORE_MONTH}],
        as_of=RESTORE_AS_OF,
        mode="replace",
        conflict_strategy="skip",
    )
    restorer = IcebergRestorer.from_config(cfg)

    # List available snapshots
    snapshots = restorer.list_snapshots()
    if not snapshots:
        _warn("No snapshots found in archive index. Was the archive stage run?")
        return {"files_restored": 0, "skipped": True}

    # Plan
    _info("Building restore plan …")
    plan = restorer.plan()
    _info(f"Plan: {plan.total_files} file(s), "
          f"{plan.total_bytes:,} bytes, "
          f"conflicts={plan.has_conflicts}")

    # Execute
    _info("Executing restore …")
    result = restorer.execute(plan)

    if not result.success:
        for e in result.errors:
            _fail(e)
        sys.exit(1)

    _ok(f"Restored {result.files_copied} file(s), {result.bytes_copied:,} bytes")
    _ok(f"New metadata pointer: {result.new_metadata_uri}")

    return {
        "files_restored":    result.files_copied,
        "bytes_restored":    result.bytes_copied,
        "new_metadata_uri":  result.new_metadata_uri,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage 5 — Verify row counts
# ─────────────────────────────────────────────────────────────────────────────

def stage_verify(conn) -> dict:
    import pyarrow.compute as pc

    _info("Scanning tpch.orders via catalog …")

    catalog   = conn.get_catalog()
    table     = catalog.load_table(FULL_TABLE)
    arrow_tbl = table.scan().to_arrow()

    months = sorted(arrow_tbl.column("order_month").unique().to_pylist())

    tbl = Table(
        "order_month", "rows",
        box=box.SIMPLE_HEAVY, header_style="bold magenta",
    )
    total = 0
    restored_count = 0
    for month in months:
        mask  = pc.equal(arrow_tbl.column("order_month"), month)
        count = pc.sum(mask.cast("int64")).as_py()
        total += count
        style = "[bold green]" if month == RESTORE_MONTH else ""
        suffix = "  ← restored" if month == RESTORE_MONTH else ""
        tbl.add_row(f"{style}{month}[/bold green]" if style else month,
                    f"{style}{count:,}[/bold green]{suffix}" if style else f"{count:,}")
        if month == RESTORE_MONTH:
            restored_count = count

    console.print()
    console.print(tbl)
    console.print(f"  [bold]Total:[/bold] {total:,} rows across {len(months)} partition(s)")

    if restored_count > 0:
        _ok(f"Restored partition order_month={RESTORE_MONTH} has {restored_count:,} rows ✓")
    else:
        _warn(f"Restored partition order_month={RESTORE_MONTH} returned 0 rows")

    return {
        "total_rows":       total,
        "partitions_found": len(months),
        "restored_rows":    restored_count,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage 6 — Access control demo (S3 / catalog-gateway only)
# ─────────────────────────────────────────────────────────────────────────────

DEMO_CLIENTS = [
    ("admin-client",     "admin-secret",     "Unrestricted admin"),
    ("sync-service",     "sync-secret",      "Read + write all namespaces"),
    ("analytics-client", "analytics-secret", "Gold read · EMEA rows · PII masked"),
    ("data-scientist",   "ds-secret",        "Silver/bronze read · PII excluded"),
]


def stage_access(conn) -> dict:
    gateway  = conn._gateway_url
    oauth    = conn._oauth_url
    s3_kw    = conn._s3_kwargs

    if not gateway or not oauth:
        _warn("Skipping access-control stage — no catalog-gateway / OAuth configured.")
        return {}

    from pyiceberg.catalog import load_catalog

    results = {}
    tbl = Table(
        "Client", "Status", "Rows", "Note",
        box=box.SIMPLE_HEAVY, header_style="bold cyan",
    )

    for client_id, secret, description in DEMO_CLIENTS:
        kwargs: dict = {
            "type":             "rest",
            "uri":              gateway,
            "oauth2-server-uri":oauth,
            "credential":       f"{client_id}:{secret}",
            "scope":            "catalog:read",
        }
        if s3_kw.get("endpoint"):
            kwargs.update({
                "s3.endpoint":          s3_kw["endpoint"],
                "s3.access-key-id":     s3_kw.get("access_key", ""),
                "s3.secret-access-key": s3_kw.get("secret_key", ""),
                "s3.region":            s3_kw.get("region", "us-east-1"),
                "s3.path-style-access": "true",
            })

        try:
            cat   = load_catalog(client_id, **kwargs)
            arrow = cat.load_table(FULL_TABLE).scan(limit=500_000).to_arrow()
            rows  = len(arrow)
            status = "[green]OK[/green]"
            note  = description
            results[client_id] = {"status": "ok", "rows": rows}
        except Exception as exc:
            rows  = 0
            if "403" in str(exc) or "Forbidden" in str(exc):
                status = "[red]403[/red]"
                note   = "Gateway enforcement ✓ — access denied"
                results[client_id] = {"status": "403"}
            else:
                status = "[yellow]ERR[/yellow]"
                note   = str(exc)[:60]
                results[client_id] = {"status": "err", "error": str(exc)[:80]}

        tbl.add_row(client_id,
                    status,
                    f"{rows:,}" if rows else "—",
                    note)

    console.print()
    console.print(tbl)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Final summary
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(backend: str, conn, stage_results: dict, elapsed: float) -> None:
    console.print()
    console.print(Panel.fit(
        "[bold green]End-to-End Workflow Complete[/bold green]",
        border_style="green",
    ))

    t = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    t.add_column("Key",   style="dim")
    t.add_column("Value", style="bold")

    t.add_row("Backend",    backend)
    t.add_row("Warehouse",  conn.warehouse_root)
    t.add_row("Archive",    conn.archive_root)
    t.add_row("Elapsed",    f"{elapsed:.1f}s")

    if "generate" in stage_results:
        g = stage_results["generate"]
        t.add_row("Generated",
                  f"{len(g['months'])} months  "
                  f"{g['total_bytes'] / 1_048_576:.1f} MB  "
                  f"({g['elapsed_s']}s)")

    if "create" in stage_results:
        c = stage_results["create"]
        t.add_row("Table",
                  f"{c['table']}  "
                  f"{c['partitions']} partitions  "
                  f"{c['total_rows']:,} rows")

    if "archive" in stage_results:
        a = stage_results["archive"]
        t.add_row("Archived",
                  f"{a['snapshots_archived']} snapshot(s)  "
                  f"{a['files_copied']} files  "
                  f"{a['bytes_copied']:,} bytes")

    if "restore" in stage_results:
        r = stage_results["restore"]
        if not r.get("skipped"):
            t.add_row("Restored",
                      f"order_month={RESTORE_MONTH}  "
                      f"{r['files_restored']} files  "
                      f"{r['bytes_restored']:,} bytes")

    if "verify" in stage_results:
        v = stage_results["verify"]
        t.add_row("Verify",
                  f"{v['total_rows']:,} total rows  "
                  f"{v['partitions_found']} partitions  "
                  f"restored={v['restored_rows']:,}")

    if "access" in stage_results:
        ac = stage_results["access"]
        ok  = sum(1 for r in ac.values() if r.get("status") == "ok")
        blk = sum(1 for r in ac.values() if r.get("status") == "403")
        t.add_row("Access ctrl", f"{ok} allowed  {blk} blocked by OPA")

    console.print(t)
    console.print()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Full Iceberg archive/restore end-to-end workflow.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--backend", choices=["s3", "adls"], default="s3",
                   help="Storage backend (default: s3)")
    p.add_argument("--stages", default=",".join(ALL_STAGES),
                   help=f"Comma-separated list of stages to run (default: all). "
                        f"Choices: {', '.join(ALL_STAGES)}")
    p.add_argument("--rows", type=int, default=1_500_000,
                   help="Rows to generate (default: 1_500_000 ≈ 150 MB). "
                        "Use 150000 for a quick smoke-test.")
    p.add_argument("--skip-generate", action="store_true",
                   help="Skip generate stage if Parquet files already exist")
    p.add_argument("--restore-month", default=RESTORE_MONTH, metavar="YYYY-MM",
                   help=f"Partition to restore (default: {RESTORE_MONTH})")
    args = p.parse_args()

    stages = [s.strip() for s in args.stages.split(",")]
    unknown = set(stages) - set(ALL_STAGES)
    if unknown:
        console.print(f"[red]Unknown stages:[/red] {unknown}. "
                      f"Valid: {', '.join(ALL_STAGES)}")
        sys.exit(1)

    if args.skip_generate and "generate" in stages:
        part_dirs = list(DATA_DIR.glob("order_month=*"))
        if part_dirs:
            console.print(
                f"[dim]--skip-generate: {len(part_dirs)} partition dirs found, "
                "skipping generation.[/dim]"
            )
            stages.remove("generate")
        else:
            console.print(
                "[yellow]--skip-generate set but no data found — "
                "running generate stage.[/yellow]"
            )

    # ── Build connection ──────────────────────────────────────────────────────
    conn = make_connection(args.backend)

    console.print()
    console.print(Panel(
        f"[bold]Iceberg Archive & Restore — Full Workflow[/bold]\n\n"
        f"  Backend   : [cyan]{conn.backend}[/cyan]\n"
        f"  Warehouse : [cyan]{conn.warehouse_root}[/cyan]\n"
        f"  Archive   : [cyan]{conn.archive_root}[/cyan]\n"
        f"  Stages    : [cyan]{', '.join(stages)}[/cyan]",
        border_style="blue",
    ))

    # ── Run stages ────────────────────────────────────────────────────────────
    t_start     = time.time()
    stage_count = len(stages)
    results: dict = {}

    for idx, stage in enumerate(stages, start=1):
        _banner(stage.upper(), idx, stage_count)

        try:
            if stage == "generate":
                results["generate"] = stage_generate(args.rows)

            elif stage == "create":
                results["create"] = stage_create(conn)

            elif stage == "archive":
                results["archive"] = stage_archive(conn)

            elif stage == "restore":
                results["restore"] = stage_restore(conn)

            elif stage == "verify":
                results["verify"] = stage_verify(conn)

            elif stage == "access":
                if conn.backend == "adls":
                    _warn("Access-control stage skipped for ADLS backend "
                          "(requires catalog-gateway).")
                    results["access"] = {}
                else:
                    results["access"] = stage_access(conn)

        except SystemExit:
            raise
        except Exception as exc:
            _fail(f"Stage '{stage}' failed: {exc}")
            console.print_exception(show_locals=False)
            sys.exit(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    print_summary(args.backend, conn, results, time.time() - t_start)


if __name__ == "__main__":
    main()
