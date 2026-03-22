"""
cli.py — Command-line interface for Iceberg catalog sync.

Usage examples:

    # Sync a single table from AWS to Azure
    iceberg-sync table \\
        --source-root "s3://warehouse/iceberg/" \\
        --target-root "abfss://iceberg@account.dfs.core.windows.net/iceberg/" \\
        --table "gold/top_customers" \\
        --source-region eu-west-2 \\
        --target-account-name mystorageaccount

    # Sync an entire namespace
    iceberg-sync namespace \\
        --source-root "s3://warehouse/iceberg/" \\
        --target-root "abfss://iceberg@account.dfs.core.windows.net/iceberg/" \\
        --namespace "gold" \\
        --source-region eu-west-2 \\
        --target-account-name mystorageaccount

    # Dry run (show what would be synced)
    iceberg-sync table --dry-run ...

    # Sync from S3 to MinIO (for testing)
    iceberg-sync table \\
        --source-root "s3://warehouse/iceberg/" \\
        --target-root "s3a://local-warehouse/iceberg/" \\
        --table "gold/top_customers" \\
        --source-region eu-west-2 \\
        --target-endpoint "http://localhost:9000" \\
        --target-access-key minioadmin \\
        --target-secret-key minioadmin
"""

from __future__ import annotations

import logging
import sys

import click
from rich.console import Console
from rich.table import Table as RichTable

from iceberg_sync.path_translator import PathTranslator
from iceberg_sync.storage import create_storage
from iceberg_sync.sync import CatalogSync

console = Console()


def _setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Debug logging")
def main(verbose: bool):
    """Iceberg Catalog Sync — replicate Iceberg tables across clouds."""
    _setup_logging(verbose)


# ── Common options ───────────────────────────────────────────────────────────

def _common_options(fn):
    """Shared CLI options for source/target storage config."""
    fn = click.option("--source-root", required=True, help="Source warehouse root URI")(fn)
    fn = click.option("--target-root", required=True, help="Target warehouse root URI")(fn)
    fn = click.option("--dry-run", is_flag=True, help="Show plan without executing")(fn)
    fn = click.option("--parallel", default=4, help="Parallel copy threads")(fn)
    fn = click.option("--all-snapshots", is_flag=True, help="Rewrite all snapshots (not just current)")(fn)
    # Source S3 options
    fn = click.option("--source-region", default="eu-west-2")(fn)
    fn = click.option("--source-endpoint", default=None, help="S3-compatible endpoint for source")(fn)
    fn = click.option("--source-access-key", default=None)(fn)
    fn = click.option("--source-secret-key", default=None)(fn)
    # Target options
    fn = click.option("--target-account-name", default=None, help="Azure storage account")(fn)
    fn = click.option("--target-account-key", default=None, help="Azure storage account key")(fn)
    fn = click.option("--target-endpoint", default=None, help="S3-compatible endpoint for target")(fn)
    fn = click.option("--target-access-key", default=None)(fn)
    fn = click.option("--target-secret-key", default=None)(fn)
    fn = click.option("--target-region", default="eu-west-2")(fn)
    fn = click.option("--target-gcs-project", default=None)(fn)
    return fn


def _build_sync(
    source_root, target_root, parallel, all_snapshots,
    source_region, source_endpoint, source_access_key, source_secret_key,
    target_account_name, target_account_key, target_endpoint,
    target_access_key, target_secret_key, target_region, target_gcs_project,
    **kwargs,
) -> CatalogSync:
    """Build CatalogSync from CLI options."""
    translator = PathTranslator([(source_root, target_root)])

    source_kwargs = {"region_name": source_region}
    if source_endpoint:
        source_kwargs["endpoint_url"] = source_endpoint
    if source_access_key:
        source_kwargs["aws_access_key_id"] = source_access_key
        source_kwargs["aws_secret_access_key"] = source_secret_key

    target_kwargs = {}
    target_scheme = target_root.split("://")[0]
    if target_scheme in ("s3", "s3a"):
        target_kwargs["region_name"] = target_region
        if target_endpoint:
            target_kwargs["endpoint_url"] = target_endpoint
        if target_access_key:
            target_kwargs["aws_access_key_id"] = target_access_key
            target_kwargs["aws_secret_access_key"] = target_secret_key
    elif target_scheme in ("abfss", "abfs"):
        if target_account_name:
            target_kwargs["storage_account_name"] = target_account_name
        if target_account_key:
            target_kwargs["storage_account_key"] = target_account_key
    elif target_scheme == "gs":
        if target_gcs_project:
            target_kwargs["project"] = target_gcs_project

    return CatalogSync(
        translator=translator,
        source_storage=create_storage(source_root, **source_kwargs),
        target_storage=create_storage(target_root, **target_kwargs),
        max_parallel_copies=parallel,
        rewrite_all_snapshots=all_snapshots,
    )


# ── Commands ─────────────────────────────────────────────────────────────────

@main.command()
@click.option("--table", required=True, help="Table path relative to warehouse root (e.g. gold/top_customers)")
@_common_options
def table(table, source_root, target_root, dry_run, **kwargs):
    """Sync a single Iceberg table."""
    sync = _build_sync(source_root=source_root, target_root=target_root, **kwargs)
    table_root = source_root.rstrip("/") + "/" + table.strip("/") + "/"

    console.print(f"\n[bold]Syncing table:[/bold] {table_root}")
    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be made[/yellow]\n")

    result = sync.sync_table(table_root, dry_run=dry_run)
    _print_result(result)

    sys.exit(0 if result.success else 1)


@main.command()
@click.option("--namespace", required=True, help="Namespace path relative to warehouse root (e.g. gold)")
@_common_options
def namespace(namespace, source_root, target_root, dry_run, **kwargs):
    """Sync all tables in an Iceberg namespace."""
    sync = _build_sync(source_root=source_root, target_root=target_root, **kwargs)
    ns_root = source_root.rstrip("/") + "/" + namespace.strip("/") + "/"

    console.print(f"\n[bold]Syncing namespace:[/bold] {ns_root}")
    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be made[/yellow]\n")

    results = sync.sync_namespace(ns_root, dry_run=dry_run)
    for r in results:
        _print_result(r)

    failed = sum(1 for r in results if not r.success)
    console.print(f"\n[bold]Summary:[/bold] {len(results)} tables, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


def _print_result(result):
    t = RichTable(show_header=False, box=None, padding=(0, 2))
    t.add_column(style="bold")
    t.add_column()

    status = "[green]✓ SUCCESS[/green]" if result.success else "[red]✗ FAILED[/red]"
    t.add_row("Status", status)
    t.add_row("Table", result.table)
    t.add_row("Files copied", str(result.files_copied))
    t.add_row("Files skipped", str(result.files_skipped))
    t.add_row("Bytes copied", f"{result.bytes_copied / 1024 / 1024:.1f} MB")
    t.add_row("Duration", f"{result.duration_seconds:.1f}s")

    if result.rewrite_stats:
        rs = result.rewrite_stats
        t.add_row("Metadata rewritten", str(rs.metadata_files_rewritten))
        t.add_row("Manifests rewritten", str(rs.manifests_rewritten))
        t.add_row("Paths translated", str(rs.data_file_paths_translated))

    if result.errors:
        t.add_row("Errors", "\n".join(result.errors))

    console.print(t)
    console.print()


if __name__ == "__main__":
    main()
