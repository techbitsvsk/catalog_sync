"""
archive_cli.py — iceberg-archive CLI entry point.

Commands
────────
  iceberg-archive archive   Archive snapshots from primary → cold storage.
  iceberg-archive restore   Restore partitions or full table from archive.
                            Dry-run is the default; pass --confirm to execute.
  iceberg-archive snapshots List available snapshots in the archive index.
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

import click
from rich.console import Console

console = Console()


# ──────────────────────────────────────────────────────────────────────────────
# Root group
# ──────────────────────────────────────────────────────────────────────────────

@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def main(verbose: bool) -> None:
    """iceberg-archive — Iceberg table archival and partition restore."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )


# ──────────────────────────────────────────────────────────────────────────────
# archive command
# ──────────────────────────────────────────────────────────────────────────────

@main.command("archive")
@click.option("--config", "config_file", type=click.Path(exists=True),
              help="Path to archive-job.yaml. Overrides all other flags when supplied.")
# Source
@click.option("--source-root", help="Primary storage root URI (s3://, abfss://, gs://).")
@click.option("--table", help="Table path relative to source-root, e.g. gold/orders.")
@click.option("--namespace", help="Namespace — archives all tables within it.")
# Archive destination
@click.option("--archive-root", help="Cold storage root URI.")
# Policy
@click.option("--older-than", default="30d", show_default=True,
              help="Archive snapshots older than this duration (e.g. 7d, 24h).")
@click.option("--min-snapshots-to-keep", default=1, show_default=True, type=int,
              help="Minimum number of snapshots to keep on primary.")
@click.option("--delete-after-archive/--no-delete-after-archive", default=True,
              show_default=True,
              help="Expire archived snapshots from primary after copying.")
# S3 credentials (source)
@click.option("--source-region")
@click.option("--source-endpoint")
@click.option("--source-access-key")
@click.option("--source-secret-key")
# S3 credentials (archive)
@click.option("--archive-region")
@click.option("--archive-endpoint")
@click.option("--archive-access-key")
@click.option("--archive-secret-key")
# Nessie / OAuth
@click.option("--nessie-uri")
@click.option("--nessie-ref", default="main")
@click.option("--oauth-url")
@click.option("--oauth-client-id")
@click.option("--oauth-client-secret")
# Execution
@click.option("--dry-run/--no-dry-run", default=True, show_default=True,
              help="Show what would be archived without writing anything.")
@click.option("--parallel", default=8, show_default=True, type=int)
def archive_cmd(
    config_file, source_root, table, namespace,
    archive_root, older_than, min_snapshots_to_keep,
    delete_after_archive, source_region, source_endpoint,
    source_access_key, source_secret_key,
    archive_region, archive_endpoint, archive_access_key, archive_secret_key,
    nessie_uri, nessie_ref, oauth_url, oauth_client_id, oauth_client_secret,
    dry_run, parallel,
) -> None:
    """Archive Iceberg snapshots from primary storage to cold storage."""
    from iceberg_sync.archive.config import (
        ArchiveJobConfig, S3Config, ADLSConfig, CatalogConfig, TransferConfig,
    )
    from iceberg_sync.archive.archiver import IcebergArchiver

    if config_file:
        cfg = ArchiveJobConfig.from_yaml(config_file)
    else:
        if not source_root:
            raise click.UsageError("--source-root is required when --config is not supplied.")
        if not archive_root:
            raise click.UsageError("--archive-root is required when --config is not supplied.")
        if not table and not namespace:
            raise click.UsageError("Provide --table or --namespace.")

        cfg = ArchiveJobConfig(
            source_root=source_root,
            table=table,
            namespace=namespace,
            archive_root=archive_root,
            older_than=older_than,
            min_snapshots_to_keep=min_snapshots_to_keep,
            delete_after_archive=delete_after_archive,
            dry_run=dry_run,
            source_s3=S3Config(
                region=source_region, endpoint=source_endpoint,
                access_key=source_access_key, secret_key=source_secret_key,
            ),
            archive_s3=S3Config(
                region=archive_region, endpoint=archive_endpoint,
                access_key=archive_access_key, secret_key=archive_secret_key,
            ),
            catalog=CatalogConfig(
                nessie_uri=nessie_uri, nessie_ref=nessie_ref,
                oauth_url=oauth_url, oauth_client_id=oauth_client_id,
                oauth_client_secret=oauth_client_secret,
            ),
            transfer=TransferConfig(parallelism=parallel),
        )
        cfg.dry_run = dry_run

    archiver = IcebergArchiver.from_config(cfg)

    if dry_run:
        console.print("[yellow]DRY RUN — no data will be written.[/yellow]\n")

    errors = []

    if cfg.namespace:
        results = archiver.archive_namespace(cfg.namespace, dry_run=cfg.dry_run)
    elif cfg.table:
        results = [archiver.archive_table(cfg.table, dry_run=cfg.dry_run)]
    else:
        console.print("[red]No table or namespace specified.[/red]")
        sys.exit(1)

    for r in results:
        status = "[green]OK[/green]" if r.success else "[red]FAILED[/red]"
        console.print(
            f"  {status}  {r.table}  "
            f"snapshots={r.snapshots_archived}  "
            f"files={r.files_copied}  "
            f"bytes={r.bytes_copied:,}"
        )
        errors.extend(r.errors)

    if errors:
        console.print(f"\n[red]{len(errors)} error(s):[/red]")
        for e in errors:
            console.print(f"  • {e}")
        sys.exit(1)

    if dry_run:
        console.print(
            "\n[dim]Re-run with --no-dry-run to execute.[/dim]"
        )


# ──────────────────────────────────────────────────────────────────────────────
# snapshots command
# ──────────────────────────────────────────────────────────────────────────────

@main.command("snapshots")
@click.option("--config", "config_file", type=click.Path(exists=True))
@click.option("--archive-root")
@click.option("--table", required=False)
@click.option("--archive-region")
@click.option("--archive-endpoint")
@click.option("--archive-access-key")
@click.option("--archive-secret-key")
def snapshots_cmd(
    config_file, archive_root, table,
    archive_region, archive_endpoint, archive_access_key, archive_secret_key,
) -> None:
    """List available snapshots in the archive index for a table."""
    from iceberg_sync.archive.config import RestoreJobConfig, S3Config
    from iceberg_sync.archive.restorer import IcebergRestorer

    if config_file:
        cfg = RestoreJobConfig.from_yaml(config_file)
    else:
        if not archive_root or not table:
            raise click.UsageError("--archive-root and --table are required.")
        # Minimal config just for listing
        cfg = RestoreJobConfig(
            archive_root=archive_root,
            table=table,
            target_root=archive_root,  # unused for listing
            archive_s3=S3Config(
                region=archive_region, endpoint=archive_endpoint,
                access_key=archive_access_key, secret_key=archive_secret_key,
            ),
        )

    restorer = IcebergRestorer.from_config(cfg)
    restorer.list_snapshots()


# ──────────────────────────────────────────────────────────────────────────────
# restore command
# ──────────────────────────────────────────────────────────────────────────────

@main.command("restore")
@click.option("--config", "config_file", type=click.Path(exists=True),
              help="Path to restore-job.yaml.")
# Archive source
@click.option("--archive-root", help="Cold storage root URI.")
@click.option("--table", help="Table path in archive, e.g. gold/orders.")
# Target
@click.option("--target-root", help="Primary storage root URI for restore destination.")
@click.option("--restore-as", help="Restore under a different table name (side-by-side).")
# What to restore
@click.option("--snapshot-id", type=int, help="Explicit snapshot ID to restore from.")
@click.option("--as-of", help="Restore the latest snapshot as of this date (YYYY-MM-DD).")
@click.option("--partition", "partitions", multiple=True,
              help=(
                  "Partition to restore, as key=value pairs joined by /. "
                  "Repeat for multiple partitions. "
                  "Omit to restore the full table. "
                  "Example: --partition year=2025/month=11"
              ))
# Behaviour
@click.option("--mode", default="replace", show_default=True,
              type=click.Choice(["replace", "append", "new_table"]),
              help="How to merge restored data into the live table.")
@click.option("--conflict-strategy", default="fail", show_default=True,
              type=click.Choice(["fail", "skip", "overwrite"]),
              help="What to do when a restored file already exists in the target.")
# Storage credentials (archive)
@click.option("--archive-region")
@click.option("--archive-endpoint")
@click.option("--archive-access-key")
@click.option("--archive-secret-key")
# Storage credentials (target)
@click.option("--target-region")
@click.option("--target-endpoint")
@click.option("--target-access-key")
@click.option("--target-secret-key")
# Nessie / OAuth
@click.option("--nessie-uri")
@click.option("--nessie-ref", default="main")
@click.option("--oauth-url")
@click.option("--oauth-client-id")
@click.option("--oauth-client-secret")
# Execution
@click.option("--confirm", is_flag=True, default=False,
              help="Execute the restore. Without this flag only a dry-run plan is shown.")
@click.option("--parallel", default=8, show_default=True, type=int)
def restore_cmd(
    config_file, archive_root, table, target_root, restore_as,
    snapshot_id, as_of, partitions, mode, conflict_strategy,
    archive_region, archive_endpoint, archive_access_key, archive_secret_key,
    target_region, target_endpoint, target_access_key, target_secret_key,
    nessie_uri, nessie_ref, oauth_url, oauth_client_id, oauth_client_secret,
    confirm, parallel,
) -> None:
    """
    Restore a full table or specific partitions from archive storage.

    By default this command prints a dry-run plan and exits.
    Add --confirm to execute the restore.

    \b
    Examples:
      # Plan only (default safe mode)
      iceberg-archive restore --archive-root s3://cold/iceberg/ \\
        --target-root s3://warehouse/iceberg/ --table gold/orders \\
        --partition year=2025/month=11 --as-of 2025-12-01

      # Execute after reviewing plan
      iceberg-archive restore --config restore.yaml --confirm
    """
    from iceberg_sync.archive.config import (
        RestoreJobConfig, S3Config, CatalogConfig, TransferConfig,
    )
    from iceberg_sync.archive.restorer import IcebergRestorer

    # Parse --partition year=2025/month=11 into [{"year": 2025, "month": 11}]
    parsed_partitions = []
    for p in partitions:
        spec: dict = {}
        for pair in p.split("/"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                try:
                    spec[k] = int(v)
                except ValueError:
                    spec[k] = v
        if spec:
            parsed_partitions.append(spec)

    if config_file:
        cfg = RestoreJobConfig.from_yaml(config_file)
        # CLI flags override config file when supplied
        if partitions:
            cfg.partitions = parsed_partitions
        if snapshot_id:
            cfg.snapshot_id = snapshot_id
        if as_of:
            cfg.as_of = as_of
        if mode != "replace":
            cfg.mode = mode
        if conflict_strategy != "fail":
            cfg.conflict_strategy = conflict_strategy
        cfg.dry_run = not confirm
    else:
        if not archive_root or not table or not target_root:
            raise click.UsageError(
                "--archive-root, --table, and --target-root are required "
                "when --config is not supplied."
            )
        cfg = RestoreJobConfig(
            archive_root=archive_root,
            table=table,
            target_root=target_root,
            restore_as=restore_as,
            as_of=as_of,
            snapshot_id=snapshot_id,
            partitions=parsed_partitions,
            mode=mode,
            conflict_strategy=conflict_strategy,
            dry_run=not confirm,
            archive_s3=S3Config(
                region=archive_region, endpoint=archive_endpoint,
                access_key=archive_access_key, secret_key=archive_secret_key,
            ),
            target_s3=S3Config(
                region=target_region, endpoint=target_endpoint,
                access_key=target_access_key, secret_key=target_secret_key,
            ),
            catalog=CatalogConfig(
                nessie_uri=nessie_uri, nessie_ref=nessie_ref,
                oauth_url=oauth_url, oauth_client_id=oauth_client_id,
                oauth_client_secret=oauth_client_secret,
            ),
            transfer=TransferConfig(parallelism=parallel),
        )

    restorer = IcebergRestorer.from_config(cfg)

    # Always build and print the plan
    try:
        plan = restorer.plan()
    except ValueError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        sys.exit(1)

    if not confirm:
        console.print(
            "\n[yellow]Plan only — no data written.[/yellow]  "
            "Add [bold]--confirm[/bold] to execute.\n"
        )
        return

    # Confirm before executing
    if plan.has_conflicts and cfg.conflict_strategy == "fail":
        console.print(
            "[red]Blocked by conflicts.[/red]  "
            "Use --conflict-strategy=overwrite to proceed."
        )
        sys.exit(1)

    console.print("\n[cyan]Executing restore...[/cyan]")
    try:
        result = restorer.execute(plan)
    except RuntimeError as exc:
        console.print(f"[red]Restore failed:[/red] {exc}")
        sys.exit(1)

    if result.success:
        console.print(
            f"\n[green]Restore complete.[/green]  "
            f"Files: {result.files_copied}  "
            f"Bytes: {result.bytes_copied:,}  "
            f"Metadata: {result.new_metadata_uri}"
        )
    else:
        console.print(f"\n[red]{len(result.errors)} error(s):[/red]")
        for e in result.errors:
            console.print(f"  • {e}")
        sys.exit(1)
