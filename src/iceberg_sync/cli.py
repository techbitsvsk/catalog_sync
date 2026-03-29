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
from typing import Optional

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
    # Nessie catalog registration (optional — skipped if not provided)
    fn = click.option("--nessie-uri", default=None, help="Nessie base URL (e.g. http://localhost:19120). "
                      "When provided, registers / updates the table in Nessie after file sync.")(fn)
    fn = click.option("--nessie-ref", default="main", show_default=True, help="Nessie branch name")(fn)
    fn = click.option("--nessie-token", default=None, help="Bearer token for secured Nessie")(fn)
    # OAuth options (for enterprise deployments with token-secured Nessie)
    fn = click.option("--oauth-url", default=None, envvar="OAUTH_URL",
                      help="OAuth service URL. When provided with --oauth-client-id and "
                           "--oauth-client-secret, tokens are automatically fetched for Nessie.")(fn)
    fn = click.option("--oauth-client-id", default=None, envvar="OAUTH_CLIENT_ID",
                      help="OAuth client ID")(fn)
    fn = click.option("--oauth-client-secret", default=None, envvar="OAUTH_CLIENT_SECRET",
                      help="OAuth client secret")(fn)
    fn = click.option("--oauth-scope", default="catalog:read catalog:write", show_default=True,
                      help="OAuth scopes to request")(fn)
    # Policy enforcement
    fn = click.option("--policy-url", default=None, envvar="POLICY_URL",
                      help="Policy service URL. When provided, data contract policies are "
                           "enforced before catalog operations.")(fn)
    # Optional metadata location override — bypasses filesystem discovery
    fn = click.option(
        "--metadata-location", default=None,
        help=(
            "Explicit URI to the source metadata.json. "
            "When supplied, filesystem discovery (version-hint.text / directory scan) "
            "is skipped entirely. Use this when the authoritative pointer comes from "
            "a REST catalog endpoint (Azure Fabric Iceberg API, AWS Glue, etc.)."
        ),
    )(fn)
    return fn


def _build_oauth_and_policy(kwargs: dict):
    """
    Extract OAuth/policy options from kwargs and return (oauth_client, policy_client).

    Both may be None if the corresponding options were not provided.
    """
    oauth_url = kwargs.pop("oauth_url", None)
    oauth_client_id = kwargs.pop("oauth_client_id", None)
    oauth_client_secret = kwargs.pop("oauth_client_secret", None)
    oauth_scope = kwargs.pop("oauth_scope", "catalog:read catalog:write")
    policy_url = kwargs.pop("policy_url", None)

    oauth_client = None
    if oauth_url and oauth_client_id and oauth_client_secret:
        from iceberg_sync.auth.oauth_client import OAuthClient
        oauth_client = OAuthClient(
            server_url=oauth_url,
            client_id=oauth_client_id,
            client_secret=oauth_client_secret,
            scope=oauth_scope,
        )

    policy_client = None
    if policy_url and oauth_client:
        from iceberg_sync.auth.policy_client import PolicyClient
        policy_client = PolicyClient(
            service_url=policy_url,
            principal=oauth_client.client_id,
        )
    elif policy_url and oauth_client_id:
        from iceberg_sync.auth.policy_client import PolicyClient
        policy_client = PolicyClient(
            service_url=policy_url,
            principal=oauth_client_id,
        )

    return oauth_client, policy_client


def _register_in_nessie(
    nessie_uri: str,
    nessie_ref: str,
    nessie_token: Optional[str],
    namespace: str,
    table: str,
    metadata_location: str,
    oauth_client=None,
    policy_client=None,
) -> None:
    """Register or update a table in Nessie after file sync."""
    from iceberg_sync.catalog.nessie import NessieCatalog

    nessie = NessieCatalog(
        uri=nessie_uri,
        ref=nessie_ref,
        token=nessie_token,
        oauth_client=oauth_client,
        policy_client=policy_client,
    )
    if not nessie.ping():
        raise RuntimeError(f"Cannot reach Nessie at {nessie_uri} — is the server running?")

    result = nessie.register_or_update(
        namespace=namespace,
        table=table,
        metadata_location=metadata_location,
    )
    if result.get("skipped"):
        console.print(f"  [dim]Nessie: {namespace}.{table} already up-to-date[/dim]")
    else:
        console.print(f"  [green]Nessie: {namespace}.{table} registered at {nessie_uri}[/green]")


def _build_sync(
    source_root, target_root, parallel, all_snapshots,
    source_region, source_endpoint, source_access_key, source_secret_key,
    target_account_name, target_account_key, target_endpoint,
    target_access_key, target_secret_key, target_region, target_gcs_project,
    nessie_uri=None,
    **kwargs,
) -> CatalogSync:
    """Build CatalogSync from CLI options."""
    translator = PathTranslator([(source_root, target_root)])

    source_scheme = source_root.split("://")[0]
    source_kwargs = {}
    if source_scheme in ("s3", "s3a", "s3n"):
        source_kwargs["region_name"] = source_region
        if source_endpoint:
            source_kwargs["endpoint_url"] = source_endpoint
        if source_access_key:
            source_kwargs["aws_access_key_id"] = source_access_key
            source_kwargs["aws_secret_access_key"] = source_secret_key
    elif source_scheme in ("abfss", "abfs"):
        # Extract account name from abfss://container@account.dfs.core.windows.net/
        import re as _re
        _m = _re.match(r"abfss?://[^@]+@([^.]+)\.", source_root)
        if _m:
            source_kwargs["storage_account_name"] = _m.group(1)
        if source_secret_key:
            source_kwargs["storage_account_key"] = source_secret_key
    elif source_scheme == "gs":
        pass  # GCS uses ADC; no extra kwargs needed here

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

    # When a REST catalog (Nessie) is the target pointer, version-hint.text
    # is irrelevant — the catalog server is the authoritative metadata pointer.
    write_version_hint = not bool(nessie_uri)

    return CatalogSync(
        translator=translator,
        source_storage=create_storage(source_root, **source_kwargs),
        target_storage=create_storage(target_root, **target_kwargs),
        max_parallel_copies=parallel,
        rewrite_all_snapshots=all_snapshots,
        write_version_hint=write_version_hint,
    )


# ── Commands ─────────────────────────────────────────────────────────────────

@main.command()
@click.option("--table", required=True, help="Table path relative to warehouse root (e.g. gold/top_customers)")
@_common_options
def table(table, source_root, target_root, dry_run, nessie_uri, nessie_ref, nessie_token,
          metadata_location, **kwargs):
    """Sync a single Iceberg table, then optionally register in Nessie."""
    oauth_client, policy_client = _build_oauth_and_policy(kwargs)
    sync = _build_sync(source_root=source_root, target_root=target_root, nessie_uri=nessie_uri, **kwargs)
    table_root = source_root.rstrip("/") + "/" + table.strip("/") + "/"

    console.print(f"\n[bold]Syncing table:[/bold] {table_root}")
    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be made[/yellow]\n")
    if nessie_uri:
        console.print(f"[dim]Nessie catalog:[/dim] {nessie_uri}  (ref: {nessie_ref})\n")
    if oauth_client:
        console.print(f"[dim]OAuth client:[/dim] {oauth_client.client_id}\n")

    if metadata_location:
        console.print(f"[dim]Metadata location:[/dim] {metadata_location} (discovery skipped)\n")

    result = sync.sync_table(table_root, dry_run=dry_run, metadata_location=metadata_location)
    _print_result(result)

    if result.success and nessie_uri and not dry_run:
        # Parse namespace and table name from the table argument
        # e.g. "gold/top_customers" → namespace="gold", table_name="top_customers"
        parts = table.strip("/").split("/")
        ns = ".".join(parts[:-1]) if len(parts) > 1 else "default"
        tbl_name = parts[-1]
        try:
            _register_in_nessie(
                nessie_uri=nessie_uri,
                nessie_ref=nessie_ref,
                nessie_token=nessie_token,
                namespace=ns,
                table=tbl_name,
                metadata_location=result.target_metadata_uri,
                oauth_client=oauth_client,
                policy_client=policy_client,
            )
        except Exception as e:
            console.print(f"  [red]Nessie registration failed: {e}[/red]")
            sys.exit(1)

    sys.exit(0 if result.success else 1)


@main.command()
@click.option("--namespace", required=True, help="Namespace path relative to warehouse root (e.g. gold)")
@_common_options
def namespace(namespace, source_root, target_root, dry_run, nessie_uri, nessie_ref, nessie_token, **kwargs):
    """Sync all tables in an Iceberg namespace, then optionally register in Nessie."""
    oauth_client, policy_client = _build_oauth_and_policy(kwargs)
    sync = _build_sync(source_root=source_root, target_root=target_root, nessie_uri=nessie_uri, **kwargs)
    ns_root = source_root.rstrip("/") + "/" + namespace.strip("/") + "/"

    console.print(f"\n[bold]Syncing namespace:[/bold] {ns_root}")
    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be made[/yellow]\n")
    if nessie_uri:
        console.print(f"[dim]Nessie catalog:[/dim] {nessie_uri}  (ref: {nessie_ref})\n")
    if oauth_client:
        console.print(f"[dim]OAuth client:[/dim] {oauth_client.client_id}\n")

    results = sync.sync_namespace(ns_root, dry_run=dry_run)
    for r in results:
        _print_result(r)

    nessie_failed = 0
    if nessie_uri and not dry_run:
        for r in results:
            if not r.success:
                continue
            # Derive table name from table root URI: last non-empty path segment
            tbl_path = r.table.rstrip("/").replace(source_root.rstrip("/") + "/", "")
            parts = tbl_path.strip("/").split("/")
            ns_cat = ".".join(parts[:-1]) if len(parts) > 1 else namespace
            tbl_name = parts[-1]
            try:
                _register_in_nessie(
                    nessie_uri=nessie_uri,
                    nessie_ref=nessie_ref,
                    nessie_token=nessie_token,
                    namespace=ns_cat,
                    table=tbl_name,
                    metadata_location=r.target_metadata_uri,
                    oauth_client=oauth_client,
                    policy_client=policy_client,
                )
            except Exception as e:
                console.print(f"  [red]Nessie registration failed for {tbl_name}: {e}[/red]")
                nessie_failed += 1

    failed = sum(1 for r in results if not r.success) + nessie_failed
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
