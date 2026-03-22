"""
airflow/operators.py — Airflow operators for Iceberg catalog sync.

These operators wrap CatalogSync for use in Airflow DAGs.  They handle:
•  Building storage backends from Airflow connections
•  XCom push of SyncResult for downstream tasks
•  Configurable failure behaviour (fail task vs. warn)

Usage in a DAG:

    from iceberg_sync.airflow.operators import IcebergTableSyncOperator

    sync_gold = IcebergTableSyncOperator(
        task_id="sync_gold_revenue",
        source_root="s3://warehouse/iceberg/",
        target_root="abfss://iceberg@account.dfs.core.windows.net/iceberg/",
        table="gold/revenue_by_order_date",
        source_storage_kwargs={"region_name": "eu-west-2"},
        target_storage_kwargs={"storage_account_name": "sticebergpipeline"},
    )
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator

log = logging.getLogger(__name__)


class IcebergTableSyncOperator(BaseOperator):
    """
    Sync a single Iceberg table from source to target cloud storage.

    Pushes SyncResult summary to XCom under key 'sync_result'.
    """

    template_fields = ("source_root", "target_root", "table")

    def __init__(
        self,
        *,
        source_root: str,
        target_root: str,
        table: str,
        source_storage_kwargs: Optional[Dict[str, Any]] = None,
        target_storage_kwargs: Optional[Dict[str, Any]] = None,
        max_parallel_copies: int = 4,
        rewrite_all_snapshots: bool = False,
        dry_run: bool = False,
        fail_on_error: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_root = source_root
        self.target_root = target_root
        self.table = table
        self.source_storage_kwargs = source_storage_kwargs or {}
        self.target_storage_kwargs = target_storage_kwargs or {}
        self.max_parallel_copies = max_parallel_copies
        self.rewrite_all_snapshots = rewrite_all_snapshots
        self.dry_run = dry_run
        self.fail_on_error = fail_on_error

    def execute(self, context):
        from iceberg_sync.path_translator import PathTranslator
        from iceberg_sync.storage import create_storage
        from iceberg_sync.sync import CatalogSync

        translator = PathTranslator([(self.source_root, self.target_root)])

        source = create_storage(self.source_root, **self.source_storage_kwargs)
        target = create_storage(self.target_root, **self.target_storage_kwargs)

        sync = CatalogSync(
            translator=translator,
            source_storage=source,
            target_storage=target,
            max_parallel_copies=self.max_parallel_copies,
            rewrite_all_snapshots=self.rewrite_all_snapshots,
        )

        table_root = self.source_root.rstrip("/") + "/" + self.table.strip("/") + "/"
        log.info(f"Syncing table: {table_root}")

        result = sync.sync_table(table_root, dry_run=self.dry_run)

        # Push summary to XCom
        summary = {
            "table": result.table,
            "success": result.success,
            "files_copied": result.files_copied,
            "files_skipped": result.files_skipped,
            "bytes_copied": result.bytes_copied,
            "duration_seconds": result.duration_seconds,
            "paths_translated": (
                result.rewrite_stats.data_file_paths_translated
                if result.rewrite_stats else 0
            ),
            "errors": result.errors,
        }
        context["ti"].xcom_push(key="sync_result", value=summary)

        if not result.success and self.fail_on_error:
            raise RuntimeError(
                f"Iceberg sync failed for {self.table}: {result.errors}"
            )

        return summary


class IcebergNamespaceSyncOperator(BaseOperator):
    """
    Discover and sync all Iceberg tables under a namespace.
    """

    template_fields = ("source_root", "target_root", "namespace")

    def __init__(
        self,
        *,
        source_root: str,
        target_root: str,
        namespace: str,
        source_storage_kwargs: Optional[Dict[str, Any]] = None,
        target_storage_kwargs: Optional[Dict[str, Any]] = None,
        max_parallel_copies: int = 4,
        rewrite_all_snapshots: bool = False,
        dry_run: bool = False,
        fail_on_error: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_root = source_root
        self.target_root = target_root
        self.namespace = namespace
        self.source_storage_kwargs = source_storage_kwargs or {}
        self.target_storage_kwargs = target_storage_kwargs or {}
        self.max_parallel_copies = max_parallel_copies
        self.rewrite_all_snapshots = rewrite_all_snapshots
        self.dry_run = dry_run
        self.fail_on_error = fail_on_error

    def execute(self, context):
        from iceberg_sync.path_translator import PathTranslator
        from iceberg_sync.storage import create_storage
        from iceberg_sync.sync import CatalogSync

        translator = PathTranslator([(self.source_root, self.target_root)])

        source = create_storage(self.source_root, **self.source_storage_kwargs)
        target = create_storage(self.target_root, **self.target_storage_kwargs)

        sync = CatalogSync(
            translator=translator,
            source_storage=source,
            target_storage=target,
            max_parallel_copies=self.max_parallel_copies,
            rewrite_all_snapshots=self.rewrite_all_snapshots,
        )

        ns_root = self.source_root.rstrip("/") + "/" + self.namespace.strip("/") + "/"
        log.info(f"Syncing namespace: {ns_root}")

        results = sync.sync_namespace(ns_root, dry_run=self.dry_run)

        summaries = []
        failed = []
        for r in results:
            s = {
                "table": r.table,
                "success": r.success,
                "files_copied": r.files_copied,
                "errors": r.errors,
            }
            summaries.append(s)
            if not r.success:
                failed.append(r.table)

        context["ti"].xcom_push(key="sync_results", value=summaries)

        if failed and self.fail_on_error:
            raise RuntimeError(f"Iceberg sync failed for {len(failed)} tables: {failed}")

        return summaries


class IcebergHealthCheckOperator(BaseOperator):
    """
    Verify that a target Iceberg table is readable after sync.

    Checks that:
    •  metadata.json exists at the target path
    •  version-hint.text is present
    •  No source-scheme URIs leak into the target metadata

    Use this as a downstream validation task after sync.
    """

    template_fields = ("target_root", "table")

    def __init__(
        self,
        *,
        target_root: str,
        table: str,
        source_scheme: str = "s3",
        target_storage_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_root = target_root
        self.table = table
        self.source_scheme = source_scheme
        self.target_storage_kwargs = target_storage_kwargs or {}

    def execute(self, context):
        from iceberg_sync.metadata.rewriter import find_latest_metadata
        from iceberg_sync.storage import create_storage

        target = create_storage(self.target_root, **self.target_storage_kwargs)
        table_root = self.target_root.rstrip("/") + "/" + self.table.strip("/") + "/"

        # Check metadata exists
        metadata_uri = find_latest_metadata(target, table_root)
        log.info(f"Found target metadata: {metadata_uri}")

        # Read and check for leaked source-scheme URIs
        metadata_text = target.read_text(metadata_uri)
        leaked_prefix = f"{self.source_scheme}://"
        if leaked_prefix in metadata_text:
            # Count occurrences
            count = metadata_text.count(leaked_prefix)
            raise RuntimeError(
                f"Target metadata still contains {count} references to "
                f"'{leaked_prefix}'. Metadata rewrite may have failed."
            )

        log.info(f"Health check passed: {table_root} — no leaked {leaked_prefix} URIs")

        context["ti"].xcom_push(key="health_check", value={
            "table": self.table,
            "metadata_uri": metadata_uri,
            "healthy": True,
        })

        return True
