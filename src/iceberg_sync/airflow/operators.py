"""
airflow/operators.py — Airflow operators for Iceberg catalog sync.

These operators wrap CatalogSync for use in Airflow DAGs.  They handle:
•  Building storage backends from Airflow connections
•  XCom push of SyncResult for downstream tasks
•  Configurable failure behaviour (fail task vs. warn)

Operators
─────────
IcebergTableSyncOperator       — sync a single table (any source → any target)
IcebergNamespaceSyncOperator   — sync all tables under a namespace
IcebergHealthCheckOperator     — verify target metadata is clean (no leaked URIs)
NessieCatalogRegisterOperator  — register / update a table in a Nessie REST catalog

Typical DAG pattern (ADLS → MinIO → Nessie):

    sync  = IcebergTableSyncOperator(task_id="sync", ...)
    check = IcebergHealthCheckOperator(task_id="check", ...)
    reg   = NessieCatalogRegisterOperator(task_id="register_nessie", ...)

    sync >> check >> reg
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
            "target_metadata_uri": result.target_metadata_uri,
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


class DatahubEmitterOperator(BaseOperator):
    """
    Emit synced Iceberg table metadata to DataHub GMS.

    Designed to run after NessieCatalogRegisterOperator in a DAG chain:

        sync >> health_check >> nessie_register >> datahub_emit

    Reads the sync result from XCom (via sync_task_id) to obtain:
      •  target_metadata_uri  — used as the metadata_location in DataHub
      •  source_metadata_uri  — used to build the upstream lineage edge
      •  files_copied / bytes_copied / duration_seconds — emitted as custom properties

    Schema is read directly from target storage so DataHub receives the full
    unmasked schema regardless of any catalog-gateway OPA column exclusions.

    Args:
        gms_server:           DataHub GMS URL (e.g. http://datahub-gms:8080).
        namespace:            Iceberg namespace (e.g. "gold").
        table:                Table name (e.g. "top_customers").
        sync_task_id:         task_id of the upstream IcebergTableSyncOperator.
                              Used to pull metadata URIs and counters from XCom.
        datahub_token:        Personal Access Token for authenticated DataHub.
        env:                  DataHub environment (PROD / DEV / STAGING).
        source_platform:      DataHub platform name for source URN.
        target_platform:      DataHub platform name for target URN.
        target_storage_kwargs: Kwargs forwarded to create_storage() for reading
                              the target metadata.json to extract the schema.
                              If omitted, schema emission is skipped.
        extra_properties:     Additional custom properties for DatasetProperties.
        fail_on_error:        If True, task fails when DataHub is unreachable.
                              Defaults to False — DataHub issues should not
                              block the sync pipeline.

    XCom output (key: 'datahub_result'):
        {
            "dataset_urn": "urn:li:dataset:(...)",
            "namespace":   "gold",
            "table":       "top_customers",
            "emitted":     True,
        }
    """

    template_fields = ("gms_server", "namespace", "table")

    def __init__(
        self,
        *,
        gms_server: str,
        namespace: str,
        table: str,
        sync_task_id: str,
        datahub_token: Optional[str] = None,
        env: str = "PROD",
        source_platform: str = "iceberg",
        target_platform: str = "iceberg",
        target_storage_kwargs: Optional[Dict[str, Any]] = None,
        extra_properties: Optional[Dict[str, str]] = None,
        fail_on_error: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gms_server = gms_server
        self.namespace = namespace
        self.table = table
        self.sync_task_id = sync_task_id
        self.datahub_token = datahub_token
        self.env = env
        self.source_platform = source_platform
        self.target_platform = target_platform
        self.target_storage_kwargs = target_storage_kwargs or {}
        self.extra_properties = extra_properties or {}
        self.fail_on_error = fail_on_error

    def execute(self, context):
        from iceberg_sync.catalog.datahub import DatahubCatalog, _build_dataset_urn

        # ── Pull sync result from upstream XCom ──────────────────────────────
        sync_result = context["ti"].xcom_pull(
            task_ids=self.sync_task_id, key="sync_result"
        )
        if not sync_result:
            msg = (
                f"DatahubEmitterOperator: no sync_result XCom from task '{self.sync_task_id}'. "
                "Ensure IcebergTableSyncOperator runs upstream and succeeds."
            )
            if self.fail_on_error:
                raise ValueError(msg)
            log.warning(msg)
            return None

        if not sync_result.get("success"):
            log.warning(
                f"Upstream sync for {self.namespace}.{self.table} was not successful "
                "— skipping DataHub emit."
            )
            return None

        target_metadata_uri: str = sync_result.get("target_metadata_uri", "")
        source_metadata_uri: str = sync_result.get("source_metadata_uri", "")

        # ── Optionally read target metadata.json for schema emission ─────────
        target_metadata = None
        if self.target_storage_kwargs and target_metadata_uri:
            try:
                import json
                from iceberg_sync.storage import create_storage
                target_storage = create_storage(
                    target_metadata_uri, **self.target_storage_kwargs
                )
                raw = target_storage.read_bytes(target_metadata_uri)
                target_metadata = json.loads(raw)
                log.info(
                    f"Read target metadata for schema: "
                    f"{len(target_metadata.get('schemas', []))} schema(s)"
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    f"Could not read target metadata for schema emission "
                    f"({target_metadata_uri}): {e}"
                )

        # ── Build extra properties from sync counters ─────────────────────────
        props = {
            "files_copied":           str(sync_result.get("files_copied", 0)),
            "files_skipped":          str(sync_result.get("files_skipped", 0)),
            "bytes_copied":           str(sync_result.get("bytes_copied", 0)),
            "sync_duration_seconds":  str(sync_result.get("duration_seconds", 0)),
            "paths_translated":       str(sync_result.get("paths_translated", 0)),
            **self.extra_properties,
        }

        # ── Emit ──────────────────────────────────────────────────────────────
        dh = DatahubCatalog(
            gms_server=self.gms_server,
            token=self.datahub_token,
            env=self.env,
            source_platform=self.source_platform,
            target_platform=self.target_platform,
            fail_silently=not self.fail_on_error,
        )

        if not dh.test_connection():
            msg = f"DataHub GMS unreachable at {self.gms_server}"
            if self.fail_on_error:
                raise RuntimeError(msg)
            log.warning(msg)
            return None

        emitted = dh.emit_table(
            namespace=self.namespace,
            table=self.table,
            metadata_location=target_metadata_uri,
            target_metadata=target_metadata,
            source_metadata_location=source_metadata_uri or None,
            extra_properties=props,
        )

        target_urn = _build_dataset_urn(
            self.target_platform, self.namespace, self.table, self.env
        )
        summary = {
            "dataset_urn": target_urn,
            "namespace":   self.namespace,
            "table":       self.table,
            "emitted":     emitted,
        }
        context["ti"].xcom_push(key="datahub_result", value=summary)
        log.info(f"DataHub emit {'succeeded' if emitted else 'failed'}: {target_urn}")
        return summary


class NessieCatalogRegisterOperator(BaseOperator):
    """
    Register or update an Iceberg table in a Nessie REST catalog.

    Run this after IcebergTableSyncOperator to make the synced table
    immediately queryable via any Iceberg REST catalog client (Spark,
    Trino, PyIceberg, DuckDB).

    The metadata_location is taken from XCom of the upstream sync task
    by default.  Override with a literal string if needed.

    Args:
        nessie_uri:         Nessie base URL (e.g. http://nessie:19120)
        namespace:          Catalog namespace (e.g. "gold")
        table:              Table name (e.g. "top_customers")
        nessie_ref:         Branch to commit to (default: "main")
        nessie_token:       Bearer token for secured Nessie deployments
        metadata_location:  Explicit metadata URI.  If None, reads from
                            XCom key 'target_metadata_uri' of the upstream
                            sync task (task_id passed via sync_task_id).
        sync_task_id:       task_id of the upstream IcebergTableSyncOperator
                            used to pull metadata_location from XCom.

    XCom output (key: 'nessie_result'):
        {"namespace": ..., "table": ..., "metadata_location": ..., "action": "registered"|"updated"|"skipped"}
    """

    template_fields = ("nessie_uri", "namespace", "table", "metadata_location")

    def __init__(
        self,
        *,
        nessie_uri: str,
        namespace: str,
        table: str,
        nessie_ref: str = "main",
        nessie_token: Optional[str] = None,
        metadata_location: Optional[str] = None,
        sync_task_id: Optional[str] = None,
        fail_on_error: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.nessie_uri = nessie_uri
        self.namespace = namespace
        self.table = table
        self.nessie_ref = nessie_ref
        self.nessie_token = nessie_token
        self.metadata_location = metadata_location
        self.sync_task_id = sync_task_id
        self.fail_on_error = fail_on_error

    def execute(self, context):
        from iceberg_sync.catalog.nessie import NessieCatalog

        # Resolve metadata_location: explicit value, or pull from upstream XCom
        metadata_loc = self.metadata_location
        if not metadata_loc and self.sync_task_id:
            sync_result = context["ti"].xcom_pull(
                task_ids=self.sync_task_id, key="sync_result"
            )
            if sync_result:
                metadata_loc = sync_result.get("target_metadata_uri")

        if not metadata_loc:
            msg = (
                "NessieCatalogRegisterOperator: could not determine metadata_location. "
                "Either set metadata_location directly or set sync_task_id to pull "
                "it from an upstream IcebergTableSyncOperator."
            )
            if self.fail_on_error:
                raise ValueError(msg)
            log.warning(msg)
            return None

        nessie = NessieCatalog(
            uri=self.nessie_uri,
            ref=self.nessie_ref,
            token=self.nessie_token,
        )

        if not nessie.ping():
            msg = f"Cannot reach Nessie at {self.nessie_uri}"
            if self.fail_on_error:
                raise RuntimeError(msg)
            log.warning(msg)
            return None

        log.info(f"Registering {self.namespace}.{self.table} → {metadata_loc}")
        result = nessie.register_or_update(
            namespace=self.namespace,
            table=self.table,
            metadata_location=metadata_loc,
        )

        action = "skipped" if result.get("skipped") else (
            "updated" if nessie.table_exists(self.namespace, self.table) else "registered"
        )

        summary = {
            "namespace": self.namespace,
            "table": self.table,
            "metadata_location": metadata_loc,
            "nessie_uri": self.nessie_uri,
            "action": action,
        }
        context["ti"].xcom_push(key="nessie_result", value=summary)
        log.info(f"Nessie {action}: {self.namespace}.{self.table}")
        return summary
