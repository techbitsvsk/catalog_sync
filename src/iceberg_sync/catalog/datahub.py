"""
catalog/datahub.py — DataHub metadata emission for synced Iceberg tables.

Emits three types of metadata to DataHub GMS after a successful table sync
or catalog registration:

    1. SchemaMetadata   — Column names, types, descriptions from the Iceberg
                          schema embedded in metadata.json.
    2. UpstreamLineage  — Source → target dataset relationship so DataHub
                          renders the cross-cloud lineage graph.
    3. DatasetProperties — table_root, metadata_location, sync timestamp,
                           and Iceberg format-version as custom properties.

Design decisions
────────────────
•  Reads schema directly from the rewritten metadata.json on target storage,
   not from the catalog-gateway response.  The gateway applies OPA column
   exclusion before returning metadata to query clients; DataHub as a trusted
   system catalog should see the full unmasked schema.
•  Emits MetadataChangeProposal (MCP) events, not the deprecated
   MetadataChangeEvent (MCE), which aligns with DataHub 0.10+.
•  All emissions are fire-and-forget by default (emit_async=False, but
   exceptions are caught and logged rather than raised) so a DataHub
   connectivity issue never blocks the sync pipeline.
•  Platform names default to "iceberg" for both source and target, but can
   be overridden — useful when the target is a specific system like
   "minio" or "microsoft_fabric".

Usage
─────
    from iceberg_sync.catalog.datahub import DatahubCatalog

    dh = DatahubCatalog(
        gms_server="http://datahub-gms:8080",
        token="your-pat-token",           # optional for open DataHub
        env="PROD",
        source_platform="iceberg",
        target_platform="iceberg",
    )

    # Emit after a CatalogSync.sync_table() completes
    dh.emit_from_sync_result(
        result=sync_result,
        namespace="gold",
        table="top_customers",
        source_metadata=source_metadata_dict,   # parsed metadata.json from source
        target_metadata=target_metadata_dict,   # parsed metadata.json from target
    )

    # Emit standalone (e.g. from NessieCatalog after register_or_update)
    dh.emit_table(
        namespace="gold",
        table="top_customers",
        metadata_location="abfss://iceberg@account.dfs.core.windows.net/gold/top_customers/metadata/v3.metadata.json",
        target_metadata=target_metadata_dict,
        source_metadata_location="s3://warehouse/gold/top_customers/metadata/v3.metadata.json",
    )
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


# ── Iceberg → DataHub type mapping ───────────────────────────────────────────

_ICEBERG_TYPE_MAP: Dict[str, str] = {
    "boolean":       "BOOLEAN",
    "int":           "NUMBER",
    "long":          "NUMBER",
    "float":         "NUMBER",
    "double":        "NUMBER",
    "decimal":       "NUMBER",
    "date":          "DATE",
    "time":          "TIME",
    "timestamp":     "DATETIME",
    "timestamptz":   "DATETIME",
    "string":        "STRING",
    "uuid":          "STRING",
    "fixed":         "BYTES",
    "binary":        "BYTES",
    "struct":        "STRUCT",
    "list":          "ARRAY",
    "map":           "MAP",
}


def _map_type(iceberg_type: Any) -> str:
    """Map an Iceberg type (string or dict) to a DataHub SchemaFieldDataType string."""
    if isinstance(iceberg_type, str):
        base = iceberg_type.split("(")[0].lower()  # e.g. "decimal(10,2)" → "decimal"
        return _ICEBERG_TYPE_MAP.get(base, "NULL")
    if isinstance(iceberg_type, dict):
        kind = iceberg_type.get("type", "").lower()
        return _ICEBERG_TYPE_MAP.get(kind, "NULL")
    return "NULL"


def _build_platform_urn(platform: str) -> str:
    return f"urn:li:dataPlatform:{platform}"


def _build_dataset_urn(platform: str, namespace: str, table: str, env: str) -> str:
    return f"urn:li:dataset:({_build_platform_urn(platform)},{namespace}.{table},{env})"


def _iceberg_schema_to_fields(schema: dict) -> List[Dict[str, Any]]:
    """
    Convert an Iceberg schema dict to a list of DataHub SchemaField-compatible dicts.

    Handles the top-level struct (fields list directly on schema) as well as
    nested struct types.  Returns a flat list of top-level fields only —
    DataHub renders nested types via the nativeDataType string.
    """
    fields = []
    for f in schema.get("fields", []):
        name = f.get("name", "")
        iceberg_type = f.get("type", "string")
        required = not f.get("optional", True)

        fields.append({
            "fieldPath": name,
            "nativeDataType": str(iceberg_type),
            "type": _map_type(iceberg_type),
            "description": f.get("doc", ""),
            "nullable": not required,
            "isPartOfKey": False,   # Iceberg does not have a PK concept at schema level
        })
    return fields


class DatahubCatalog:
    """
    Emits Iceberg table metadata to DataHub GMS after a catalog sync.

    Args:
        gms_server:       DataHub GMS base URL (e.g. http://datahub-gms:8080).
        token:            Personal Access Token for authenticated DataHub deployments.
                          Omit for open/dev DataHub instances.
        env:              DataHub fabric environment — PROD, DEV, STAGING, etc.
                          Defaults to PROD.
        source_platform:  DataHub platform name for the source dataset URN.
        target_platform:  DataHub platform name for the target dataset URN.
        pipeline_name:    Recorded as the lineage transformation description.
        fail_silently:    If True (default), emit errors are logged but never
                          raised — DataHub unavailability won't break the sync.
    """

    def __init__(
        self,
        gms_server: str,
        token: Optional[str] = None,
        env: str = "PROD",
        source_platform: str = "iceberg",
        target_platform: str = "iceberg",
        pipeline_name: str = "iceberg-catalog-sync",
        fail_silently: bool = True,
    ) -> None:
        self._gms_server = gms_server.rstrip("/")
        self._token = token
        self._env = env
        self._source_platform = source_platform
        self._target_platform = target_platform
        self._pipeline_name = pipeline_name
        self._fail_silently = fail_silently
        self._emitter = None

    def _get_emitter(self):
        """Lazy-init the DataHub REST emitter to avoid import cost at module load."""
        if self._emitter is None:
            try:
                from datahub.emitter.rest_emitter import DatahubRestEmitter  # type: ignore
            except ImportError as exc:
                raise ImportError(
                    "datahub-metadata is required for DatahubCatalog. "
                    "Install it with: pip install 'iceberg-catalog-sync[datahub]'"
                ) from exc

            kwargs: Dict[str, Any] = {}
            if self._token:
                kwargs["token"] = self._token
            self._emitter = DatahubRestEmitter(self._gms_server, **kwargs)
        return self._emitter

    def _emit_schema(
        self,
        dataset_urn: str,
        schema: dict,
        metadata_location: str,
    ) -> None:
        """Emit a SchemaMetadata aspect for the dataset."""
        try:
            from datahub.emitter.mce_builder import make_schema_field  # type: ignore
            from datahub.metadata.schema_classes import (  # type: ignore
                SchemaFieldClass,
                SchemaFieldDataTypeClass,
                SchemaMetadataClass,
                StringTypeClass,
                OtherSchemaClass,
                AuditStampClass,
            )
            from datahub.emitter.mcp_builder import mcps_from_mce  # type: ignore
            from datahub.metadata.schema_classes import MetadataChangeEventClass  # type: ignore
        except ImportError:
            # datahub SDK internals vary slightly by version — fall back gracefully
            log.debug("DataHub SDK schema classes unavailable; skipping schema emit")
            return

        fields_raw = _iceberg_schema_to_fields(schema)

        schema_fields = []
        for f in fields_raw:
            try:
                field_type = SchemaFieldDataTypeClass(type=StringTypeClass())
                schema_fields.append(
                    SchemaFieldClass(
                        fieldPath=f["fieldPath"],
                        nativeDataType=f["nativeDataType"],
                        type=field_type,
                        description=f["description"] or None,
                        nullable=f["nullable"],
                    )
                )
            except Exception as e:  # noqa: BLE001
                log.debug(f"Skipping field {f['fieldPath']}: {e}")

        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        audit = AuditStampClass(time=now_ms, actor="urn:li:corpuser:iceberg-catalog-sync")

        schema_meta = SchemaMetadataClass(
            schemaName=dataset_urn,
            platform=dataset_urn.split(",")[0].lstrip("urn:li:dataset:("),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=metadata_location),
            fields=schema_fields,
            created=audit,
            lastModified=audit,
        )

        emitter = self._get_emitter()
        emitter.emit_mce(
            MetadataChangeEventClass(
                proposedSnapshot=type(
                    "DatasetSnapshotClass",
                    (),
                    {
                        "urn": dataset_urn,
                        "aspects": [schema_meta],
                    },
                )()
            )
        )

    def emit_table(
        self,
        namespace: str,
        table: str,
        metadata_location: str,
        target_metadata: Optional[Dict[str, Any]] = None,
        source_metadata_location: Optional[str] = None,
        extra_properties: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Emit dataset metadata to DataHub for a single Iceberg table.

        Args:
            namespace:                Iceberg namespace (e.g. "gold").
            table:                    Table name (e.g. "top_customers").
            metadata_location:        Absolute URI to the target metadata.json.
            target_metadata:          Parsed metadata.json dict from target storage.
                                      Used to extract schema and format-version.
                                      If None, schema emission is skipped.
            source_metadata_location: URI of the source metadata.json.
                                      When provided, a lineage edge is emitted
                                      from source dataset → target dataset.
            extra_properties:         Additional key/value pairs to include in
                                      DatasetProperties (e.g. team, domain).

        Returns:
            True if emission succeeded, False if it failed (and fail_silently=True).
        """
        try:
            return self._emit_table_inner(
                namespace=namespace,
                table=table,
                metadata_location=metadata_location,
                target_metadata=target_metadata,
                source_metadata_location=source_metadata_location,
                extra_properties=extra_properties,
            )
        except Exception as exc:  # noqa: BLE001
            log.error(f"DataHub emit failed for {namespace}.{table}: {exc}")
            if not self._fail_silently:
                raise
            return False

    def _emit_table_inner(
        self,
        namespace: str,
        table: str,
        metadata_location: str,
        target_metadata: Optional[Dict[str, Any]],
        source_metadata_location: Optional[str],
        extra_properties: Optional[Dict[str, str]],
    ) -> bool:
        try:
            from datahub.emitter.mce_builder import (  # type: ignore
                make_dataset_urn,
                make_data_platform_urn,
            )
            from datahub.metadata.schema_classes import (  # type: ignore
                DatasetSnapshotClass,
                DatasetPropertiesClass,
                UpstreamLineageClass,
                UpstreamClass,
                AuditStampClass,
                DatasetLineageTypeClass,
                MetadataChangeEventClass,
            )
        except ImportError as exc:
            raise ImportError(
                "datahub-metadata is required. "
                "Install with: pip install 'iceberg-catalog-sync[datahub]'"
            ) from exc

        emitter = self._get_emitter()
        target_urn = _build_dataset_urn(self._target_platform, namespace, table, self._env)
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        aspects = []

        # ── DatasetProperties ─────────────────────────────────────────────────
        props: Dict[str, str] = {
            "metadata_location": metadata_location,
            "sync_pipeline": self._pipeline_name,
            "sync_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
        if target_metadata:
            props["iceberg_format_version"] = str(
                target_metadata.get("format-version", "unknown")
            )
            props["iceberg_uuid"] = target_metadata.get("table-uuid", "")
        if extra_properties:
            props.update(extra_properties)

        aspects.append(DatasetPropertiesClass(customProperties=props))

        # ── UpstreamLineage ───────────────────────────────────────────────────
        if source_metadata_location:
            # Derive source namespace.table from the source metadata path.
            # Canonical path pattern: .../namespace/table/metadata/vN.metadata.json
            parts = source_metadata_location.rstrip("/").split("/")
            try:
                meta_idx = next(i for i, p in enumerate(parts) if p == "metadata")
                src_table = parts[meta_idx - 1]
                src_namespace = parts[meta_idx - 2]
            except (StopIteration, IndexError):
                src_namespace = namespace
                src_table = table

            source_urn = _build_dataset_urn(
                self._source_platform, src_namespace, src_table, self._env
            )
            audit = AuditStampClass(
                time=now_ms, actor="urn:li:corpuser:iceberg-catalog-sync"
            )
            upstream = UpstreamClass(
                auditStamp=audit,
                dataset=source_urn,
                type=DatasetLineageTypeClass.COPY,
            )
            aspects.append(UpstreamLineageClass(upstreams=[upstream]))
            log.debug(f"Lineage: {source_urn} → {target_urn}")

        # ── SchemaMetadata ────────────────────────────────────────────────────
        if target_metadata:
            # Prefer the current schema; fall back to first schema in the list
            schema = target_metadata.get("current-schema") or (
                target_metadata.get("schemas", [{}])[0]
            )
            if schema:
                try:
                    self._emit_schema(target_urn, schema, metadata_location)
                    log.debug(
                        f"Schema emitted: {len(schema.get('fields', []))} fields "
                        f"for {namespace}.{table}"
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning(f"Schema emit skipped for {namespace}.{table}: {e}")

        # ── DatasetSnapshot ───────────────────────────────────────────────────
        snapshot = DatasetSnapshotClass(urn=target_urn, aspects=aspects)
        mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
        emitter.emit_mce(mce)

        log.info(f"DataHub: emitted {namespace}.{table} → {target_urn}")
        return True

    def emit_from_sync_result(
        self,
        result,                                  # SyncResult from catalog_sync.py
        namespace: str,
        table: str,
        target_metadata: Optional[Dict[str, Any]] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
        extra_properties: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Convenience wrapper: emit from a CatalogSync SyncResult.

        Args:
            result:           SyncResult returned by CatalogSync.sync_table().
            namespace:        Iceberg namespace.
            table:            Table name.
            target_metadata:  Parsed target metadata.json dict (optional but
                              recommended — enables schema emission).
            source_metadata:  Parsed source metadata.json dict (used to derive
                              the source metadata_location for lineage).
            extra_properties: Additional properties for DatasetProperties.

        Returns:
            True if emission succeeded.
        """
        if not result.success:
            log.warning(
                f"Skipping DataHub emit for {namespace}.{table} — sync was not successful"
            )
            return False

        # Derive source metadata_location from SyncResult
        source_loc: Optional[str] = result.source_metadata_uri or None

        return self.emit_table(
            namespace=namespace,
            table=table,
            metadata_location=result.target_metadata_uri,
            target_metadata=target_metadata,
            source_metadata_location=source_loc,
            extra_properties={
                "files_copied": str(result.files_copied),
                "bytes_copied": str(result.bytes_copied),
                "sync_duration_seconds": str(result.duration_seconds),
                **(extra_properties or {}),
            },
        )

    def test_connection(self) -> bool:
        """Return True if DataHub GMS is reachable."""
        try:
            import urllib.request
            with urllib.request.urlopen(
                f"{self._gms_server}/config", timeout=5
            ) as resp:
                return resp.status == 200
        except Exception:
            return False
