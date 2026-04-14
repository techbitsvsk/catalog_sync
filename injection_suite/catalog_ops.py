"""
injection_suite/catalog_ops.py
================================
PyIceberg catalog operations for the injection test suite.

Two clients are used:
  ops-client       — write access (poisons table metadata)
  analytics-client — read access  (simulates the downstream RAG pipeline)

Both connect through catalog-gateway so OPA enforcement is exercised.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

# ── Connection constants ──────────────────────────────────────────────────────

GATEWAY_URL     = os.getenv("GATEWAY_URL",     "http://localhost:8083")
OAUTH_URL       = os.getenv("OAUTH_URL",       "http://localhost:8081/token")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://localhost:9000")
MINIO_KEY       = os.getenv("MINIO_KEY",       "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET",    "minioadmin")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET",    "warehouse")

# Namespace for all injection test tables
INJECTION_NS    = "gold"
INJECTION_TABLE = "orders_inj"
FULL_TABLE_NAME = f"{INJECTION_NS}.{INJECTION_TABLE}"

# Iceberg schema — simplified TPC-H orders (no PII columns)
ORDERS_SCHEMA = Schema(
    NestedField(1,  "order_key",      LongType(),         required=True),
    NestedField(2,  "customer_key",   LongType()),
    NestedField(3,  "order_status",   StringType()),
    NestedField(4,  "total_price",    DecimalType(15, 2)),
    NestedField(5,  "order_date",     DateType()),
    NestedField(6,  "order_priority", StringType()),
    NestedField(7,  "clerk",          StringType()),
    NestedField(8,  "ship_priority",  IntegerType()),
    NestedField(9,  "comment",        StringType()),   # ← column doc injection target
    NestedField(10, "order_month",    StringType()),   # partition column
)

PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=10, field_id=1000,
        transform=IdentityTransform(), name="order_month",
    )
)


# ── Catalog factory ───────────────────────────────────────────────────────────

def _make_catalog(client_id: str, client_secret: str):
    """Return a PyIceberg REST catalog authenticated as the given OAuth client."""
    return load_catalog(
        "rest",
        **{
            "type":                  "rest",
            "uri":                   GATEWAY_URL,
            "oauth2-server-uri":     OAUTH_URL,
            "credential":            f"{client_id}:{client_secret}",
            "scope":                 "catalog:read catalog:write",
            "s3.endpoint":           MINIO_ENDPOINT,
            "s3.access-key-id":      MINIO_KEY,
            "s3.secret-access-key":  MINIO_SECRET,
            "s3.region":             "us-east-1",
            "s3.path-style-access":  "true",
        },
    )


def ops_catalog():
    """Catalog client with write access (ops-client)."""
    return _make_catalog("ops-client", "ops-secret")


def reader_catalog():
    """Catalog client with read-only access (analytics-client)."""
    return _make_catalog("analytics-client", "analytics-secret")


# ── Table bootstrap ───────────────────────────────────────────────────────────

def ensure_table() -> Any:
    """
    Create the injection test table if it doesn't exist.
    Returns the table object (ops-client view).
    """
    cat = ops_catalog()

    # Ensure namespace exists
    existing_ns = [ns[0] for ns in cat.list_namespaces()]
    if INJECTION_NS not in existing_ns:
        cat.create_namespace(INJECTION_NS)

    if cat.table_exists(FULL_TABLE_NAME):
        return cat.load_table(FULL_TABLE_NAME)

    table = cat.create_table(
        FULL_TABLE_NAME,
        schema=ORDERS_SCHEMA,
        partition_spec=PARTITION_SPEC,
        location=f"s3a://{MINIO_BUCKET}/iceberg/{INJECTION_NS}/{INJECTION_TABLE}",
        properties={
            "write.format.default":          "parquet",
            "write.parquet.compression-codec": "snappy",
            "description":                   "Injection test table — safe baseline.",
        },
    )
    return table


# ── Metadata poisoning ────────────────────────────────────────────────────────

def poison_table_description(payload_text: str) -> None:
    """
    Write an injection payload into the table's 'description' property.
    Simulates a rogue data engineer poisoning catalog metadata.
    """
    cat = ops_catalog()
    table = cat.load_table(FULL_TABLE_NAME)
    with table.transaction() as tx:
        tx.set_properties({"description": payload_text})


def poison_column_doc(column_name: str, payload_text: str) -> None:
    """
    Write an injection payload into a column's doc string.
    """
    cat = ops_catalog()
    table = cat.load_table(FULL_TABLE_NAME)
    with table.update_schema() as upd:
        upd.update_column(column_name, doc=payload_text)


def poison_table_property(key: str, payload_text: str) -> None:
    """
    Write an injection payload into an arbitrary table property.
    Simulates ETL pipelines writing poisoned lineage/audit metadata.
    """
    cat = ops_catalog()
    table = cat.load_table(FULL_TABLE_NAME)
    with table.transaction() as tx:
        tx.set_properties({key: payload_text})


def reset_metadata() -> None:
    """Restore table metadata to a clean baseline between test scenarios."""
    cat = ops_catalog()
    table = cat.load_table(FULL_TABLE_NAME)
    with table.transaction() as tx:
        tx.set_properties({
            "description": "Injection test table — safe baseline.",
            "etl.notes":   "partition_key=order_date | owner=data-engineering",
        })
    with table.update_schema() as upd:
        upd.update_column("comment", doc="Free-text order comment entered by clerk.")


# ── Metadata reading (RAG pipeline perspective) ───────────────────────────────

class TableMetadata:
    """Structured snapshot of the metadata visible to the RAG agent."""

    def __init__(
        self,
        table_description: str,
        column_docs: Dict[str, str],
        table_properties: Dict[str, str],
        namespace: str,
        table_name: str,
    ):
        self.table_description  = table_description
        self.column_docs        = column_docs
        self.table_properties   = table_properties
        self.namespace          = namespace
        self.table_name         = table_name

    def all_text_fields(self) -> Dict[str, str]:
        """All metadata text the RAG agent would feed into the LLM."""
        fields: Dict[str, str] = {}
        if self.table_description:
            fields["table_description"] = self.table_description
        for col, doc in self.column_docs.items():
            if doc:
                fields[f"column:{col}"] = doc
        for k, v in self.table_properties.items():
            if k not in ("write.format.default", "write.parquet.compression-codec"):
                fields[f"property:{k}"] = v
        return fields

    def to_prompt_context(self) -> str:
        """Format metadata as the context block injected into the RAG prompt."""
        lines = [
            f"Table: {self.namespace}.{self.table_name}",
            f"Description: {self.table_description}",
        ]
        if self.column_docs:
            lines.append("\nColumn documentation:")
            for col, doc in self.column_docs.items():
                lines.append(f"  {col}: {doc}")
        extras = {
            k: v for k, v in self.table_properties.items()
            if k not in ("description",
                         "write.format.default",
                         "write.parquet.compression-codec")
        }
        if extras:
            lines.append("\nAdditional properties:")
            for k, v in extras.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)


def read_metadata(as_ops: bool = False) -> Optional[TableMetadata]:
    """
    Read current table metadata through the catalog gateway.
    By default uses analytics-client (enforces OPA row/column filters).
    Pass as_ops=True to read without restrictions (ops-client view).
    """
    try:
        cat = ops_catalog() if as_ops else reader_catalog()
        table = cat.load_table(FULL_TABLE_NAME)
        table.refresh()

        props       = dict(table.metadata.properties)
        description = props.get("description", "")
        schema      = table.schema()
        col_docs    = {
            field.name: (field.doc or "")
            for field in schema.fields
            if field.doc
        }
        return TableMetadata(
            table_description=description,
            column_docs=col_docs,
            table_properties=props,
            namespace=INJECTION_NS,
            table_name=INJECTION_TABLE,
        )
    except Exception as exc:
        print(f"[catalog_ops] read_metadata failed: {exc}")
        return None
