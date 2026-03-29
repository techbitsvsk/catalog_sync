"""Pydantic models for policy service — access control, RLS, and CLS."""
from typing import Dict, List, Optional
from pydantic import BaseModel


# ── Request / Response models ─────────────────────────────────────────────────

class Resource(BaseModel):
    namespace: str
    table: Optional[str] = None


class CheckRequest(BaseModel):
    principal: str          # OAuth client_id
    resource: Resource
    operation: str          # read | write | drop | admin


class CheckResponse(BaseModel):
    allowed: bool
    principal: str
    resource: Resource
    operation: str
    matched_contract: Optional[str] = None
    reason: str


class FiltersRequest(BaseModel):
    principal: str
    namespace: str
    table: str


class FiltersResponse(BaseModel):
    """
    Row-Level Security (RLS) and Column-Level Security (CLS) response.

    Returned by GET /filters — query engines (Spark, Trino, Flink) call this
    endpoint before executing a table scan to obtain:

      row_filters         — SQL WHERE expressions (OR-combined across contracts)
      iceberg_expressions — structured Iceberg Expression dicts (engine-agnostic pushdown)
      column_masks        — column → SQL expression for PII masking
      excluded_columns    — columns that must not be projected at all
      spark_filter        — convenience: row_filters pre-joined with OR for Spark pushdown

    iceberg_expressions format (one entry per active row filter):
      {"type": "eq",  "term": "region",  "value": "EMEA"}
      {"type": "in",  "term": "country", "values": ["GB", "DE", "FR"]}
      {"type": "and", "left": {...},      "right": {...}}
      {"type": "or",  "left": {...},      "right": {...}}
      {"type": "not", "child": {...}}
      {"type": "lt" | "lte" | "gt" | "gte", "term": "...", "value": ...}
      {"type": "is_null" | "not_null",   "term": "..."}

    Consumers that support Iceberg scan planning (PyIceberg, Iceberg Java API) should
    prefer iceberg_expressions for file-level pruning via column statistics.
    Consumers without Iceberg support fall back to row_filters (SQL strings).
    """
    principal: str
    resource: Resource
    access: str                          # "granted" | "denied"
    row_filters: List[str]               # SQL WHERE clause fragments (OR-combined)
    iceberg_expressions: List[dict]      # structured Iceberg Expression per row filter
    column_masks: Dict[str, str]         # { column_name: mask_sql_expression }
    excluded_columns: List[str]          # columns to drop entirely from projection
    matched_contracts: List[str]         # IDs of contracts that contributed
    spark_filter: Optional[str]          # pre-joined row_filters for Spark pushdown
    trino_row_filter: Optional[str]      # same, Trino-style alias
    note: str


# ── Data contract sub-models (RLS / CLS) ─────────────────────────────────────

class RowFilter(BaseModel):
    """
    A row-level filter restricting which rows a principal may read.

    table:             Table name this filter applies to. Use "*" for all tables.
    filter_expression: SQL WHERE clause fragment (no WHERE keyword).
                       Applied as: SELECT ... FROM table WHERE <filter_expression>
                       Multiple filters across contracts are OR-combined.
    iceberg_expression: Optional structured Iceberg Expression dict.
                        Engine-agnostic; enables file-level pruning via column
                        statistics in Iceberg scan planning (skips Parquet files
                        entirely before row-level evaluation).
                        If omitted, only the SQL string is used.

    Expression types:
      {"type": "eq",      "term": "col",   "value": val}
      {"type": "ne",      "term": "col",   "value": val}
      {"type": "lt",      "term": "col",   "value": val}
      {"type": "lte",     "term": "col",   "value": val}
      {"type": "gt",      "term": "col",   "value": val}
      {"type": "gte",     "term": "col",   "value": val}
      {"type": "in",      "term": "col",   "values": [v1, v2, ...]}
      {"type": "not_in",  "term": "col",   "values": [v1, v2, ...]}
      {"type": "is_null", "term": "col"}
      {"type": "not_null","term": "col"}
      {"type": "and",     "left": {...},   "right": {...}}
      {"type": "or",      "left": {...},   "right": {...}}
      {"type": "not",     "child": {...}}

    Examples:
      filter_expression: "region = 'EMEA'"
      iceberg_expression: {type: eq, term: region, value: EMEA}

      filter_expression: "country IN ('GB', 'DE', 'FR')"
      iceberg_expression: {type: in, term: country, values: [GB, DE, FR]}

      filter_expression: "region = 'EMEA' AND status = 'COMPLETED'"
      iceberg_expression:
        type: and
        left:  {type: eq, term: region, value: EMEA}
        right: {type: eq, term: status, value: COMPLETED}
    """
    table: str = "*"
    filter_expression: str
    iceberg_expression: Optional[dict] = None


class ColumnMask(BaseModel):
    """
    A column-level mask transforming how a sensitive column is returned.

    The column is still projected (visible in SELECT *) but its value is
    replaced with the mask_expression result.

    table:           Table this mask applies to. Use "*" for all tables.
    column:          Exact column name to mask.
    mask_expression: SQL expression returning the masked value.
                     Use 'NULL' to return NULL.
                     Use a string constant like '***REDACTED***'.
                     Use a SQL function like CONCAT(LEFT(email,2),'****@****.com').

    Examples:
      column: email
      mask_expression: "CONCAT(LEFT(email, 2), '****@****.com')"

      column: phone
      mask_expression: "'***-***-****'"

      column: salary
      mask_expression: "NULL"
    """
    table: str = "*"
    column: str
    mask_expression: str


class ColumnExclusion(BaseModel):
    """
    Completely hides one or more columns from the principal.

    Unlike a column mask (which returns a transformed value), an exclusion
    means the column must not appear in any projection. Query engines should
    raise an error or return empty if the column is explicitly requested.

    table:   Table this exclusion applies to. Use "*" for all tables.
    columns: List of column names to exclude.
    """
    table: str = "*"
    columns: List[str]


# ── Data contract ─────────────────────────────────────────────────────────────

class DataContract(BaseModel):
    """
    A data contract defines the complete access policy for a set of principals
    on a set of catalog resources.

    Access control layers:
      Layer 1 — Coarse: namespace/table/operation allow-list  (principals × namespaces × tables × operations)
      Layer 2 — Fine:   row_filters    (RLS — which rows are visible)
      Layer 3 — Fine:   column_masks   (CLS — PII masking on specific columns)
      Layer 4 — Fine:   column_exclusions (CLS — completely hidden columns)

    Layers 2-4 are optional. If absent, no RLS/CLS restrictions apply beyond
    the coarse-grained allow-list in layer 1.
    """
    id: str
    name: str
    description: str = ""

    # Layer 1: coarse-grained access control
    principals: List[str]    # OAuth client_ids; "*" = any principal
    namespaces: List[str]    # Iceberg namespace names; "*" = any
    tables: List[str]        # Table names; "*" = any; fnmatch patterns supported
    operations: List[str]    # read | write | drop | admin; "*" = any

    # Layer 2: Row-Level Security
    row_filters: List[RowFilter] = []
    """
    SQL WHERE expressions applied at query time to restrict visible rows.
    All filters from all matching contracts are OR-combined — a principal
    with two row-filter contracts sees the union of their allowed rows.
    """

    # Layer 3: Column masking (CLS — transform)
    column_masks: List[ColumnMask] = []
    """
    SQL mask expressions for PII columns. The column remains in the schema
    but its value is replaced. When multiple contracts mask the same column,
    the first matching contract's mask wins (most-restrictive-first ordering).
    """

    # Layer 4: Column exclusions (CLS — hide)
    column_exclusions: List[ColumnExclusion] = []
    """
    Columns completely hidden from this principal. Union across all matching
    contracts — if any contract excludes a column, the column is excluded.
    Takes precedence over column_masks for the same column.
    """

    enabled: bool = True


class ContractSet(BaseModel):
    version: str
    contracts: List[DataContract]
