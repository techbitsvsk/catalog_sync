"""Policy enforcement client — coarse access control, RLS, and CLS."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

log = logging.getLogger(__name__)


class AccessDeniedError(PermissionError):
    """Raised when a policy check denies access."""
    pass


@dataclass
class DataFilters:
    """
    Row-Level Security (RLS) and Column-Level Security (CLS) constraints
    returned by PolicyClient.get_filters().

    Use these at query time to restrict data visibility:

      Spark (SQL string fallback):
        df = spark.read.format("iceberg").load("catalog.namespace.table")
        if filters.spark_filter:
            df = df.filter(filters.spark_filter)
        if filters.excluded_columns:
            df = df.drop(*filters.excluded_columns)
        for col, expr in filters.column_masks.items():
            df = df.withColumn(col, F.expr(expr))

      PyIceberg (scan pushdown — skips Parquet files via column statistics):
        table = catalog.load_table("namespace.table")
        scan = table.scan()
        for expr in filters.to_pyiceberg_expressions():
            scan = scan.filter(expr)
        df = scan.to_arrow()

      Trino (SystemAccessControl plugin):
        getRowFilter()    → filters.trino_row_filter
        getColumnMask()   → filters.column_masks.get(column_name)
    """
    access: str                         # "granted" | "denied"
    row_filters: List[str]              # SQL WHERE predicates (OR-combined)
    iceberg_expressions: List[dict]     # structured Iceberg Expression dicts
    column_masks: Dict[str, str]        # { column: SQL mask expression }
    excluded_columns: List[str]         # columns to drop from projection
    matched_contracts: List[str]        # contract IDs that contributed
    spark_filter: Optional[str]         # row_filters pre-joined with OR
    trino_row_filter: Optional[str]     # same, Trino alias
    note: str

    @property
    def has_rls(self) -> bool:
        """True if any row filters are active."""
        return bool(self.row_filters)

    @property
    def has_cls(self) -> bool:
        """True if any column masks or exclusions are active."""
        return bool(self.column_masks or self.excluded_columns)

    @property
    def is_unrestricted(self) -> bool:
        """True if principal has full access with no RLS/CLS restrictions."""
        return self.access == "granted" and not self.has_rls and not self.has_cls

    def to_pyiceberg_expressions(self) -> list:
        """
        Convert iceberg_expressions dicts to PyIceberg Expression objects.

        Requires pyiceberg to be installed. Returns an empty list if pyiceberg
        is not available or if no iceberg_expressions are present.

        Usage:
            table = catalog.load_table("gold.orders")
            scan = table.scan()
            for expr in filters.to_pyiceberg_expressions():
                scan = scan.filter(expr)
        """
        if not self.iceberg_expressions:
            return []
        try:
            from pyiceberg import expressions as E  # noqa: N812
            return [_dict_to_pyiceberg(expr, E) for expr in self.iceberg_expressions]
        except ImportError:
            log.debug("pyiceberg not installed — iceberg_expressions not converted")
            return []


def _dict_to_pyiceberg(expr: dict, E) -> object:  # noqa: N803
    """Recursively convert an Iceberg expression dict to a PyIceberg Expression."""
    t = expr["type"]
    if t == "eq":
        return E.EqualTo(expr["term"], expr["value"])
    if t == "ne":
        return E.NotEqualTo(expr["term"], expr["value"])
    if t == "lt":
        return E.LessThan(expr["term"], expr["value"])
    if t == "lte":
        return E.LessThanOrEqual(expr["term"], expr["value"])
    if t == "gt":
        return E.GreaterThan(expr["term"], expr["value"])
    if t == "gte":
        return E.GreaterThanOrEqual(expr["term"], expr["value"])
    if t == "in":
        return E.In(expr["term"], expr["values"])
    if t == "not_in":
        return E.NotIn(expr["term"], expr["values"])
    if t == "is_null":
        return E.IsNull(expr["term"])
    if t == "not_null":
        return E.NotNull(expr["term"])
    if t == "and":
        return E.And(_dict_to_pyiceberg(expr["left"], E), _dict_to_pyiceberg(expr["right"], E))
    if t == "or":
        return E.Or(_dict_to_pyiceberg(expr["left"], E), _dict_to_pyiceberg(expr["right"], E))
    if t == "not":
        return E.Not(_dict_to_pyiceberg(expr["child"], E))
    raise ValueError(f"Unknown Iceberg expression type: {t!r}")


class PolicyClient:
    """
    Client for the catalog policy enforcement service.

    Checks data contracts before catalog operations to enforce fine-grained
    access control. Raises AccessDeniedError if the principal is not allowed
    to perform the requested operation.

    Args:
        service_url: Base URL of the policy service (e.g. http://localhost:8082)
        principal:   The OAuth client_id making the request

    Usage:
        policy = PolicyClient("http://localhost:8082", principal="analytics-client")
        policy.enforce(namespace="gold", operation="read")           # OK
        policy.enforce(namespace="gold", operation="write")          # raises AccessDeniedError
        policy.enforce(namespace="gold", table="sales", operation="read")  # OK
    """

    def __init__(self, service_url: str, principal: str):
        self._url = service_url.rstrip("/")
        self._principal = principal

    @property
    def principal(self) -> str:
        return self._principal

    def check(
        self,
        namespace: str,
        operation: str,
        table: Optional[str] = None,
    ) -> bool:
        """Return True if the principal is allowed to perform operation on namespace[.table]."""
        try:
            resp = requests.post(
                f"{self._url}/check",
                json={
                    "principal": self._principal,
                    "resource": {"namespace": namespace, "table": table},
                    "operation": operation,
                },
                timeout=5,
            )
            if resp.ok:
                return resp.json().get("allowed", False)
            log.warning(f"Policy service returned {resp.status_code}: {resp.text}")
            return False
        except requests.RequestException as e:
            log.warning(f"Policy service unreachable ({e}); defaulting to DENY")
            return False

    def enforce(
        self,
        namespace: str,
        operation: str,
        table: Optional[str] = None,
    ) -> None:
        """Like check() but raises AccessDeniedError if access is denied."""
        try:
            resp = requests.post(
                f"{self._url}/check",
                json={
                    "principal": self._principal,
                    "resource": {"namespace": namespace, "table": table},
                    "operation": operation,
                },
                timeout=5,
            )
            if resp.ok:
                result = resp.json()
                if result.get("allowed"):
                    log.debug(
                        f"Policy ALLOW: {self._principal} -> {operation} on "
                        f"{namespace}{f'.{table}' if table else ''} "
                        f"[{result.get('matched_contract')}]"
                    )
                    return
                raise AccessDeniedError(result.get("reason", "Access denied by policy"))
            raise AccessDeniedError(
                f"Policy service error ({resp.status_code}): {resp.text}"
            )
        except requests.RequestException as e:
            raise AccessDeniedError(f"Policy service unreachable: {e}") from e

    def get_filters(self, namespace: str, table: str) -> DataFilters:
        """
        Fetch Row-Level Security (RLS) and Column-Level Security (CLS) constraints
        for self.principal reading namespace.table.

        Returns a DataFilters object describing:
          - row_filters:       SQL WHERE predicates to apply (OR-combined)
          - column_masks:      PII masking expressions per column
          - excluded_columns:  columns to drop entirely from projection
          - spark_filter:      convenience pre-joined row filter for Spark
          - trino_row_filter:  same, Trino alias

        Usage:
            filters = policy.get_filters("gold", "customers")
            if filters.has_rls:
                df = df.filter(filters.spark_filter)
            df = df.drop(*filters.excluded_columns)
            for col, expr in filters.column_masks.items():
                df = df.withColumn(col, F.expr(expr))

        Fails closed: if the policy service is unreachable, returns access=denied.
        """
        try:
            resp = requests.get(
                f"{self._url}/filters",
                params={
                    "principal": self._principal,
                    "namespace": namespace,
                    "table": table,
                },
                timeout=5,
            )
            if resp.ok:
                data = resp.json()
                filters = DataFilters(
                    access=data.get("access", "denied"),
                    row_filters=data.get("row_filters", []),
                    iceberg_expressions=data.get("iceberg_expressions", []),
                    column_masks=data.get("column_masks", {}),
                    excluded_columns=data.get("excluded_columns", []),
                    matched_contracts=data.get("matched_contracts", []),
                    spark_filter=data.get("spark_filter"),
                    trino_row_filter=data.get("trino_row_filter"),
                    note=data.get("note", ""),
                )
                log.debug(
                    f"Filters for {self._principal} on {namespace}.{table}: "
                    f"RLS={filters.has_rls}, CLS={filters.has_cls}, "
                    f"note={filters.note}"
                )
                return filters
            log.warning(f"Policy service /filters returned {resp.status_code}: {resp.text}")
            return DataFilters(
                access="denied", row_filters=[], iceberg_expressions=[],
                column_masks={}, excluded_columns=[], matched_contracts=[],
                spark_filter=None, trino_row_filter=None,
                note=f"Policy service error: {resp.status_code}",
            )
        except requests.RequestException as e:
            log.warning(f"Policy service unreachable for /filters ({e}); defaulting to DENY")
            return DataFilters(
                access="denied", row_filters=[], iceberg_expressions=[],
                column_masks={}, excluded_columns=[], matched_contracts=[],
                spark_filter=None, trino_row_filter=None,
                note=f"Policy service unreachable: {e}",
            )
