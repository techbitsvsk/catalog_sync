"""
Policy evaluation engine.

Evaluates data contracts for:
  - Coarse access decisions  (check)         — allow/deny namespace.table.operation
  - Row-Level Security       (get_filters)   — SQL WHERE predicates for row visibility
  - Column-Level Security    (get_filters)   — column masks + excluded columns
"""
import fnmatch
import logging
from typing import Dict, List, Optional

import yaml

from models import (
    CheckRequest, CheckResponse, ContractSet, DataContract,
    FiltersResponse, Resource,
)

log = logging.getLogger(__name__)


class PolicyEngine:
    def __init__(self, contracts_path: str):
        self._contracts_path = contracts_path
        self._contracts: List[DataContract] = []
        self.reload()

    def reload(self):
        """Reload contracts from YAML file."""
        with open(self._contracts_path) as f:
            raw = yaml.safe_load(f)
        cs = ContractSet(**raw)
        self._contracts = [c for c in cs.contracts if c.enabled]
        log.info(
            f"Loaded {len(self._contracts)} active contracts from {self._contracts_path}"
        )

    # ── Layer 1: coarse access check ─────────────────────────────────────────

    def check(self, request: CheckRequest) -> CheckResponse:
        """
        Evaluate whether a principal may perform an operation on a resource.
        Returns allow/deny — first matching contract wins.
        """
        for contract in self._contracts:
            if self._matches(request, contract):
                return CheckResponse(
                    allowed=True,
                    principal=request.principal,
                    resource=request.resource,
                    operation=request.operation,
                    matched_contract=contract.id,
                    reason=f"Allowed by contract '{contract.name}'",
                )
        return CheckResponse(
            allowed=False,
            principal=request.principal,
            resource=request.resource,
            operation=request.operation,
            reason=(
                f"No contract grants '{request.principal}' permission to "
                f"'{request.operation}' on {request.resource.namespace}"
                + (f".{request.resource.table}" if request.resource.table else "")
            ),
        )

    # ── Layers 2-4: RLS and CLS ───────────────────────────────────────────────

    def get_filters(
        self,
        principal: str,
        namespace: str,
        table: str,
    ) -> FiltersResponse:
        """
        Collect all Row-Level Security (RLS) and Column-Level Security (CLS)
        constraints that apply to principal reading namespace.table.

        Evaluation rules:
          - Row filters:       OR-combined across all matching contracts.
                               Principal sees rows satisfying ANY of their filters.
                               No row_filters = unrestricted row access.
          - Column masks:      First matching contract's mask per column wins.
                               (contracts are ordered; put most-restrictive first)
          - Column exclusions: Union across all matching contracts.
                               If ANY contract excludes a column, it is excluded.
                               Exclusions take precedence over masks.
        """
        read_req = CheckRequest(
            principal=principal,
            resource=Resource(namespace=namespace, table=table),
            operation="read",
        )
        matching = [c for c in self._contracts if self._matches(read_req, c)]

        if not matching:
            return FiltersResponse(
                principal=principal,
                resource=Resource(namespace=namespace, table=table),
                access="denied",
                row_filters=[],
                iceberg_expressions=[],
                column_masks={},
                excluded_columns=[],
                matched_contracts=[],
                spark_filter=None,
                trino_row_filter=None,
                note="Access denied — no contract grants read permission",
            )

        # ── Collect RLS row filters (OR-combined) ─────────────────────────
        row_filters: List[str] = []
        iceberg_expressions: List[dict] = []
        for contract in matching:
            for rf in contract.row_filters:
                if self._match_list(table, [rf.table]):
                    if rf.filter_expression not in row_filters:
                        row_filters.append(rf.filter_expression)
                        if rf.iceberg_expression:
                            iceberg_expressions.append(rf.iceberg_expression)

        # ── Collect CLS column masks (first match per column wins) ────────
        column_masks: Dict[str, str] = {}
        for contract in matching:
            for cm in contract.column_masks:
                if self._match_list(table, [cm.table]):
                    if cm.column not in column_masks:   # first contract wins
                        column_masks[cm.column] = cm.mask_expression

        # ── Collect CLS column exclusions (union) ─────────────────────────
        excluded_set: set = set()
        for contract in matching:
            for ce in contract.column_exclusions:
                if self._match_list(table, [ce.table]):
                    excluded_set.update(ce.columns)

        # Exclusions override masks — remove masked columns that are also excluded
        for col in excluded_set:
            column_masks.pop(col, None)

        # ── Build convenience filter strings ──────────────────────────────
        spark_filter = (
            " OR ".join(f"({f})" for f in row_filters) if row_filters else None
        )

        excluded_columns = sorted(excluded_set)
        mask_count = len(column_masks)
        excl_count = len(excluded_columns)
        filter_count = len(row_filters)

        note_parts = [f"{len(matching)} contract(s) matched"]
        if filter_count:
            note_parts.append(f"{filter_count} row filter(s) (OR-combined)")
        else:
            note_parts.append("no row filters (unrestricted row access)")
        if mask_count:
            note_parts.append(f"{mask_count} column mask(s)")
        if excl_count:
            note_parts.append(f"{excl_count} excluded column(s)")

        return FiltersResponse(
            principal=principal,
            resource=Resource(namespace=namespace, table=table),
            access="granted",
            row_filters=row_filters,
            iceberg_expressions=iceberg_expressions,
            column_masks=column_masks,
            excluded_columns=excluded_columns,
            matched_contracts=[c.id for c in matching],
            spark_filter=spark_filter,
            trino_row_filter=spark_filter,
            note="; ".join(note_parts),
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _matches(self, req: CheckRequest, contract: DataContract) -> bool:
        return (
            self._match_list(req.principal, contract.principals)
            and self._match_list(req.resource.namespace, contract.namespaces)
            and self._match_list(req.resource.table or "*", contract.tables)
            and self._match_list(req.operation, contract.operations)
        )

    @staticmethod
    def _match_list(value: str, patterns: List[str]) -> bool:
        """Return True if value matches any pattern (fnmatch; '*' = any)."""
        return any(
            p == "*" or fnmatch.fnmatch(value, p)
            for p in patterns
        )

    # ── Contract management ───────────────────────────────────────────────────

    def get_contracts(self) -> List[DataContract]:
        return list(self._contracts)

    def add_contract(self, contract: DataContract) -> None:
        self._contracts.append(contract)
        self._persist()

    def remove_contract(self, contract_id: str) -> bool:
        """Remove a contract by ID. Returns True if found and removed."""
        before = len(self._contracts)
        self._contracts = [c for c in self._contracts if c.id != contract_id]
        if len(self._contracts) < before:
            self._persist()
            return True
        return False

    def _persist(self):
        """Write current in-memory contracts back to YAML."""
        data = {
            "version": "1.0",
            "contracts": [c.model_dump() for c in self._contracts],
        }
        with open(self._contracts_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
