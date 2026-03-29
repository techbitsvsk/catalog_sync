"""
policy_service/main.py — Data Contract Policy Enforcement Service

Endpoints
─────────
  POST /check                    — coarse access decision (allow/deny)
  GET  /filters                  — RLS row filters + CLS column masks for a principal+table
  GET  /contracts                — list all active contracts
  POST /contracts                — add a new contract (admin-only)
  DELETE /contracts/{id}         — remove a contract (admin-only)
  POST /reload                   — reload contracts from YAML (admin-only)
  GET  /health                   — health check

Policy model
────────────
  Layer 1 (coarse)  — principals × namespaces × tables × operations → allow/deny
  Layer 2 (RLS)     — row_filters: SQL WHERE predicates per table (OR-combined)
  Layer 3 (CLS)     — column_masks: PII masking SQL expressions per column
  Layer 4 (CLS)     — column_exclusions: columns hidden entirely from projection

Query engine integration
────────────────────────
  Spark:  call GET /filters, apply spark_filter as a DataFrame.filter() predicate,
          drop excluded_columns from the schema, apply column_masks via withColumn().

  Trino:  call GET /filters, register as a SystemAccessControl row filter and
          column masking function via Trino's access control plugin API.
"""
import logging
import os
from typing import Annotated, Optional

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from engine import PolicyEngine
from models import CheckRequest, CheckResponse, DataContract, FiltersResponse

log = logging.getLogger(__name__)

CONTRACTS_PATH = os.environ.get("CONTRACTS_PATH", "/app/contracts/default.yaml")
ADMIN_TOKEN    = os.environ.get("ADMIN_TOKEN", "admin-secret-change-me")

engine = PolicyEngine(CONTRACTS_PATH)

app = FastAPI(
    title="Catalog Policy Service",
    version="2.0.0",
    description="Data contract policy enforcement: coarse access control + RLS + CLS",
)
security = HTTPBearer(auto_error=False)


def require_admin(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]):
    if not credentials or credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")


# ── Layer 1: coarse access check ─────────────────────────────────────────────

@app.post("/check", response_model=CheckResponse)
def check_access(request: CheckRequest) -> CheckResponse:
    """
    Evaluate whether a principal may perform an operation on a catalog resource.
    Returns allow/deny + the matched contract ID.
    """
    result = engine.check(request)
    if not result.allowed:
        log.warning(
            f"DENY  {request.principal} → {request.operation} on "
            f"{request.resource.namespace}.{request.resource.table or '*'}"
        )
    else:
        log.debug(
            f"ALLOW {request.principal} → {request.operation} on "
            f"{request.resource.namespace} [{result.matched_contract}]"
        )
    return result


# ── Layers 2-4: RLS + CLS ─────────────────────────────────────────────────────

@app.get("/filters", response_model=FiltersResponse)
def get_filters(
    principal: str = Query(..., description="OAuth client_id"),
    namespace: str = Query(..., description="Iceberg namespace"),
    table: str     = Query(..., description="Iceberg table name"),
) -> FiltersResponse:
    """
    Return all Row-Level Security (RLS) and Column-Level Security (CLS)
    constraints applicable to principal reading namespace.table.

    Response fields:
      row_filters       — SQL WHERE predicates; apply with OR between each
      column_masks      — {column: SQL expression} — replace value at query time
      excluded_columns  — columns to drop from projection entirely
      spark_filter      — convenience: row_filters pre-joined with OR
      trino_row_filter  — same, Trino-style alias

    Query engine usage:
      Spark  → df.filter(spark_filter).drop(*excluded_columns)
                 .withColumn("email", expr(column_masks["email"]))
      Trino  → register trino_row_filter as SystemAccessControl.getRowFilter()
                 + register column_masks as getColumnMask()
    """
    result = engine.get_filters(principal=principal, namespace=namespace, table=table)
    if result.access == "denied":
        log.warning(f"FILTERS DENIED: {principal} → {namespace}.{table}")
    else:
        log.debug(
            f"FILTERS: {principal} → {namespace}.{table} | "
            f"{len(result.row_filters)} filters, "
            f"{len(result.column_masks)} masks, "
            f"{len(result.excluded_columns)} exclusions"
        )
    return result


# ── Contract management ───────────────────────────────────────────────────────

@app.get("/contracts")
def list_contracts():
    """List all active contracts including their RLS/CLS rules."""
    return {"contracts": [c.model_dump() for c in engine.get_contracts()]}


@app.post("/contracts", dependencies=[Depends(require_admin)], status_code=201)
def add_contract(contract: DataContract):
    """
    Add a new data contract.

    Supports all four layers:
      - principals, namespaces, tables, operations (Layer 1 — access)
      - row_filters   (Layer 2 — RLS)
      - column_masks  (Layer 3 — CLS masking)
      - column_exclusions (Layer 4 — CLS exclusions)
    """
    existing_ids = {c.id for c in engine.get_contracts()}
    if contract.id in existing_ids:
        raise HTTPException(status_code=409, detail=f"Contract '{contract.id}' already exists")
    engine.add_contract(contract)
    log.info(f"Contract added: {contract.id}")
    return {
        "status": "added",
        "contract_id": contract.id,
        "rls": len(contract.row_filters),
        "cls_masks": len(contract.column_masks),
        "cls_exclusions": sum(len(e.columns) for e in contract.column_exclusions),
    }


@app.delete("/contracts/{contract_id}", dependencies=[Depends(require_admin)])
def remove_contract(contract_id: str):
    """Remove a contract by ID."""
    removed = engine.remove_contract(contract_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Contract '{contract_id}' not found")
    log.info(f"Contract removed: {contract_id}")
    return {"status": "removed", "contract_id": contract_id}


@app.post("/reload", dependencies=[Depends(require_admin)])
def reload_contracts():
    """Reload all contracts from the YAML file. Reverts any runtime additions."""
    engine.reload()
    return {"status": "reloaded", "count": len(engine.get_contracts())}


@app.get("/health")
def health():
    contracts = engine.get_contracts()
    rls_count = sum(len(c.row_filters) for c in contracts)
    cls_mask_count = sum(len(c.column_masks) for c in contracts)
    cls_excl_count = sum(
        len(e.columns) for c in contracts for e in c.column_exclusions
    )
    return {
        "status": "ok",
        "service": "policy",
        "contracts": len(contracts),
        "rls_rules": rls_count,
        "cls_masks": cls_mask_count,
        "cls_exclusions": cls_excl_count,
    }
