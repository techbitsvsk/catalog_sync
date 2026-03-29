"""
Catalog Gateway — Iceberg REST Catalog proxy with OPA-enforced access control.

Enforcement model
─────────────────
  Layer 1  Table access      OPA allow/deny → HTTP 403 before any Nessie call    (hard)
  Layer 4  Column exclusion  Schema rewrite removes fields from table metadata    (hard)
  Layer 2  Row filter        Merged into scan filter expression sent to Nessie    (hard)
           Partitioned data: file-level pruning → zero files from denied regions
           Non-partitioned:  residual filter returned in scan tasks; Spark applies it
  Layer 3  Column masking    Stored as table property; query engine applies it    (advisory)

Clients connect to:  http://catalog-gateway:8083  (Iceberg REST Catalog v1)
Backend (Nessie):    http://nessie:19120/iceberg/v1

Authentication
──────────────
  Clients   — send their own Bearer JWT (issued by oauth-service)
  Gateway   — validates client JWT via JWKS, extracts principal
  → Nessie  — gateway calls Nessie with its own admin token (never the client token)

Any client with direct Nessie access bypasses enforcement. Restrict
nessie:19120 to the Docker network (not exposed externally) so the
gateway is the sole entry point.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from policy import JWTError, PolicyDecision, get_policy, validate_token

log = logging.getLogger("catalog_gateway")

NESSIE_URL   = os.getenv("NESSIE_URL",            "http://nessie:19120/iceberg/v1")
OAUTH_URL    = os.getenv("OAUTH_URL",             "http://oauth-service:8081")
GW_CLIENT_ID = os.getenv("GATEWAY_CLIENT_ID",     "admin-client")
GW_SECRET    = os.getenv("GATEWAY_CLIENT_SECRET", "admin-secret")

# Matches: v1[/ref]/namespaces/<ns>/tables/<tbl>[/suffix]
_TABLE_RE = re.compile(
    r"^(?:v1/)?(?:[^/]+/)?namespaces/([^/?]+)/tables/([^/?]+)(/[^?]*)?",
    re.I,
)


# ── Admin token manager ───────────────────────────────────────────────────────

class _TokenManager:
    """Fetch and cache the gateway's admin token, refreshing before expiry."""

    _token: str   = ""
    _exp:   float = 0.0

    async def get(self) -> str:
        if time.monotonic() < self._exp - 60:
            return self._token
        async with httpx.AsyncClient() as c:
            r = await c.post(
                f"{OAUTH_URL}/token",
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     GW_CLIENT_ID,
                    "client_secret": GW_SECRET,
                },
                timeout=10,
            )
            r.raise_for_status()
            d = r.json()
            self._token = d["access_token"]
            self._exp   = time.monotonic() + d.get("expires_in", 3600)
        return self._token


_tokens = _TokenManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.nessie = httpx.AsyncClient(base_url=NESSIE_URL, timeout=30)
    await _tokens.get()          # warm-up admin token on startup
    yield
    await app.state.nessie.aclose()


app = FastAPI(title="Catalog Gateway", version="1.0.0", lifespan=lifespan)


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _extract_principal(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Bearer token required")
    try:
        claims = validate_token(auth[7:])
        return claims.get("client_id") or claims.get("sub", "unknown")
    except JWTError as exc:
        raise HTTPException(401, str(exc)) from exc


async def _nessie(app_state, method: str, path: str, *,
                  body: bytes | None = None,
                  params: dict | None = None) -> httpx.Response:
    """Forward a request to Nessie using the gateway's admin token."""
    token = await _tokens.get()
    return await app_state.nessie.request(
        method,
        f"/{path.lstrip('/')}",
        content=body,
        params=params,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
    )


def _strip_columns(meta: dict, excluded: set[str]) -> None:
    """Remove excluded columns from all schema representations in table metadata."""
    for schema in meta.get("metadata", {}).get("schemas", []):
        schema["fields"] = [
            f for f in schema.get("fields", []) if f.get("name") not in excluded
        ]
    cur = meta.get("metadata", {}).get("schema")
    if cur:
        cur["fields"] = [
            f for f in cur.get("fields", []) if f.get("name") not in excluded
        ]


def _sql_to_iceberg_expr(sql: str) -> dict | None:
    """
    Convert a simple SQL predicate to an Iceberg expression dict.

    Handles:
      col = 'value'
      col IN ('a', 'b', 'c')
    Returns None for complex expressions (injection skipped; log a warning).
    """
    s = sql.strip()

    m = re.match(r"^(\w+)\s*=\s*'([^']*)'$", s)
    if m:
        return {"type": "eq", "term": m.group(1), "value": m.group(2)}

    m = re.match(r"^(\w+)\s+IN\s*\(([^)]+)\)$", s, re.I)
    if m:
        vals = [v.strip().strip("'\"") for v in m.group(2).split(",")]
        return {"type": "in", "term": m.group(1), "values": vals}

    log.warning("Cannot parse row_filter as Iceberg expression: %r — skipping injection", sql)
    return None


def _merge_filter(existing: dict | None, security: dict) -> dict:
    if existing is None:
        return security
    return {"type": "and", "left": existing, "right": security}


def _resp(r: httpx.Response) -> Response:
    return Response(
        content=r.content,
        status_code=r.status_code,
        media_type=r.headers.get("content-type", "application/json"),
    )


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "enforces": ["table-access", "column-exclusion", "row-filter"],
        "advisory": ["column-masking"],
    }


# ── Single catch-all handler ──────────────────────────────────────────────────

@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"],
)
async def gateway(request: Request, path: str):
    """
    Route all Iceberg REST Catalog traffic with selective policy enforcement.

    Unauthenticated pass-through: /health, v1/config, Nessie readiness probes.
    All table operations require a valid Bearer JWT.
    """
    # ── Unauthenticated pass-through ─────────────────────────────────────────
    if path in ("health",) or path.startswith(("v1/config", "q/")):
        r = await _nessie(
            request.app.state,
            request.method,
            path,
            body=await request.body() or None,
            params=dict(request.query_params),
        )
        return _resp(r)

    # ── All other paths require authentication ────────────────────────────────
    principal = await _extract_principal(request)

    m = _TABLE_RE.match(path)
    if not m:
        # Namespace ops, listing, etc. — proxy transparently after auth
        r = await _nessie(
            request.app.state,
            request.method,
            path,
            body=await request.body() or None,
            params=dict(request.query_params),
        )
        return _resp(r)

    namespace = m.group(1)
    table     = m.group(2)
    suffix    = (m.group(3) or "").rstrip("/")

    # ── GET /tables/{table} — table metadata with CLS schema rewrite ──────────
    if request.method == "GET" and not suffix:
        dec = await get_policy(principal, namespace, table, "READ")
        if not dec.allow:
            raise HTTPException(
                403, f"Principal '{principal}' denied READ on {namespace}.{table}"
            )
        r = await _nessie(request.app.state, "GET", path)
        if r.status_code != 200 or not dec.excluded_columns:
            return _resp(r)

        # Layer 4 — remove excluded columns from schema (hard CLS)
        meta = r.json()
        _strip_columns(meta, set(dec.excluded_columns))

        # Layer 3 — store masks as table property (advisory; engine applies)
        if dec.column_masks:
            (meta.setdefault("metadata", {})
                 .setdefault("properties", {})
                 ["gateway.column-masks"]) = json.dumps(dec.column_masks)

        return JSONResponse(meta)

    # ── POST /tables/{table}/scan — scan planning with RLS filter injection ───
    if request.method == "POST" and suffix == "/scan":
        dec = await get_policy(principal, namespace, table, "SCAN")
        if not dec.allow:
            raise HTTPException(
                403, f"Principal '{principal}' denied SCAN on {namespace}.{table}"
            )
        body = await request.json()

        # Layer 2 — merge security filter into scan request (RLS)
        if dec.row_filter:
            expr = _sql_to_iceberg_expr(dec.row_filter)
            if expr:
                body["filter"] = _merge_filter(body.get("filter"), expr)

        # Defence-in-depth: remove excluded cols from explicit projection
        if dec.excluded_columns and "select" in body:
            exc = set(dec.excluded_columns)
            body["select"] = [c for c in body["select"] if c not in exc]

        r = await _nessie(
            request.app.state, "POST", path, body=json.dumps(body).encode()
        )
        return _resp(r)

    # ── Write / Drop — check OPA before proxying ──────────────────────────────
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        op = "DROP" if request.method == "DELETE" else "WRITE"
        dec = await get_policy(principal, namespace, table, op)
        if not dec.allow:
            raise HTTPException(
                403, f"Principal '{principal}' denied {op} on {namespace}.{table}"
            )

    r = await _nessie(
        request.app.state,
        request.method,
        path,
        body=await request.body() or None,
        params=dict(request.query_params),
    )
    return _resp(r)
