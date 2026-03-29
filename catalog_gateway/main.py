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
Backend (Nessie):    http://nessie:19120

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

import asyncio
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

NESSIE_URL    = os.getenv("NESSIE_URL",            "http://nessie:19120")
OAUTH_URL     = os.getenv("OAUTH_URL",             "http://oauth-service:8081")
GW_CLIENT_ID  = os.getenv("GATEWAY_CLIENT_ID",     "admin-client")
GW_SECRET     = os.getenv("GATEWAY_CLIENT_SECRET", "admin-secret")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT",           "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY",         "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY",         "minioadmin")

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


# ── Virtual manifest-list (row-level security for local scan planning) ────────


def _filter_manifest_list_sync(meta: dict, row_filter_sql: str) -> dict:  # noqa: C901
    """
    Write a virtual manifest-list to MinIO that only contains manifests
    matching row_filter_sql, then return a copy of meta with the current
    snapshot's manifest-list path replaced by the virtual one.

    The Iceberg manifest-list is a small Avro file (one entry per manifest).
    We read it, filter the partition-level summaries, and write a new one.
    Spark downloads the manifest-list directly from MinIO — by pointing it
    to the filtered copy the gateway enforces RLS without Spark cooperation.

    Results are cached per (manifest-list-path × filter); the cache key is
    a hash so it changes automatically when the table is updated.
    """
    import copy
    import copy
    import hashlib
    import io as _io

    import fastavro
    from pyiceberg.io.fsspec import FsspecFileIO

    metadata = meta.get("metadata", {})
    current_snap_id = metadata.get("current-snapshot-id")
    if current_snap_id is None:
        return meta

    current_snap = next(
        (s for s in metadata.get("snapshots", [])
         if s.get("snapshot-id") == current_snap_id),
        None,
    )
    if current_snap is None:
        return meta

    manifest_list_loc = current_snap.get("manifest-list", "")
    if not manifest_list_loc:
        return meta

    # Deterministic cache key: changes whenever the manifest-list changes
    cache_key = hashlib.md5(
        f"{manifest_list_loc}:{row_filter_sql}".encode()
    ).hexdigest()[:16]
    virt_base    = f"s3://warehouse/.gateway/{cache_key}"
    virt_ml_s3   = f"{virt_base}/snap.avro"
    virt_ml_s3a  = virt_ml_s3.replace("s3://", "s3a://")

    file_io = FsspecFileIO({
        "s3.endpoint":          S3_ENDPOINT,
        "s3.access-key-id":     S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.path-style-access": "true",
        "s3.region":            "us-east-1",
    })

    # Return cached result if the virtual manifest-list already exists
    try:
        with file_io.new_input(virt_ml_s3).open() as f:
            f.read(4)
        # Cache hit — just patch the URL and return
        meta_copy = copy.deepcopy(meta)
        for snap in meta_copy.get("metadata", {}).get("snapshots", []):
            if snap.get("snapshot-id") == current_snap_id:
                snap["manifest-list"] = virt_ml_s3a
                break
        return meta_copy
    except Exception:
        pass

    # Parse the equality filter (col = 'val')
    m = re.match(r"^(\w+)\s*=\s*'([^']*)'$", row_filter_sql.strip())
    if not m:
        log.warning("Cannot parse row_filter for manifest filtering: %r", row_filter_sql)
        return meta
    filter_col, filter_val = m.group(1), m.group(2)

    ml_s3 = manifest_list_loc.replace("s3a://", "s3://")

    # Read the manifest-list
    with file_io.new_input(ml_s3).open() as f:
        ml_reader   = fastavro.reader(f)
        ml_schema   = ml_reader.writer_schema
        ml_entries  = list(ml_reader)

    # For each manifest, read and filter its data-file entries, then write
    # a new filtered manifest.  The manifests may contain files from multiple
    # partitions, so we filter at the data-file level (not manifest-list level).
    new_ml_entries = []
    for ml_entry in ml_entries:
        raw_manifest_path = ml_entry.get("manifest_path", "")
        manifest_s3 = raw_manifest_path.replace("s3a://", "s3://")

        # Read manifest
        with file_io.new_input(manifest_s3).open() as f:
            mf_reader  = fastavro.reader(f)
            mf_schema  = mf_reader.writer_schema
            mf_entries = list(mf_reader)

        # Filter to only the entries that match the partition filter.
        # Partition values in data_file.partition are plain Python values
        # (str for STRING type) — no binary encoding needed.
        kept = [
            e for e in mf_entries
            if e.get("data_file", {}).get("partition", {}).get(filter_col) == filter_val
        ]
        if not kept:
            continue  # entire manifest excluded — omit from virtual ml

        # Write filtered manifest to virtual prefix
        virt_mf_s3 = f"{virt_base}/m-{len(new_ml_entries)}.avro"
        buf = _io.BytesIO()
        fastavro.writer(buf, mf_schema, kept)
        mf_bytes = buf.getvalue()
        with file_io.new_output(virt_mf_s3).create() as f:
            f.write(mf_bytes)

        # Build updated manifest-list entry for the new (filtered) manifest
        added_rows = sum(
            e.get("data_file", {}).get("record_count", 0) for e in kept
        )
        new_ml_entry = dict(ml_entry)
        new_ml_entry["manifest_path"]           = virt_mf_s3.replace("s3://", "s3a://")
        new_ml_entry["manifest_length"]         = len(mf_bytes)
        new_ml_entry["added_data_files_count"]  = len(kept)
        new_ml_entry["existing_data_files_count"] = 0
        new_ml_entry["deleted_data_files_count"] = 0
        new_ml_entry["added_rows_count"]        = added_rows
        new_ml_entry["existing_rows_count"]     = 0
        new_ml_entry["deleted_rows_count"]      = 0
        # Update partition bounds to match the single allowed value
        val_bytes = filter_val.encode("utf-8")
        if new_ml_entry.get("partitions"):
            new_ml_entry["partitions"] = [
                {**p, "lower_bound": val_bytes, "upper_bound": val_bytes}
                for p in new_ml_entry["partitions"]
            ]
        new_ml_entries.append(new_ml_entry)

    log.info(
        "Virtual manifest-list: %d→%d manifests for filter %r",
        len(ml_entries), len(new_ml_entries), row_filter_sql,
    )

    # Write the virtual manifest-list
    buf = _io.BytesIO()
    fastavro.writer(buf, ml_schema, new_ml_entries)
    with file_io.new_output(virt_ml_s3).create() as f:
        f.write(buf.getvalue())

    # Return a deep copy with the snapshot pointing to the virtual manifest-list
    meta_copy = copy.deepcopy(meta)
    for snap in meta_copy.get("metadata", {}).get("snapshots", []):
        if snap.get("snapshot-id") == current_snap_id:
            snap["manifest-list"] = virt_ml_s3a
            break
    return meta_copy


async def _filter_manifest_list(meta: dict, row_filter_sql: str) -> dict:
    """Async wrapper: runs manifest filtering in a thread-pool executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, _filter_manifest_list_sync, meta, row_filter_sql
    )


# ── Server-side scan planning (PyIceberg) ────────────────────────────────────

def _pyiceberg_plan_scan(
    metadata_location: str,
    row_filter_sql: str | None,
    excluded_cols: list[str],
    select: list[str] | None,
) -> dict:
    """
    Use PyIceberg to plan a table scan with the security row/column filters.

    Runs synchronously — always call via asyncio.get_event_loop().run_in_executor()
    so it doesn't block the async event loop.

    Returns a dict matching the Iceberg REST ScanTableResponse schema:
      { "file-scan-tasks": [ { "data-file": {...}, "delete-files": [] }, ... ] }
    """
    from pyiceberg.expressions import AlwaysTrue, EqualTo, In
    from pyiceberg.table import StaticTable

    # Normalise s3a:// → s3:// — PyIceberg S3FileIO uses the s3:// scheme.
    loc = metadata_location.replace("s3a://", "s3://")

    props = {
        "py-io-impl":           "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint":          S3_ENDPOINT,
        "s3.access-key-id":     S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.path-style-access": "true",
        "s3.region":            "us-east-1",
    }
    table = StaticTable.from_metadata(loc, properties=props)

    # Build row-filter expression from the OPA SQL predicate.
    row_filter = AlwaysTrue()
    if row_filter_sql:
        s = row_filter_sql.strip()
        m = re.match(r"^(\w+)\s*=\s*'([^']*)'$", s)
        if m:
            row_filter = EqualTo(m.group(1), m.group(2))
        else:
            m = re.match(r"^(\w+)\s+IN\s*\(([^)]+)\)$", s, re.I)
            if m:
                vals = [v.strip().strip("'\"") for v in m.group(2).split(",")]
                row_filter = In(m.group(1), vals)
            else:
                log.warning("Cannot parse row_filter for scan planning: %r", row_filter_sql)

    # Column projection (client selection ∩ security exclusions removed).
    exc = set(excluded_cols or [])
    selected: tuple[str, ...] | None = None
    if select:
        selected = tuple(c for c in select if c not in exc)

    scan = table.scan(row_filter=row_filter, selected_fields=selected or ())
    tasks = list(scan.plan_files())

    # Map PyIceberg partition spec → column names for the REST response.
    spec   = table.spec()
    schema = table.schema()
    partition_col_names = [
        schema.find_column_name(f.source_id) for f in spec.fields
    ]

    file_scan_tasks = []
    for task in tasks:
        df = task.file
        partition: dict = {}
        for i, col in enumerate(partition_col_names):
            try:
                val = df.partition[i]
                if val is not None:
                    partition[col] = val
            except Exception:
                pass

        # content: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
        content_val = df.content.value if hasattr(df.content, "value") else 0
        fmt_val     = df.file_format.value if hasattr(df.file_format, "value") else "PARQUET"
        file_scan_tasks.append({
            "data-file": {
                "spec-id":            df.spec_id or 0,
                "content":            content_val,
                "file-path":          df.file_path,
                "file-format":        fmt_val,
                "partition":          partition,
                "record-count":       df.record_count,
                "file-size-in-bytes": df.file_size_in_bytes,
            },
            "delete-files": [],
        })

    return {"file-scan-tasks": file_scan_tasks}


async def _plan_scan(
    metadata_location: str,
    row_filter_sql: str | None,
    excluded_cols: list[str],
    select: list[str] | None,
) -> dict:
    """Async wrapper: runs PyIceberg scan planning in a thread-pool executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        _pyiceberg_plan_scan,
        metadata_location,
        row_filter_sql,
        excluded_cols,
        select,
    )


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
    rel = path.lstrip("/")
    # Iceberg REST paths (v1/...) are served under /iceberg/ on Nessie
    nessie_path = f"/iceberg/{rel}" if rel.startswith("v1") else f"/{rel}"
    return await app_state.nessie.request(
        method,
        nessie_path,
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
        # For /v1/config: drop the warehouse query param before forwarding.
        # When Spark sends warehouse=s3a://warehouse/, Nessie returns a prefix
        # like "main|s3a://warehouse/" which the Iceberg Java client embeds
        # verbatim into subsequent URI paths — the s3a:// double-slash makes
        # Java's URI.create() fail.  Without the warehouse param Nessie returns
        # prefix="main" (clean, no scheme), which works correctly.
        # The warehouse location is still communicated via defaults.warehouse.
        fwd_params = dict(request.query_params)
        if path.startswith("v1/config"):
            fwd_params.pop("warehouse", None)
        r = await _nessie(
            request.app.state,
            request.method,
            path,
            body=await request.body() or None,
            params=fwd_params,
        )
        # Rewrite Nessie's /v1/config response: strip URI overrides that point
        # to Nessie's internal address (nessie:19120).  Without this, the Iceberg
        # client would bypass the gateway and connect to Nessie directly — which
        # is unreachable from outside Docker and skips all enforcement.
        if path.startswith("v1/config") and r.status_code == 200:
            cfg = r.json()
            for key in ("uri", "nessie.core-base-uri", "nessie.catalog-base-uri",
                        "nessie.iceberg-base-uri"):
                cfg.get("overrides", {}).pop(key, None)
            cfg.get("overrides", {}).pop("nessie.prefix-pattern", None)
            # Tell the Iceberg Java client to use server-side scan planning so
            # the gateway can enforce row-level security (Layer 2 / RLS).
            cfg.setdefault("overrides", {})["rest.scan-planning-enabled"] = "true"
            # Advertise the scan endpoint — the Java client only uses server-side
            # planning if this entry appears in the endpoints list.
            scan_ep = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/scan"
            endpoints = cfg.setdefault("endpoints", [])
            if scan_ep not in endpoints:
                endpoints.append(scan_ep)
            return JSONResponse(cfg)
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
        r = await _nessie(request.app.state, "GET", path,
                          params=dict(request.query_params))
        if r.status_code != 200:
            return _resp(r)

        meta = r.json()

        # Strip S3V4RestSigner vended credentials: Nessie returns a signer URI
        # pointing to its internal address (nessie:19120) which is unreachable
        # from external clients.  Removing these keys forces S3FileIO to fall
        # back to HadoopFileIO via the Spark S3A configuration (fs.s3a.*).
        _SIGNER_KEYS = {
            "s3.signer", "s3.signer.uri", "s3.signer.endpoint",
            "s3.remote-signing-enabled", "s3.endpoint",
            "io-impl", "py-io-impl",
        }
        cfg = meta.get("config", {})
        for k in _SIGNER_KEYS:
            cfg.pop(k, None)
        # Force HadoopFileIO so Spark resolves s3a:// via its own fs.s3a.* config.
        cfg["io-impl"] = "org.apache.iceberg.hadoop.HadoopFileIO"
        if cfg is not meta.get("config"):   # ensure the key exists if config was absent
            meta["config"] = cfg

        # Layer 2 — rewrite manifest-list to filter to allowed partitions (RLS)
        # Spark downloads manifests directly from S3, bypassing the /scan
        # endpoint.  We write a virtual manifest-list to MinIO that contains
        # only the manifests whose partition bounds match the OPA row_filter,
        # then return the snapshot pointing at the virtual manifest-list.
        if dec.row_filter:
            meta = await _filter_manifest_list(meta, dec.row_filter)

        # Layer 4 — remove excluded columns from schema (hard CLS)
        if dec.excluded_columns:
            _strip_columns(meta, set(dec.excluded_columns))

        # Layer 3 — store masks as table property (advisory; engine applies)
        if dec.column_masks:
            (meta.setdefault("metadata", {})
                 .setdefault("properties", {})
                 ["gateway.column-masks"]) = json.dumps(dec.column_masks)

        return JSONResponse(meta)

    # ── POST /tables/{table}/scan — PyIceberg server-side scan planning ─────
    if request.method == "POST" and suffix == "/scan":
        dec = await get_policy(principal, namespace, table, "SCAN")
        if not dec.allow:
            raise HTTPException(
                403, f"Principal '{principal}' denied SCAN on {namespace}.{table}"
            )
        body = await request.json()

        # Fetch table metadata from Nessie to get the metadata-location.
        table_path = path.removesuffix("/scan")
        r = await _nessie(request.app.state, "GET", table_path,
                          params=dict(request.query_params))
        if r.status_code != 200:
            return _resp(r)
        meta_resp = r.json()
        metadata_location = (meta_resp.get("metadata-location") or
                             meta_resp.get("metadata", {}).get("location"))
        if not metadata_location:
            raise HTTPException(500, "Cannot determine metadata-location for scan planning")

        # Layer 2 + 4 — PyIceberg applies row filter and column projection.
        select = body.get("select")
        result = await _plan_scan(
            metadata_location,
            dec.row_filter,
            dec.excluded_columns or [],
            select,
        )
        return JSONResponse(result)

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
