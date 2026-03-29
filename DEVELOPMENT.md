# Developer Guide — iceberg-catalog-sync

This guide covers the internal architecture, all modules, the auth/policy stack,
and a complete step-by-step end-to-end walkthrough that tests every layer including
fine-grained access control enforcement.

---

## Table of Contents

1. [Why This Tool Exists](#1-why-this-tool-exists)
2. [Project Layout](#2-project-layout)
3. [Architecture Overview](#3-architecture-overview)
4. [Module Reference](#4-module-reference)
   - [4.1 PathTranslator](#41-pathtranslator)
   - [4.2 StorageBackend](#42-storagebackend)
   - [4.3 CatalogSync](#43-catalogsync)
   - [4.4 MetadataReader](#44-metadatareader)
   - [4.5 MetadataRewriter](#45-metadatarewriter)
   - [4.6 NessieCatalog](#46-nessiecatalog)
   - [4.7 OAuthClient](#47-oauthclient)
   - [4.8 PolicyClient](#48-policyclient)
   - [4.9 CLI](#49-cli)
   - [4.10 Airflow Operators](#410-airflow-operators)
5. [Full Data Flow](#5-full-data-flow)
6. [Local Development Setup](#6-local-development-setup)
7. [Running Tests](#7-running-tests)
8. [End-to-End Test Walkthrough](#8-end-to-end-test-walkthrough)
   - [8.1 Start the Stack](#81-start-the-stack)
   - [8.2 Verify Services](#82-verify-services)
   - [8.3 Seed Test Data in MinIO](#83-seed-test-data-in-minio)
   - [8.4 Test OAuth Token Issuance](#84-test-oauth-token-issuance)
   - [8.5 Test Nessie Authentication](#85-test-nessie-authentication)
   - [8.6 Test Fine-Grained Access Control](#86-test-fine-grained-access-control)
   - [8.7 Run a Full Sync with Policy Enforcement](#87-run-a-full-sync-with-policy-enforcement)
   - [8.8 Verify the Registered Table in Nessie](#88-verify-the-registered-table-in-nessie)
   - [8.9 Test Access Denied at Sync Time](#89-test-access-denied-at-sync-time)
   - [8.10 Add a Runtime Contract and Re-test](#810-add-a-runtime-contract-and-re-test)
   - [8.11 Test Row-Level Security (RLS)](#811-test-row-level-security-rls)
   - [8.12 Test Column-Level Security (CLS)](#812-test-column-level-security-cls)
   - [8.13 Simulate Spark Query-Time RLS + CLS](#813-simulate-spark-query-time-rls--cls)
   - [8.14 PySpark + Nessie End-to-End Test (real JVM)](#814-pyspark--nessie-end-to-end-test-real-jvm)
9. [Adding a New Storage Backend](#9-adding-a-new-storage-backend)
10. [Adding a New Catalog Integration](#10-adding-a-new-catalog-integration)
11. [OAuth Service Internals](#11-oauth-service-internals)
12. [OPA Policy Internals](#12-opa-policy-internals)
13. [Key Design Decisions](#13-key-design-decisions)
14. [Troubleshooting](#14-troubleshooting)
15. [Common Pitfalls](#15-common-pitfalls)

---

## 1. Why This Tool Exists

Apache Iceberg embeds **absolute storage URIs** at every level of its metadata chain:

```
metadata.json          →  location: s3://src-bucket/gold/top_customers
  └─ manifest-list.avro  →  manifest_path: s3://src-bucket/.../snap-1.avro
       └─ manifest.avro  →  data_file.file_path: s3://src-bucket/.../0001.parquet
```

Copying the Parquet data files to another cloud without rewriting metadata leaves the target
table broken — all internal pointers still resolve back to the source cloud.

`iceberg-catalog-sync` solves this by:
1. Discovering the Iceberg metadata chain on the source.
2. Incrementally copying only new data files (manifest-based diff).
3. Rewriting every embedded URI in every metadata/manifest file using `PathTranslator`.
4. Registering the rewritten table in Nessie — authenticated with an OAuth JWT and
   authorized by data contract policy.

**Zero JVM dependency.** Avro manifests are read and written with `fastavro` (pure Python).

---

## 2. Project Layout

```
catalog-sync/
├── src/iceberg_sync/
│   ├── __init__.py              # package version ("0.1.0")
│   ├── cli.py                   # Click CLI — iceberg-sync command
│   ├── path_translator.py       # URI translation engine
│   ├── auth/
│   │   ├── __init__.py          # exports OAuthClient, PolicyClient, AccessDeniedError
│   │   ├── oauth_client.py      # client_credentials token manager + auto-refresh
│   │   └── policy_client.py     # data contract enforcer
│   ├── sync/
│   │   └── catalog_sync.py      # CatalogSync orchestrator
│   ├── storage/
│   │   ├── base.py              # StorageBackend ABC + FileInfo dataclass
│   │   ├── factory.py           # create_storage() scheme dispatcher
│   │   ├── s3.py                # S3 / MinIO (boto3)
│   │   ├── adls.py              # Azure ADLS Gen2 (azure-storage-blob)
│   │   ├── gcs.py               # Google Cloud Storage
│   │   └── memory.py            # In-memory (tests only)
│   ├── metadata/
│   │   ├── reader.py            # manifest chain walker
│   │   └── rewriter.py          # URI rewrite + version-hint commit
│   ├── catalog/
│   │   └── nessie.py            # Nessie v2 API client (OAuth + policy)
│   └── airflow/
│       └── operators.py         # Airflow operator wrappers
│
├── oauth_service/               # Standalone FastAPI OAuth 2.0 server
│   ├── main.py                  # endpoints: /token, /.well-known/*, /clients
│   ├── crypto.py                # RSA keygen, JWT sign/verify, JWKS
│   ├── models.py                # SQLAlchemy: OAuthClient, KeyStore
│   ├── config.py                # pydantic-settings: DATABASE_URL, ISSUER, etc.
│   ├── requirements.txt
│   └── Dockerfile
│
├── catalog_gateway/             # Iceberg REST Catalog proxy + OPA enforcement
│   ├── main.py                  # Catch-all proxy: JWT validate, OPA check, enforce, forward
│   ├── policy.py                # JWT validation (PyJWKClient) + OPA async client
│   ├── requirements.txt
│   └── Dockerfile
│
├── opa/
│   └── policies/
│       └── iceberg.rego         # Rego access rules (hot-reloaded from volume)
│
├── docker/
│   ├── docker-compose.yml       # 7-service enterprise stack
│   ├── docker-compose.airflow.yml
│   └── postgres/
│       └── init-dbs.sh          # Creates: nessie, oauth, airflow databases
│
├── docs/
│   ├── oauth-setup.md           # OAuth token flow + client management guide
│   └── opa-policies.md          # OPA policy structure, enforcement layers, how-to
│
├── airflow_dags/
│   └── iceberg_sync_dag.py      # DAG factory + SyncPipelineConfig
├── examples/
│   └── adls_to_nessie_poc.py    # End-to-end ADLS → Nessie POC
├── tests/
│   ├── test_path_translator.py
│   └── test_metadata_rewrite.py
└── pyproject.toml
```

---

## 3. Architecture Overview

### Service interaction map

```
                              ┌─────────────────────────────────┐
                              │        postgres:5432             │
                              │  db: nessie | oauth | airflow   │
                              └────┬────────────┬───────────────┘
                          JDBC     │            │ SQLAlchemy
                                   │            │
              ┌────────────────────┘            └────────────────────────┐
              ▼                                                           ▼
   ┌──────────────────────┐                             ┌────────────────────────┐
   │   nessie:19120        │                             │  oauth-service:8081    │
   │  (NOT host-exposed)   │ ◄── OIDC validates JWT      │                        │
   │  /iceberg/v1/*        │     via JWKS               │  POST /token           │
   │  (JDBC backend)       │                             │  GET  /.well-known/*   │
   └──────────▲────────────┘                             └────────────────────────┘
              │ admin Bearer JWT
              │
   ┌──────────┴──────────────────────────────────────────────────────────────────┐
   │                    catalog-gateway:8083   ← sole client entry point         │
   │                                                                              │
   │   1. Validate client JWT via JWKS     ──────────────► oauth-service:8081    │
   │   2. POST /v1/data/iceberg/policy     ──────────────► opa:8181              │
   │      → allow / excluded_columns / row_filter / column_masks                 │
   │   3. Enforce: 403 / schema rewrite / scan filter injection                  │
   │   4. Forward with admin token         ──────────────► nessie:19120          │
   └──────────────────────────────────────────────────────────────────────────────┘

   ┌─────────────────────────┐     ┌────────────────────────────────────────────┐
   │  opa:8181               │     │  minio:9000                                │
   │  (Rego policies)        │     │                                            │
   │  POST /v1/data/iceberg/ │     │  Bucket: warehouse/                        │
   │       policy            │     │  Console: http://localhost:9001            │
   │  (hot-reload from vol.) │     └────────────────────────────────────────────┘
   └─────────────────────────┘

   ┌──────────────────────────────────────────────────────────────────────────────┐
   │      Spark / iceberg-sync CLI / Airflow DAG   (Iceberg RESTCatalog)          │
   │                                                                              │
   │   → All traffic goes to catalog-gateway:8083 (never directly to Nessie)     │
   │   → Bearer JWT issued by oauth-service, principal extracted by gateway       │
   │   → RLS + CLS enforcement is transparent — no application-level filtering    │
   └──────────────────────────────────────────────────────────────────────────────┘
```

### Request lifecycle for a Spark table read (analytics-client)

```
Spark (RESTCatalog → catalog-gateway:8083)

  ① GET /v1/namespaces/gold/tables/orders
       → gateway validates Bearer JWT (analytics-client), extracts principal
       → gateway: POST /v1/data/iceberg/policy  (OPA)
         input:  {principal: "analytics-client", namespace: "gold", table: "orders", operation: "READ"}
         result: {allow: true, excluded_columns: ["ssn","credit_card_number"], row_filter: null, column_masks: {...}}
       → gateway: forwards GET to nessie with admin token
       → gateway: strips ssn + credit_card_number from schema in response (Layer 4 hard CLS)
       → gateway: stores column_masks as table property "gateway.column-masks" (Layer 3 advisory)
       → Spark receives table metadata WITHOUT ssn / credit_card_number in schema

  ② POST /v1/namespaces/gold/tables/orders/scan
       → gateway: POST /v1/data/iceberg/policy  (OPA, SCAN operation)
         result: {allow: true, row_filter: "region = 'EMEA'", ...}
       → gateway: merges {"type":"eq","term":"region","value":"EMEA"} into scan filter
       → forwards POST to nessie with merged filter
       → Nessie prunes scan: only EMEA partition files returned (file-level pruning)
       → Spark reads 2 rows from EMEA Parquet files only
```

### Request lifecycle for a sync CLI write (sync-service)

```
iceberg-sync CLI → catalog-gateway:8083

  ① OAuthClient.get_token()
       → POST http://localhost:8081/token {client_credentials, sync-service}
       → Returns RS256 JWT

  ② NessieCatalog.register_or_update(namespace="gold", table="orders", ...)
       → POST /v1/namespaces/gold/tables   Bearer: sync-service JWT
       → gateway: OPA WRITE check → allowed (sync-service is unrestricted)
       → gateway: forwards to nessie with admin token
       → Nessie commits ICEBERG_TABLE to main branch
```

---

## 4. Module Reference

### 4.1 PathTranslator

**File:** `src/iceberg_sync/path_translator.py`

The single source of truth for URI translation. All rewrites in the metadata chain
ultimately call `PathTranslator.translate()`.

```python
from iceberg_sync.path_translator import PathTranslator

t = PathTranslator([
    ("abfss://iceberg@account.dfs.core.windows.net/iceberg/", "s3a://warehouse/azure/"),
])
t.translate("abfss://iceberg@account.dfs.core.windows.net/iceberg/gold/orders/data/0001.parquet")
# → "s3a://warehouse/azure/gold/orders/data/0001.parquet"
```

**Key behaviours:**
- Roots normalised to always have trailing slash internally
- Mapping list is ordered — first match wins
- `strict=True` (default) raises `ValueError` for unmapped URIs
- `strict=False` passes through unknown URIs unchanged (used for `metadata-log` entries)
- `reverse()` returns a new translator with source/target swapped

**Convenience factories:**
```python
PathTranslator.aws_to_minio(s3_bucket="my-bucket", s3_prefix="iceberg/",
                             minio_bucket="warehouse", minio_prefix="aws")
PathTranslator.aws_to_azure(...)
PathTranslator.aws_to_gcs(...)
```

---

### 4.2 StorageBackend

**File:** `src/iceberg_sync/storage/base.py`

Abstract base class all backends implement:

```python
class StorageBackend(ABC):
    def list_objects(self, prefix: str) -> Iterator[FileInfo]: ...
    def read_bytes(self, uri: str) -> bytes: ...
    def write_bytes(self, uri: str, data: bytes) -> None: ...
    def exists(self, uri: str) -> bool: ...
    def delete(self, uri: str) -> None: ...
    def copy_from(self, source: StorageBackend, source_uri: str, target_uri: str) -> int: ...
```

`copy_from` is the hot path for large data files — each backend provides an optimised
implementation (S3 uses boto3 streaming; ADLS uses block copy).

**Factory:**
```python
from iceberg_sync.storage import create_storage

src = create_storage("s3://bucket/", region_name="eu-west-2")
tgt = create_storage("s3a://warehouse/",
                     endpoint_url="http://localhost:9000",
                     aws_access_key_id="minioadmin",
                     aws_secret_access_key="minioadmin")
```

---

### 4.3 CatalogSync

**File:** `src/iceberg_sync/sync/catalog_sync.py`

Main orchestrator. Constructed once and reused across multiple table syncs.

```
sync_table(table_root)
   │
   ├─ 1. DISCOVER: find_latest_metadata()
   │       version-hint.text (fast path) → scan metadata/ (fallback)
   │
   ├─ 2. DIFF: read_snapshot_data_files() vs list_objects(target)
   │
   ├─ 3. COPY: parallel thread pool — target.copy_from(source, ...)
   │            abort entirely if any copy fails
   │
   ├─ 4. REWRITE: MetadataRewriter.rewrite_table()
   │       manifests → manifest-lists → metadata.json → historical → version-hint.text
   │
   └─ 5. RETURN SyncResult
```

**SyncResult fields:**

| Field | Type | Description |
|-------|------|-------------|
| `files_copied` | int | Data files actually transferred |
| `files_skipped` | int | Already present on target |
| `bytes_copied` | int | Total bytes transferred |
| `target_metadata_uri` | str | Rewritten metadata.json URI on target |
| `rewrite_stats` | RewriteStats | Metadata rewrite counters |
| `success` | property | True if no errors |

---

### 4.4 MetadataReader

**File:** `src/iceberg_sync/metadata/reader.py`

Read-only manifest chain traversal using `fastavro`. Handles Iceberg v1 and v2 manifest
schema differences automatically.

```python
files = read_snapshot_data_files(storage, metadata_dict, table_root)
# Returns Dict[relative_path → FileInfo] for the current snapshot
```

Pass `content_types={0}` to restrict to DATA files only (exclude positional/equality deletes).

---

### 4.5 MetadataRewriter

**File:** `src/iceberg_sync/metadata/rewriter.py`

Reads metadata from source, rewrites all URIs using `PathTranslator`, writes to target.

**Rewrite chain (in order):**
1. Manifests (Avro) — translate `data_file.file_path` per row
2. Manifest-lists (Avro) — translate `manifest_path` per row
3. `metadata.json` (JSON) — translate `location`, `snapshots[].manifest-list`, `metadata-log`
4. Historical metadata files — rewrite each `metadata-log` entry (required by Iceberg 1.4+)
5. `version-hint.text` — atomic pointer; skipped when `write_version_hint_flag=False`
   (i.e. when a REST catalog like Nessie owns the pointer)

**Why fastavro, not PyIceberg?**

PyIceberg ties client versions to Iceberg spec versions. This caused version incompatibilities
in practice. `fastavro` reads and writes raw Avro bytes with no version coupling — the
rewriter acts as a byte-level URI substitution engine.

---

### 4.6 NessieCatalog

**File:** `src/iceberg_sync/catalog/nessie.py`

Thin REST client for the Nessie native v2 API. Now accepts `oauth_client` and `policy_client`.

```python
nessie = NessieCatalog(
    uri="http://localhost:19120",
    oauth_client=oauth,      # OAuthClient — token injected via _OAuthTokenAuth
    policy_client=policy,    # PolicyClient — enforce() called before mutations
)
```

**`_OAuthTokenAuth`** (inner class extending `requests.auth.AuthBase`):
- Called by `requests` on every outgoing request
- Calls `oauth_client.get_token()` — returns cached token or fetches a new one
- Sets `Authorization: Bearer <token>` header automatically
- Token refresh is transparent; no caller changes needed

**Policy enforcement hooks:**

| Method | Operation enforced |
|--------|--------------------|
| `register_table()` | `write` |
| `update_table()` | `write` |
| `drop_table()` | `drop` |
| `list_tables()` | `read` |
| `get_metadata_location()` | `read` |

**Commit model:** Every write is a two-step operation:
1. `GET /api/v2/trees/{ref}` → current branch hash
2. `POST /api/v2/trees/{ref}@{hash}/history/commit` — Nessie rejects if another writer
   raced (returns 409)

> **Important:** Always use `/api/v2`, not `/iceberg/v1`. The Iceberg REST compatibility
> layer requires extra Quarkus config and is absent in most Nessie deployments.

---

### 4.7 OAuthClient

**File:** `src/iceberg_sync/auth/oauth_client.py`

Client credentials token manager with automatic refresh.

```python
from iceberg_sync.auth import OAuthClient

oauth = OAuthClient(
    server_url="http://localhost:8081",
    client_id="sync-service",
    client_secret="sync-secret",
    scope="catalog:read catalog:write",
)

token = oauth.get_token()   # fetches if absent; returns cached if valid; refreshes 30s before expiry
oauth.invalidate()          # force next get_token() to fetch a new token
```

**Caching strategy:** Uses `time.monotonic()` for expiry tracking. Refreshes when
`time.monotonic() >= expires_at - 30`. Thread-safe for single-threaded use; add a lock
if sharing across threads in Airflow DAGs.

---

### 4.8 Policy enforcement (gateway)

Policy enforcement is in the catalog-gateway, not in a client library. The sync CLI
does not call a policy service — it sends requests to the gateway (port 8083) with a
Bearer JWT, and the gateway enforces OPA policy before forwarding to Nessie.

```python
# catalog_gateway/policy.py
from policy import get_policy, validate_token

# Validate client JWT (PyJWKClient — JWKS fetched from oauth-service)
claims = validate_token(bearer_token)
principal = claims.get("client_id") or claims.get("sub")

# Query OPA for a policy decision (async)
dec = await get_policy(principal, namespace="gold", table="orders", operation="WRITE")
if not dec.allow:
    raise HTTPException(403, f"Principal '{principal}' denied WRITE on gold.orders")
# → forwards to Nessie with admin token
```

**Failure mode:** If OPA is unreachable, `get_policy()` raises an HTTP error →
gateway returns HTTP 500. Fails closed — never grants access when OPA is down.

---

### 4.9 CLI

**File:** `src/iceberg_sync/cli.py`

Click group with `table` and `namespace` commands. Key additions vs original:

```
iceberg-sync [--verbose]
    table     --table <path>     [storage opts] [nessie opts] [oauth opts] [policy opts]
    namespace --namespace <path> [storage opts] [nessie opts] [oauth opts] [policy opts]
```

**OAuth/policy flow in CLI:**

```python
# _build_oauth_and_policy() creates clients from CLI opts/env vars
oauth_client = OAuthClient(oauth_url, oauth_client_id, oauth_client_secret, oauth_scope)
policy_client = PolicyClient(policy_url, principal=oauth_client_id)

# Both are passed through to _register_in_nessie() → NessieCatalog()
nessie = NessieCatalog(uri=..., oauth_client=oauth_client, policy_client=policy_client)
```

**Environment variable equivalents (useful for CI/CD):**

```bash
export OAUTH_URL=http://localhost:8081
export OAUTH_CLIENT_ID=sync-service
export OAUTH_CLIENT_SECRET=sync-secret
export NESSIE_URI=http://localhost:8083
```

---

### 4.10 Airflow Operators

**File:** `src/iceberg_sync/airflow/operators.py`

| Operator | XCom key | Purpose |
|----------|----------|---------|
| `IcebergTableSyncOperator` | `sync_result` | Sync single table |
| `IcebergNamespaceSyncOperator` | `sync_results` | Sync all tables in namespace |
| `IcebergHealthCheckOperator` | `health_result` | Verify no leaked source URIs |
| `NessieCatalogRegisterOperator` | `nessie_result` | Register in Nessie post-sync |

**XCom chaining:**
```python
register = NessieCatalogRegisterOperator(
    metadata_location="{{ task_instance.xcom_pull('sync')['target_metadata_uri'] }}",
    ...
)
```

---

## 5. Full Data Flow

```
User / Airflow
    │
    ▼
CLI: iceberg-sync table \
       --oauth-url http://localhost:8081 \
       --oauth-client-id sync-service \
       --oauth-client-secret sync-secret \
       --nessie-uri http://localhost:8083 \
       --table gold/orders ...
    │
    ├─ _build_oauth()
    │   └─ OAuthClient("http://localhost:8081", "sync-service", "sync-secret")
    │
    ├─ _build_sync()  [no policy check here — gateway enforces on registration]
    │   ├─ PathTranslator([(source_root, target_root)])
    │   ├─ create_storage(source_root, **source_kwargs)
    │   └─ create_storage(target_root, **target_kwargs)
    │
    └─ CatalogSync.sync_table(table_root)
        │
        ├─ ① DISCOVER
        │   └─ find_latest_metadata(source, table_root)
        │       ├─ try: read version-hint.text
        │       └─ fallback: scan metadata/ for NNN-UUID.metadata.json
        │
        ├─ ② DIFF
        │   ├─ read_snapshot_data_files(source, metadata)
        │   │   └─ source.read_bytes(manifest-list.avro) → source.read_bytes(manifest.avro) [each]
        │   └─ target.list_objects(table_root)
        │       └─ diff: source_files - target_files = files_to_copy
        │
        ├─ ③ COPY (parallel thread pool, abort on any failure)
        │   └─ target.copy_from(source, src_uri, tgt_uri) [for each missing file]
        │
        ├─ ④ REWRITE
        │   └─ MetadataRewriter.rewrite_table(source_metadata_uri)
        │       ├─ _rewrite_manifest()      → translate data_file.file_path (Avro)
        │       ├─ _rewrite_manifest_list() → translate manifest_path (Avro)
        │       ├─ translate metadata.json  → location, snapshot pointers (JSON)
        │       ├─ _rewrite_historical_metadata() → metadata-log entries
        │       └─ _write_version_hint()   → SKIPPED (REST catalog owns pointer)
        │
        ├─ ⑤ RETURN SyncResult { target_metadata_uri, files_copied, ... }
        │
        └─ ⑥ REGISTER [only if --nessie-uri provided]
            │
            └─ NessieCatalog.register_or_update("gold", "orders", target_metadata_uri)
                │
                ├─ OAuthClient.get_token() → "eyJ..." (cached or fresh)
                ├─ POST /v1/namespaces/gold/tables  (to catalog-gateway:8083)
                │   Authorization: Bearer eyJ...  (sync-service JWT)
                │   gateway: OPA check → WRITE allowed for sync-service
                │   gateway: forwards to nessie with admin token
                └─ Nessie commits ICEBERG_TABLE to main branch
```

---

## 6. Local Development Setup

```bash
# 1. Clone
git clone <repo> catalog-sync
cd catalog-sync

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install package in editable mode with all extras
pip install -e ".[nessie,dev,auth]"

# 4. Verify CLI
iceberg-sync --help
```

> The `auth` extra adds `requests` and `pyjwt` for the `OAuthClient` and `PolicyClient`.
> The `nessie` extra adds `requests` for `NessieCatalog`. Both are included in `dev`.

---

## 7. Running Tests

```bash
# All unit tests (no Docker, no network — uses MemoryStorageBackend)
pytest

# With coverage report
pytest --cov=iceberg_sync --cov-report=term-missing

# Single file
pytest tests/test_path_translator.py -v

# Single test
pytest tests/test_metadata_rewrite.py::test_full_rewrite_chain -v
```

`test_metadata_rewrite.py` builds a realistic in-memory Iceberg table (metadata.json +
manifest-list + manifest + fake Parquet) and verifies every URI is correctly translated
after `rewrite_table()`.

---

## 8. End-to-End Test Walkthrough

This section walks through starting the full stack and validating every layer: OAuth token
issuance, Nessie JWT authentication, policy enforcement, table sync, and catalog registration.

### 8.1 Start the Stack

```bash
cd docker

# First time or after dependency changes: force a full image rebuild
docker compose down -v
docker compose build --no-cache
docker compose up -d
```

Watch the startup chain — Nessie starts last (depends on oauth-service being healthy):

```bash
docker compose logs -f oauth-service nessie
```

Expected milestones in order:

```
oauth-service  | INFO: Application startup complete.
nessie         | (fetches OIDC config + JWKS from oauth-service)
nessie         | (health checks at /q/health/ready begin returning 200)
```

Check final status:

```bash
docker compose ps
```

Expected — every service `healthy`, mc-init `Exited (0)`:

```
NAME             STATUS
postgres         Up (healthy)
oauth-service    Up (healthy)
opa              Up (healthy)
catalog-gateway  Up (healthy)
nessie           Up (healthy)
minio            Up (healthy)
mc-init          Exited (0)
```

> **If Nessie is missing from `docker compose ps`** it never started. This almost always
> means oauth-service is `unhealthy`. Fix the upstream service first —
> Nessie will not start until its `depends_on: condition: service_healthy` is satisfied.
> See [Troubleshooting](#troubleshooting) below.

---

### 8.2 Verify Services

#### PostgreSQL — databases exist

```bash
docker compose exec postgres psql -U postgres -c "\l"
# Look for: nessie, oauth, airflow databases in the list
```

#### OAuth service — health + OIDC discovery

```bash
# Health
curl -s http://localhost:8081/health
# Expected: {"status":"ok","service":"oauth"}

# OIDC discovery (Nessie fetches this on startup)
curl -s http://localhost:8081/.well-known/openid-configuration
# Expected JSON with "issuer": "http://oauth-service:8081"

# JWKS (public key Nessie uses to verify JWTs)
curl -s http://localhost:8081/.well-known/jwks.json
# Expected: {"keys":[{"kty":"RSA","use":"sig","kid":"...","alg":"RS256",...}]}
```

#### Get a token and verify it works

```bash
curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret"
# Expected: {"access_token":"eyJ...","token_type":"Bearer","expires_in":3600,...}
```

Save the token for subsequent steps:

```bash
# bash/zsh
TOKEN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret" \
  | jq -r .access_token)
echo $TOKEN
```

#### Catalog Gateway — health

```bash
curl -s http://localhost:8083/health | jq .
# Expected: {"status":"ok","enforces":["table-access","column-exclusion","row-filter"],"advisory":["column-masking"]}
```

#### Nessie — authenticated API call via gateway

```bash
# Nessie port 19120 is NOT exposed to host — all access via gateway
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8083/v1/config | jq .
# Expected: {"defaults":{},"overrides":{},...}
```

#### OPA — health + policy query

```bash
curl -s http://localhost:8181/health | jq .
# Expected: {}  (OPA returns empty JSON on healthy)

curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"admin-client","namespace":"gold","table":"orders","operation":"READ"}}' \
  | jq .result.allow
# Expected: true
```

#### MinIO

```bash
curl -s http://localhost:9000/minio/health/live && echo "MinIO OK"
# MinIO console: http://localhost:9001  (minioadmin / minioadmin)
```

---

### 8.3 Seed Test Data in MinIO

The seed script lives at [scripts/seed_test_data.py](../scripts/seed_test_data.py).
It writes a minimal Iceberg table to MinIO (`source/gold/orders/`) — Parquet stub +
Avro manifests + `metadata.json` — enough for `iceberg-sync` to copy and rewrite the
metadata chain. The Parquet content is stub bytes, not queryable rows; for a test with
real queryable data use [scripts/test_pyspark_nessie.py](../scripts/test_pyspark_nessie.py)
(section 8.14).

```bash
pip install boto3 fastavro   # if not already installed
python scripts/seed_test_data.py
```

Verify the files exist in MinIO:
```bash
# Check files were created
curl -s "http://localhost:9000/warehouse?list-type=2&prefix=source/gold/orders/" \
  --user minioadmin:minioadmin | grep -o '<Key>[^<]*</Key>' | head -10
```

Or browse at http://localhost:9001 (minioadmin / minioadmin) → warehouse bucket.

---

### 8.4 Test OAuth Token Issuance

Test each default client gets a token with the correct scopes:

```bash
# Helper function
get_token() {
  curl -s -X POST http://localhost:8081/token \
    -d "grant_type=client_credentials&client_id=$1&client_secret=$2" \
    | jq -r .access_token
}

# sync-service — should get catalog:read catalog:write
SYNC_TOKEN=$(get_token sync-service sync-secret)
echo "sync-service token: ${SYNC_TOKEN:0:50}..."

# Inspect claims
echo $SYNC_TOKEN | cut -d. -f2 | base64 -d 2>/dev/null | python -m json.tool
# Expected: {"iss":"http://oauth-service:8081","sub":"sync-service",
#             "aud":["nessie-server"],"scope":"catalog:read catalog:write",...}

# analytics-client — should get catalog:read only
ANALYTICS_TOKEN=$(get_token analytics-client analytics-secret)
echo $ANALYTICS_TOKEN | cut -d. -f2 | base64 -d 2>/dev/null | jq .scope
# Expected: "catalog:read"

# Test token introspection
curl -s -X POST http://localhost:8081/introspect \
  -d "token=$SYNC_TOKEN" | jq '{active, client_id, scope}'
# Expected: {"active": true, "client_id": "sync-service", "scope": "catalog:read catalog:write"}

# Test wrong credentials
curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=sync-service&client_secret=wrong" \
  | jq .detail
# Expected: "invalid_client"
```

---

### 8.5 Test Nessie Authentication

```bash
SYNC_TOKEN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=sync-service&client_secret=sync-secret" \
  | jq -r .access_token)

# Authenticated call — should succeed (200)
curl -s -H "Authorization: Bearer $SYNC_TOKEN" \
  http://localhost:19120/api/v2/trees/main | jq .reference.name
# Expected: "main"

# No token — should fail (401)
curl -s http://localhost:19120/api/v2/trees/main | jq .
# Expected: 401 Unauthorized

# Expired/tampered token — should fail (401)
curl -s -H "Authorization: Bearer eyJfake.token.here" \
  http://localhost:19120/api/v2/trees/main | jq .
# Expected: 401

# List namespaces (uses GET — requires at minimum catalog:read scope in JWT)
curl -s -H "Authorization: Bearer $SYNC_TOKEN" \
  "http://localhost:19120/api/v2/trees/main/entries" | jq .entries
```

---

### 8.6 Test Fine-Grained Access Control

Query OPA directly to verify the access matrix, then verify gateway enforcement with real HTTP calls.

**Query OPA directly (policy engine):**

```bash
opa_check() {
  # Usage: opa_check <principal> <namespace> <table> <operation>
  RESULT=$(curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
    -H "Content-Type: application/json" \
    -d "{\"input\":{\"principal\":\"$1\",\"namespace\":\"$2\",\"table\":\"$3\",\"operation\":\"$4\"}}")
  ALLOW=$(echo $RESULT | jq -r '.result.allow')
  EXCL=$(echo $RESULT | jq -c '.result.excluded_columns')
  FILTER=$(echo $RESULT | jq -r '.result.row_filter // "(none)"')
  printf "%-20s %-6s %-8s %-6s  excl=%-30s filter=%s\n" "$1" "$4" "$2" "$ALLOW" "$EXCL" "$FILTER"
}

echo "=== OPA Access Matrix ==="
opa_check "admin-client"     "gold"   "orders"  "READ"
opa_check "admin-client"     "gold"   "orders"  "WRITE"
opa_check "analytics-client" "gold"   "orders"  "READ"
opa_check "analytics-client" "gold"   "orders"  "WRITE"   # deny
opa_check "analytics-client" "silver" "facts"   "READ"    # deny
opa_check "data-scientist"   "silver" "facts"   "READ"
opa_check "data-scientist"   "bronze" "raw"     "READ"
opa_check "data-scientist"   "gold"   "orders"  "READ"    # deny
```

Expected output:
```
=== OPA Access Matrix ===
admin-client         READ   gold     true   excl=[]                            filter=(none)
admin-client         WRITE  gold     true   excl=[]                            filter=(none)
analytics-client     READ   gold     true   excl=["ssn","credit_card_number"]  filter=region = 'EMEA'
analytics-client     WRITE  gold     false  excl=[]                            filter=(none)
analytics-client     READ   silver   false  excl=[]                            filter=(none)
data-scientist       READ   silver   true   excl=["ssn","credit_card_number","date_of_birth"]  filter=(none)
data-scientist       READ   bronze   true   excl=["ssn","credit_card_number","date_of_birth"]  filter=(none)
data-scientist       READ   gold     false  excl=[]                            filter=(none)
```

**Verify gateway enforcement (real HTTP 403):**

```bash
# Get tokens for each principal
TOKEN_ADMIN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret" \
  | jq -r .access_token)
TOKEN_ANALYTICS=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=analytics-client&client_secret=analytics-secret" \
  | jq -r .access_token)
TOKEN_DS=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=data-scientist&client_secret=ds-secret" \
  | jq -r .access_token)

# admin: 200
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $TOKEN_ADMIN" \
  http://localhost:8083/v1/namespaces/gold/tables/orders
# Expected: 200

# analytics-client: 200 on gold (allowed)
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  http://localhost:8083/v1/namespaces/gold/tables/orders
# Expected: 200

# analytics-client: 403 on silver (denied)
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  http://localhost:8083/v1/namespaces/silver/tables/facts
# Expected: 403

# data-scientist: 403 on gold (denied)
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $TOKEN_DS" \
  http://localhost:8083/v1/namespaces/gold/tables/orders
# Expected: 403
```

---

### 8.7 Run a Full Sync with Policy Enforcement

This syncs the seeded test table from `source/gold/orders` to `target/gold/orders`,
with OAuth token auto-fetch and policy enforcement.

```bash
iceberg-sync table \
  --source-root "s3a://warehouse/source/" \
  --target-root "s3a://warehouse/target/" \
  --table "gold/orders" \
  --source-endpoint http://localhost:9000 \
  --source-access-key minioadmin \
  --source-secret-key minioadmin \
  --target-endpoint http://localhost:9000 \
  --target-access-key minioadmin \
  --target-secret-key minioadmin \
  --nessie-uri http://localhost:8083 \
  --nessie-ref main \
  --oauth-url http://localhost:8081 \
  --oauth-client-id sync-service \
  --oauth-client-secret sync-secret \
  --verbose
```

Expected output:
```
Syncing table: s3a://warehouse/source/gold/orders/
Nessie catalog: http://localhost:19120  (ref: main)

 Status        ✓ SUCCESS
 Table         s3a://warehouse/source/gold/orders/
 Files copied  3
 Files skipped 0
 Bytes copied  0.1 MB
 Duration      2.3s
 Metadata rewritten  1
 Manifests rewritten 2
 Paths translated    1

  Nessie: gold.orders registered at http://localhost:19120
```

Verify the rewritten metadata has correct target URIs (NO source URIs should appear):
```bash
SYNC_TOKEN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=sync-service&client_secret=sync-secret" \
  | jq -r .access_token)

# Get the metadata location from Nessie
META_LOC=$(curl -s -H "Authorization: Bearer $SYNC_TOKEN" \
  "http://localhost:19120/api/v2/trees/main/contents/gold.orders" \
  | jq -r .content.metadataLocation)
echo "Registered metadata: $META_LOC"
# Expected: s3a://warehouse/target/gold/orders/metadata/v1.metadata.json

# Download and inspect the rewritten metadata.json
MINIO_KEY="${META_LOC#s3a://warehouse/}"
curl -s "http://localhost:9000/warehouse/$MINIO_KEY" \
  --user minioadmin:minioadmin | python -m json.tool | grep location
# Expected: "location": "s3a://warehouse/target/gold/orders"  ← no source URIs!

# Health check: scan target for any leaked source URIs
grep -r "source/" ~/.cache 2>/dev/null || true
# Use the policy service or catalog directly to list registered metadata
```

---

### 8.8 Verify the Registered Table in Nessie

```bash
SYNC_TOKEN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=sync-service&client_secret=sync-secret" \
  | jq -r .access_token)

# List namespaces
curl -s -H "Authorization: Bearer $SYNC_TOKEN" \
  "http://localhost:19120/api/v2/trees/main/entries" \
  | jq '[.entries[] | {type, name: .name.elements | join(".") }]'

# Get the table content
curl -s -H "Authorization: Bearer $SYNC_TOKEN" \
  "http://localhost:19120/api/v2/trees/main/contents/gold.orders" \
  | jq '.content | {type, metadataLocation}'

# Expected:
# {
#   "type": "ICEBERG_TABLE",
#   "metadataLocation": "s3a://warehouse/target/gold/orders/metadata/v1.metadata.json"
# }
```

---

### 8.9 Test Access Denied at Sync Time

Now attempt the same sync using `analytics-client` — which has `catalog:read` scope
and a policy contract that only allows `read` on `gold`. The policy enforcer should
block the `write` operation before Nessie is called.

```bash
iceberg-sync table \
  --source-root "s3a://warehouse/source/" \
  --target-root "s3a://warehouse/target2/" \
  --table "gold/orders" \
  --source-endpoint http://localhost:9000 \
  --source-access-key minioadmin \
  --source-secret-key minioadmin \
  --target-endpoint http://localhost:9000 \
  --target-access-key minioadmin \
  --target-secret-key minioadmin \
  --nessie-uri http://localhost:8083 \
  --oauth-url http://localhost:8081 \
  --oauth-client-id analytics-client \
  --oauth-client-secret analytics-secret
```

Expected output:
```
Syncing table: s3a://warehouse/source/gold/orders/

 Status        ✗ FAILED
 Errors        HTTP 403: Principal 'analytics-client' denied WRITE on gold.orders
```

The file copy may have succeeded (storage operation), but the catalog registration is
blocked by the gateway with HTTP 403. The catalog is not updated.

Now verify denial directly via OPA:

```bash
# data-scientist can only access silver/bronze — gold should be denied
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"data-scientist","namespace":"gold","table":"orders","operation":"WRITE"}}' \
  | jq '{allow: .result.allow}'
# Expected: {"allow": false}

# Gateway returns 403:
TOKEN_DS=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=data-scientist&client_secret=ds-secret" \
  | jq -r .access_token)
curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST http://localhost:8083/v1/namespaces/gold/tables \
  -H "Authorization: Bearer $TOKEN_DS" \
  -H "Content-Type: application/json" \
  -d '{}'
# Expected: 403
```

---

### 8.10 Update a Policy Rule and Re-test

OPA hot-reloads Rego files from the mounted volume. Edit `opa/policies/iceberg.rego`
and OPA picks up the change within a few seconds — no restart needed.

```bash
# Step 1: Verify DENIED before change
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"data-scientist","namespace":"gold","table":"orders","operation":"READ"}}' \
  | jq .result.allow
# Expected: false

# Step 2: Edit opa/policies/iceberg.rego — add a rule:
#   allow if {
#       input.principal == "data-scientist"
#       input.namespace == "gold"
#       input.operation in {"READ", "LIST", "SCAN"}
#   }
# (Save the file — OPA reloads automatically)

# Step 3: Wait ~2s then verify ALLOWED (no restart)
sleep 2
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"data-scientist","namespace":"gold","table":"orders","operation":"READ"}}' \
  | jq .result.allow
# Expected: true

# Step 4: Revert the Rego file (remove the added rule) → DENIED again within seconds
```

---

## 8.11 Test Row-Level Security (RLS)

RLS is enforced by the gateway injecting an Iceberg scan filter expression into every
`POST /scan` request. Nessie uses the filter for file-level partition pruning.

```bash
TOKEN_ANALYTICS=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=analytics-client&client_secret=analytics-secret" \
  | jq -r .access_token)
TOKEN_ADMIN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret" \
  | jq -r .access_token)

# ── Confirm OPA row_filter for analytics-client on gold.orders ────────────────
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"analytics-client","namespace":"gold","table":"orders","operation":"SCAN"}}' \
  | jq '.result | {allow, row_filter}'
# Expected: {"allow": true, "row_filter": "region = 'EMEA'"}

# ── Confirm OPA returns no row filter for admin ────────────────────────────────
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"admin-client","namespace":"gold","table":"orders","operation":"SCAN"}}' \
  | jq '.result | {allow, row_filter}'
# Expected: {"allow": true, "row_filter": null}

# ── Verify gateway injects filter in scan request ─────────────────────────────
# Submit a scan request with no filter — gateway adds the EMEA constraint:
curl -s -X POST http://localhost:8083/v1/namespaces/gold/tables/orders/scan \
  -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  -H "Content-Type: application/json" \
  -d '{}' | jq '.tasks | length'
# Returns: number of scan tasks — only EMEA partition files (≤2 tasks for 2 EMEA rows)
# Admin sees all partition files:
curl -s -X POST http://localhost:8083/v1/namespaces/gold/tables/orders/scan \
  -H "Authorization: Bearer $TOKEN_ADMIN" \
  -H "Content-Type: application/json" \
  -d '{}' | jq '.tasks | length'
# Returns: more tasks (EMEA + APAC + AMER partitions)
```

---

## 8.12 Test Column-Level Security (CLS)

CLS (column exclusion) is enforced by the gateway rewriting the table schema response —
Spark never sees excluded columns and cannot include them in queries.

```bash
TOKEN_ANALYTICS=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=analytics-client&client_secret=analytics-secret" \
  | jq -r .access_token)
TOKEN_ADMIN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret" \
  | jq -r .access_token)

# ── Confirm OPA excluded_columns for analytics-client ─────────────────────────
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"analytics-client","namespace":"gold","table":"orders","operation":"READ"}}' \
  | jq '.result | {excluded_columns, column_masks}'
# Expected:
# {
#   "excluded_columns": ["ssn", "credit_card_number"],
#   "column_masks": {"customer_email": "regexp_replace(...)", "ip_address": "regexp_replace(...)"}
# }

# ── Gateway schema rewrite: analytics-client cannot see ssn ───────────────────
# Admin sees all columns including ssn + credit_card_number:
curl -s -H "Authorization: Bearer $TOKEN_ADMIN" \
  http://localhost:8083/v1/namespaces/gold/tables/orders \
  | jq '[.metadata.schema.fields[].name]'
# Expected: includes "ssn", "credit_card_number"

# Analytics-client: ssn + credit_card_number stripped from schema
curl -s -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  http://localhost:8083/v1/namespaces/gold/tables/orders \
  | jq '[.metadata.schema.fields[].name]'
# Expected: does NOT include "ssn" or "credit_card_number"

# ── Column masks stored as table property (advisory) ─────────────────────────
curl -s -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  http://localhost:8083/v1/namespaces/gold/tables/orders \
  | jq '.metadata.properties["gateway.column-masks"]'
# Expected: JSON string with customer_email + ip_address mask expressions
```

---

## 8.13 Verify End-to-End Enforcement (curl)

This section verifies all four enforcement layers using only `curl` and `jq` — no Spark
needed. It tests the gateway's behavior at the HTTP protocol level.

```bash
# Bootstrap tokens
TOKEN_ADMIN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret" \
  | jq -r .access_token)
TOKEN_ANALYTICS=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=analytics-client&client_secret=analytics-secret" \
  | jq -r .access_token)
TOKEN_DS=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=data-scientist&client_secret=ds-secret" \
  | jq -r .access_token)

echo "=== Layer 1: Allow / Deny (HTTP 403) ==="
# admin-client on gold: 200
echo -n "admin   gold.orders READ: "
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $TOKEN_ADMIN" \
  http://localhost:8083/v1/namespaces/gold/tables/orders

# analytics-client on silver: 403
echo -n "analytics silver.facts READ: "
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  http://localhost:8083/v1/namespaces/silver/tables/facts

# data-scientist on gold: 403
echo -n "data-scientist gold.orders READ: "
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $TOKEN_DS" \
  http://localhost:8083/v1/namespaces/gold/tables/orders

echo ""
echo "=== Layer 4: Column Exclusion (schema rewrite) ==="
# Admin sees ssn:
echo -n "admin sees ssn: "
curl -s -H "Authorization: Bearer $TOKEN_ADMIN" \
  http://localhost:8083/v1/namespaces/gold/tables/orders \
  | jq '[.metadata.schema.fields[].name] | any(. == "ssn")'

# Analytics cannot see ssn:
echo -n "analytics sees ssn: "
curl -s -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  http://localhost:8083/v1/namespaces/gold/tables/orders \
  | jq '[.metadata.schema.fields[].name] | any(. == "ssn")'

echo ""
echo "=== Layer 3: Column Masks as Table Property (advisory) ==="
curl -s -H "Authorization: Bearer $TOKEN_ANALYTICS" \
  http://localhost:8083/v1/namespaces/gold/tables/orders \
  | jq '.metadata.properties["gateway.column-masks"]'
```

Expected output:
```
=== Layer 1: Allow / Deny (HTTP 403) ===
admin   gold.orders READ: 200
analytics silver.facts READ: 403
data-scientist gold.orders READ: 403

=== Layer 4: Column Exclusion (schema rewrite) ===
admin sees ssn: true
analytics sees ssn: false

=== Layer 3: Column Masks as Table Property (advisory) ===
"{\"customer_email\":\"regexp_replace(customer_email, '@.*$', '@***.com')\",\"ip_address\":\"regexp_replace(ip_address, '[0-9]+$', 'xxx')\"}"
```

---

## 8.14 PySpark + Catalog Gateway End-to-End Test (real JVM)

This test uses a real PySpark session connected to the catalog-gateway using the standard
Iceberg RESTCatalog. Enforcement is fully transparent to Spark — no manual filtering.

> **What the test proves:**
> - Spark connects via Iceberg RESTCatalog (not NessieCatalog) through the gateway
> - Tables are created with format-version=2 and PARTITIONED BY (region)
> - analytics-client receives only 2 EMEA rows with no explicit `df.filter()` call
> - ssn + credit_card_number are absent from `df.columns` without any `df.drop()` call
> - data-scientist gets an exception (HTTP 403) when reading gold.orders

### Prerequisites

| Requirement | Notes |
|-------------|-------|
| Java 11+ | `java -version` must show 11 or 17 |
| `pyspark` | `pip install pyspark` (PySpark 3.5) |
| Stack up | `cd docker && docker compose up -d` — all services healthy |

On **first run** `spark.jars.packages` downloads ~150 MB of JARs from Maven Central.

### JARs loaded automatically

| JAR | Purpose |
|-----|---------|
| `iceberg-spark-runtime-3.5_2.12:1.9.1` | Iceberg format v2 + REST catalog client |
| `hadoop-aws:3.3.4` | S3A filesystem for MinIO |
| `aws-java-sdk-bundle:1.12.262` | AWS SDK for S3A |

No Nessie-specific JARs required — pure Iceberg REST protocol.

### Run the test

```bash
# From the project root (not docker/)
python scripts/test_pyspark_nessie.py
```

### What the script does

```
[1/4] Start SparkSession  — three named REST catalogs (one per principal)
[2/4] Seed gw_admin.gold.orders  — format-version=2, PARTITIONED BY region, 6 rows
[3/4] Read via each catalog  — enforcement is automatic (gateway enforces)
[4/4] Run assertions  — counts + column visibility, no manual filtering
```

### Expected output — principal breakdown

**gw_admin** (unrestricted):
```
Catalog: gw_admin
  Visible columns: ['id', 'region', 'status', 'amount', 'customer_email', 'ip_address', 'ssn', 'credit_card_number']
  Row count:       6
```

**gw_analytics** (gateway enforces: EMEA only, ssn + credit_card_number excluded):
```
Catalog: gw_analytics
  Visible columns: ['id', 'region', 'status', 'amount', 'customer_email', 'ip_address']
  Row count:       2
+---+------+---------+------+-----------------+--------------+
|id |region|status   |amount|customer_email   |ip_address    |
+---+------+---------+------+-----------------+--------------+
|1  |EMEA  |COMPLETED|1500.0|alice@example.com|192.168.1.10  |
|2  |EMEA  |COMPLETED|2300.0|bob@example.com  |10.0.0.2      |
+---+------+---------+------+-----------------+--------------+
```

**gw_ds** (gateway returns 403 for gold):
```
Catalog: gw_ds
  [BLOCKED] Gateway denied access: ...403...
```

### Assertions verified

| Assertion | How enforced |
|-----------|-------------|
| admin: 6 rows, ssn visible | no restrictions |
| analytics: 2 rows, no explicit filter | gateway injects scan filter → Nessie partition pruning |
| analytics: ssn absent from df.columns | gateway strips from schema before Spark plans query |
| analytics: only EMEA regions | scan filter prevents other files from being fetched |
| data-scientist: exception on gold | gateway returns HTTP 403 |

### Troubleshooting

**`java.lang.UnsupportedClassVersionError`** — JDK too old; needs 11+:
```bash
java -version   # must show 11 or 17
```

**`Connection refused` on gateway or OAuth** — stack not healthy:
```bash
cd docker && docker compose ps
# catalog-gateway, opa, oauth-service, nessie must show (healthy)
```

**`NoSuchNamespaceException: gold`** — namespace creation failed; check admin token:
```bash
curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret" \
  | jq -r .access_token | xargs -I{} curl -s \
  -H "Authorization: Bearer {}" http://localhost:8083/v1/namespaces/gold
```

**JARs download every run** — Maven cache in `~/.ivy2/`. Corrupted:
```bash
rm -rf ~/.ivy2/cache/org.apache.iceberg && python scripts/test_pyspark_nessie.py
```

---

## 9. Adding a New Storage Backend

1. Create `src/iceberg_sync/storage/<name>.py`
2. Subclass `StorageBackend` and implement all six abstract methods
3. Register the scheme in `storage/factory.py`:

```python
_SCHEME_MAP = {
    ...
    "myscheme": MyStorageBackend,
}
```

4. Add scheme-based kwargs dispatch in `cli.py → _build_sync()`
5. Add tests using `MemoryStorageBackend` as a reference

**Checklist:**
- [ ] `list_objects()` must yield every object recursively, not just top-level
- [ ] `copy_from()` must stream bytes, not buffer the whole file in memory
- [ ] Use retries/backoff for transient errors (see S3 backend)
- [ ] Normalise root URI with trailing slash

---

## 10. Adding a New Catalog Integration

1. Create `src/iceberg_sync/catalog/<name>.py`
2. Implement at minimum: `register_or_update(ns, table, metadata_location) → dict`, `ping() → bool`
3. Add a CLI option (similar to `--nessie-uri`)
4. Call after `sync.sync_table()` returns success, passing `result.target_metadata_uri`

No base class required — duck typing is used intentionally.

---

## 11. OAuth Service Internals

**File layout:**
```
oauth_service/
├── config.py     # pydantic-settings: DATABASE_URL, ISSUER, JWKS_KID, ADMIN_TOKEN
├── crypto.py     # RSA key generation, get_jwks(), create_access_token(), decode_token()
├── models.py     # SQLAlchemy: OAuthClient (clients table), KeyStore (keys table)
└── main.py       # FastAPI app, lifespan (key load/generate + seed clients)
```

**Startup sequence:**
1. `Base.metadata.create_all()` — create tables if not exists
2. Load RSA private key from `key_store` table (or generate + persist if first start)
3. Seed four default clients if not already in `oauth_clients` table
4. FastAPI application ready

**JWT structure:**
```json
{
  "iss": "http://oauth-service:8081",
  "sub": "sync-service",
  "aud": ["nessie-server"],
  "iat": 1700000000,
  "exp": 1700003600,
  "jti": "uuid-...",
  "client_id": "sync-service",
  "scope": "catalog:read catalog:write"
}
```

**Key persistence:** The RSA key pair is generated once and stored in the `key_store`
PostgreSQL table. JWKS is cached in memory after first build. This means:
- Nessie's cached JWKS remains valid across oauth-service restarts
- Multiple oauth-service replicas share the same key (all from the same DB)

**Scope intersection:** When a client requests scopes, the granted set is the
intersection of requested and allowed: `granted = requested ∩ client.scopes`.
If no scopes are requested, all allowed scopes are granted.

---

## 12. OPA Policy Internals

**File layout:**
```
opa/policies/
└── iceberg.rego     # All access rules — hot-reloaded from mounted volume

catalog_gateway/
├── policy.py        # JWT validation (PyJWKClient) + async OPA client
├── main.py          # FastAPI proxy — all enforcement happens here
├── requirements.txt
└── Dockerfile
```

**OPA query protocol:**

```
POST /v1/data/iceberg/policy
Input:  { "input": { "principal": str, "namespace": str, "table": str, "operation": str } }
Output: { "result": { "allow": bool, "excluded_columns": [...], "row_filter": str|null, "column_masks": {...} } }
```

**Rego evaluation model:**

```rego
package iceberg
import rego.v1

# Single document — one OPA call returns all enforcement data
policy := {
    "allow":            allow,
    "excluded_columns": excluded_columns,
    "row_filter":       row_filter,
    "column_masks":     column_masks,
}

default allow := false

# Trusted internals: unrestricted
allow if input.principal in {"admin-client", "sync-service", "catalog-gateway"}

# analytics-client: gold namespace, read-only
allow if {
    input.principal == "analytics-client"
    input.namespace == "gold"
    input.operation in {"READ", "LIST", "SCAN"}
}
```

**Enforcement layers in `catalog_gateway/main.py`:**

| Layer | Trigger | How |
|-------|---------|-----|
| 1 — Allow/Deny | Every table request | `if not dec.allow: raise HTTPException(403)` |
| 4 — Column exclusion | `GET /tables/{t}` | `_strip_columns(meta, excluded)` removes fields from schema JSON |
| 2 — Row filter | `POST /tables/{t}/scan` | `_sql_to_iceberg_expr()` converts SQL → Iceberg expr, merged with AND |
| 3 — Column masks | `GET /tables/{t}` | Stored as `gateway.column-masks` table property (advisory) |

**Hot reload:** OPA watches the `/policies` volume mount. Edit any `.rego` file → OPA
reloads within seconds. No container restart needed.

**Stateless:** Every request calls OPA fresh. No in-memory contract cache in the gateway.
OPA itself caches compiled policy bundles — evaluation is sub-millisecond.

---

## 13. Key Design Decisions

### No PyIceberg / No JVM

PyIceberg ties client versions to Iceberg spec versions. This caused `NessieCatalog 0.79.0`
vs `Nessie server latest` incompatibilities in practice. Using `fastavro` directly gives
full control over Avro I/O with zero version coupling.

### Manifest-Based Diff (Not Directory Diff)

The diff compares files referenced in the **manifest chain** against files present on the
target. This correctly handles:
- Tables with a custom `write.data.path` outside the table root
- Iceberg v2 equality/positional delete files
- Files that exist in the target directory but belong to a different table

### Optimistic Metadata Write

Data files are copied first. Metadata chain is rewritten last. If copy fails halfway,
the target retains its previous valid metadata state — it never points at missing files.

### Infrastructure Enforcement via Catalog Gateway

Policy is enforced at the HTTP protocol layer by the catalog-gateway — not in application
code. This means:
- Any Spark/Trino/Python client using the Iceberg REST catalog is enforced automatically
- Clients cannot bypass enforcement by skipping an API call — enforcement is on the path
- Nessie's port 19120 is not exposed to the host — the gateway is the sole entry point
- Fail-closed: gateway returns 403 if OPA is unreachable

### Iceberg REST Catalog, Not Nessie Native API

The gateway exposes the standard Iceberg REST Catalog protocol (`/iceberg/v1`).
Clients use `type: rest` — no Nessie-specific extensions or JARs needed.
Nessie's native `/api/v2` is still used internally by the `iceberg-sync` CLI.

### Docker-Internal Issuer URL

The JWT `iss` claim uses `http://oauth-service:8081` (Docker-internal hostname) so Nessie
can resolve it for OIDC discovery. External callers reach the service at
`http://localhost:8081` (port-mapped) but this is only for getting tokens — the `iss`
consistency is what matters for Nessie's JWT validation.

---

## 14. Troubleshooting

### Nessie never appears in `docker compose ps`

**Symptom:** `docker compose ps` shows postgres, oauth-service, opa, minio — but no `nessie` row.

**Cause:** Nessie has `depends_on: oauth-service: condition: service_healthy`. If oauth-service
is `unhealthy`, Docker never starts Nessie.

**Diagnosis:**

```bash
docker compose ps
# Look for (unhealthy) next to oauth-service or catalog-gateway

docker compose logs oauth-service --tail=30
docker compose logs catalog-gateway --tail=30
docker compose logs opa --tail=30
```

**Fix:** Resolve the upstream service issue, then:

```bash
docker compose up -d   # starts Nessie now that dependencies are healthy
```

---

### oauth-service / catalog-gateway stays `unhealthy` forever

**Symptom:** Service is `Up` and serving requests correctly, but `docker compose ps` shows `(unhealthy)`.

**Cause:** The health check uses `curl`, which is **not installed** in `python:3.12-slim`.
The health check command silently fails with "not found", marking the service unhealthy.

**Fix:** Health checks must use Python's built-in `urllib` (already fixed in `docker-compose.yml`):

```yaml
healthcheck:
  test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8081/health')"]
```

If you see this after pulling the repo: rebuild with `--no-cache`:

```bash
docker compose build --no-cache oauth-service catalog-gateway
docker compose up -d
```

---

### `passlib` / bcrypt `ValueError: password cannot be longer than 72 bytes`

**Symptom:** oauth-service crashes on startup with a stack trace ending in:
```
ValueError: password cannot be longer than 72 bytes, truncate manually if necessary
```

**Cause:** `passlib 1.7.4` is incompatible with `bcrypt >= 4.0`. Passlib's internal
bug-detection code (`detect_wrap_bug`) tries to hash a 72-byte test string during
backend initialization, and bcrypt 4.x raises `ValueError` instead of silently truncating.

**Fix (already applied):** `requirements.txt` uses `bcrypt==4.2.0` directly (passlib removed).
Secrets are hashed with `bcrypt` + SHA-256 pre-hash via `_hash_secret()` in `main.py`.

If you see this after upgrading dependencies, force a full rebuild:

```bash
docker compose down -v
docker compose build --no-cache oauth-service
docker compose up -d
```

---

### `AttributeError: module 'bcrypt' has no attribute '__about__'`

Same root cause as above — `passlib` trying to read the `bcrypt` module version.
Resolved by removing `passlib` entirely. See fix above.

---

### Nessie returns `401 Unauthorized` on all requests

**Possible causes and fixes:**

| Cause | Fix |
|-------|-----|
| oauth-service was unhealthy when Nessie started; OIDC init failed | `docker compose restart nessie` |
| Token expired (default: 1 hour) | Fetch a new token |
| Wrong `client_id` / `client_secret` | Check the four default clients in [docs/oauth-setup.md](docs/oauth-setup.md) |
| RSA key was rotated (row deleted from `key_store` table) | Restart Nessie to reload JWKS |

**Verify the token is valid:**

```bash
# Decode the JWT payload (no signature check — just inspect claims)
TOKEN=eyJ...your_token...
echo $TOKEN | cut -d. -f2 | base64 -d 2>/dev/null | jq '{iss, aud, exp, sub}'
# iss must be "http://oauth-service:8081"
# aud must contain "nessie-server"
# exp must be in the future (Unix timestamp)
```

---

### Nessie health check loops forever without starting

**Symptom:** Logs show `/q/health/ready` returning `200` every 15 seconds indefinitely,
but no "Nessie server started" message.

**Explanation:** This is normal. `/q/health/ready` returning `200` **means Nessie is running**.
The repeated health check lines are Docker polling the container to maintain its `healthy`
status. Nessie does not print a "started" banner after the initial startup message.

**Verify Nessie is actually serving requests:**

```bash
TOKEN=$(curl -s -X POST http://localhost:8081/token \
  -d "grant_type=client_credentials&client_id=admin-client&client_secret=admin-secret" \
  | jq -r .access_token)

curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:19120/api/v2/trees/main | jq .reference.name
# "main" = Nessie is fully operational
```

---

### `docker compose down -v` vs `docker compose down`

| Command | Effect |
|---------|--------|
| `docker compose down` | Stops and removes containers; **keeps** volumes (PostgreSQL data survives) |
| `docker compose down -v` | Stops containers **and deletes volumes** — PostgreSQL is wiped, all clients/keys re-seeded on next `up` |

Use `-v` when:
- Switching from passlib hashes to bcrypt hashes (incompatible hash format)
- Changing the PostgreSQL `init-dbs.sh` init script
- Starting fresh after a failed first-time setup

---

## 15. Common Pitfalls

### `TypeError: unexpected keyword argument 'region_name'`

Adding `region_name` unconditionally to `source_kwargs` breaks ADLS sources. Always check
the source URI scheme before building kwargs — see `_build_sync()` in `cli.py`.

### `'bool' object is not callable`

Naming a stored flag the same as an existing method (e.g. `self._write_version_hint = True`
shadows `def _write_version_hint()`). Append `_flag` to stored booleans that share a name
with methods.

### Nessie `400 Bad Request: no content ID`

When updating an existing table key in Nessie, the PUT payload must include the `id` field
from the current content object. `register_table()` handles this automatically.

### Nessie `404` on table contents URL with slashes

Nessie content keys are dot-separated, not slash-separated:
```
/api/v2/trees/main/contents/gold.top_customers   ✓
/api/v2/trees/main/contents/gold/top_customers   ✗ (404)
```

### Nessie `401` when authentication is enabled

If Nessie starts before `oauth-service` is healthy, Quarkus OIDC fails to fetch the JWKS
and Nessie may reject all tokens. The `depends_on: oauth-service: condition: service_healthy`
in `docker-compose.yml` prevents this, but after a force-restart you may need to:
```bash
docker compose restart nessie
```

### OPA unreachable → gateway returns 500

If OPA is down, `get_policy()` raises an HTTP error which the gateway propagates as 500.
The gateway never fails open — if OPA cannot be reached, the request fails. If your sync
is unexpectedly failing with 500, check:
```bash
curl -s http://localhost:8181/health
docker compose logs opa
docker compose logs catalog-gateway
```

### RSA key rotation

The oauth-service stores the RSA key pair in PostgreSQL. If you delete the `key_store`
row and restart, a new key is generated. Nessie's JWKS cache will have the old public key
and all existing tokens will fail validation until the cache refreshes (typically < 5 min).
During key rotation: let both keys coexist or restart Nessie after key change.

### ADLS Cross-Tenant Auth (`AADSTS500212`)

`DefaultAzureCredential` is blocked by some Azure AD admin policies when accessing storage
from a different tenant. Pass `--source-secret-key` with the storage account key to use
account-key auth instead.

### ADLS `--source-root` Missing Inner Subfolder

If blobs live at `container/iceberg/gold/...`, the root must include the inner path:
```
abfss://iceberg@account.dfs.core.windows.net/iceberg   ✓
abfss://iceberg@account.dfs.core.windows.net/           ✗
```

### Spark warehouse trailing slash

Spark's Hadoop catalog strips trailing slashes from the warehouse URI then concatenates
namespace names directly, giving `s3a://warehouseaws/` instead of `s3a://warehouse/aws/`.

Always set warehouse **without** a trailing slash:
```
spark.sql.catalog.aws.warehouse = s3a://warehouse/aws    ✓
spark.sql.catalog.aws.warehouse = s3a://warehouse/aws/   ✗
```
