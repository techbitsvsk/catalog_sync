# Developer Guide — iceberg-catalog-sync

This guide explains the internal architecture, module responsibilities, data
flows, and extension points of `iceberg-catalog-sync`.  Read this before
making changes to the codebase.

---

## Table of Contents

1. [Why This Tool Exists](#1-why-this-tool-exists)
2. [Project Layout](#2-project-layout)
3. [Core Concepts](#3-core-concepts)
4. [Module Reference](#4-module-reference)
   - [PathTranslator](#41-pathtranslator)
   - [StorageBackend](#42-storagebackend)
   - [CatalogSync](#43-catalogsync)
   - [MetadataReader](#44-metadatareader)
   - [MetadataRewriter](#45-metadatarewriter)
   - [NessieCatalog](#46-nessiecatalog)
   - [CLI](#47-cli)
   - [Airflow Operators](#48-airflow-operators)
5. [Full Data Flow](#5-full-data-flow)
6. [Local Development Setup](#6-local-development-setup)
7. [Running Tests](#7-running-tests)
8. [Adding a New Storage Backend](#8-adding-a-new-storage-backend)
9. [Adding a New Catalog Integration](#9-adding-a-new-catalog-integration)
10. [Key Design Decisions](#10-key-design-decisions)
11. [Common Pitfalls](#11-common-pitfalls)

---

## 1. Why This Tool Exists

Apache Iceberg embeds **absolute storage URIs** at every level of its metadata
chain:

```
metadata.json          →  location: s3://src-bucket/gold/top_customers
  └─ manifest-list.avro  →  manifest_path: s3://src-bucket/.../snap-1.avro
       └─ manifest.avro  →  data_file.file_path: s3://src-bucket/.../0001.parquet
```

Copying the Parquet data files to another cloud without rewriting every URI
leaves the target table broken — all internal pointers still resolve back to
the source cloud.

`iceberg-catalog-sync` solves this by:
1. Discovering the Iceberg metadata chain on the source.
2. Incrementally copying only new data files.
3. Rewriting every embedded URI in every metadata/manifest file using a
   configurable `PathTranslator`.
4. Optionally registering the rewritten table in a REST catalog (Nessie).

There is **no JVM dependency** — Avro manifests are read and written with
`fastavro` (pure Python).

---

## 2. Project Layout

```
iceberg-catalog-sync/
├── src/iceberg_sync/
│   ├── __init__.py              # package version
│   ├── cli.py                   # Click CLI entry point
│   ├── path_translator.py       # URI translation engine
│   ├── sync/
│   │   └── catalog_sync.py      # main orchestrator (CatalogSync)
│   ├── storage/
│   │   ├── base.py              # StorageBackend ABC + FileInfo
│   │   ├── factory.py           # create_storage() dispatcher
│   │   ├── s3.py                # AWS S3 / MinIO / S3-compatible
│   │   ├── adls.py              # Azure ADLS Gen2
│   │   ├── gcs.py               # Google Cloud Storage
│   │   └── memory.py            # In-memory backend (tests only)
│   ├── metadata/
│   │   ├── reader.py            # manifest-chain walker
│   │   └── rewriter.py          # URI rewrite + version-hint commit
│   ├── catalog/
│   │   └── nessie.py            # Nessie v2 native API client
│   └── airflow/
│       └── operators.py         # Airflow operator wrappers
├── airflow_dags/
│   └── iceberg_sync_dag.py      # DAG factory + pipeline registry
├── docker/
│   └── docker-compose.yml       # Nessie + MinIO local stack
├── examples/
│   └── adls_to_nessie_poc.py    # end-to-end POC script
├── tests/
│   ├── test_path_translator.py
│   └── test_metadata_rewrite.py
└── pyproject.toml
```

---

## 3. Core Concepts

### Iceberg Metadata Chain

An Iceberg table has four layers, each referencing the next by absolute URI:

| Layer | Format | Contains |
|-------|--------|---------|
| `metadata.json` | JSON | `location`, `snapshots[].manifest-list`, `metadata-log[]` |
| manifest-list (`snap-NNN.avro`) | Avro | one record per manifest: `manifest_path` |
| manifest (`m-NNN.avro`) | Avro | one record per data file: `data_file.file_path` |
| data file (`*.parquet`) | Parquet | actual table data |

All four layers must be on the target, and **every URI field** in the top three
layers must be rewritten to point at the target storage — otherwise any query
engine will follow the chain back to the source cloud.

### Snapshot vs. All-Snapshot Mode

By default only the **current snapshot** is synced (the one pointed to by
`current-snapshot-id` in `metadata.json`). Pass `--all-snapshots` / set
`rewrite_all_snapshots=True` to copy and rewrite every snapshot in the
metadata log — necessary for time-travel queries.

### Hadoop Catalog vs. REST Catalog

- **Hadoop catalog** uses `version-hint.text` as the atomic pointer to the
  latest metadata file.  The rewriter writes this file last so the target is
  always in a valid state.
- **REST catalog** (Nessie, Polaris, Glue) owns the pointer.  When
  `--nessie-uri` is provided, `version-hint.text` is intentionally skipped
  and the CLI calls `NessieCatalog.register_or_update()` after the file sync
  completes.

### URI Schemes in Use

| Scheme | Backend | Notes |
|--------|---------|-------|
| `s3://` | S3StorageBackend | AWS S3 (native) |
| `s3a://` | S3StorageBackend | Hadoop-style S3 (MinIO, Ceph, on-prem) |
| `s3n://` | S3StorageBackend | Legacy Hadoop S3 |
| `abfss://` | ADLSStorageBackend | Azure ADLS Gen2 (TLS) |
| `abfs://` | ADLSStorageBackend | Azure ADLS Gen2 (no TLS) |
| `gs://` | GCSStorageBackend | Google Cloud Storage |

`create_storage()` in `storage/factory.py` dispatches based on the URI scheme.

---

## 4. Module Reference

### 4.1 PathTranslator

**File:** `src/iceberg_sync/path_translator.py`

The single source of truth for URI translation.  All rewrites in the
metadata chain ultimately call `PathTranslator.translate()`.

```
source_root  =  "s3://my-bucket/iceberg/"
target_root  =  "s3a://warehouse/aws"

translate("s3://my-bucket/iceberg/gold/top_customers/data/0001.parquet")
    → "s3a://warehouse/aws/gold/top_customers/data/0001.parquet"
```

**Key behaviours:**

- Roots are **normalised** to always have a trailing slash internally.
- The mapping list is **ordered** — first match wins.  This lets you map
  sub-prefixes before more general prefixes.
- `strict=True` (default) raises `ValueError` for unmapped URIs.
  `strict=False` returns the URI unchanged — used when rewriting
  `metadata-log` entries that may reference files outside the table root.
- `reverse()` returns a new `PathTranslator` with source and target swapped,
  enabling symmetric failback.

**Convenience factories:**

```python
from iceberg_sync.path_translator import PathTranslator

t = PathTranslator.aws_to_minio(
    s3_bucket="my-bucket",
    s3_prefix="iceberg/",
    minio_bucket="warehouse",
    minio_prefix="aws",
)
```

---

### 4.2 StorageBackend

**File:** `src/iceberg_sync/storage/base.py`

Abstract base class every backend must implement:

```python
class StorageBackend(ABC):
    def list_objects(self, prefix: str) -> Iterator[FileInfo]: ...
    def read_bytes(self, uri: str) -> bytes: ...
    def write_bytes(self, uri: str, data: bytes) -> None: ...
    def exists(self, uri: str) -> bool: ...
    def delete(self, uri: str) -> None: ...
    def copy_from(self, source: StorageBackend, source_uri: str, target_uri: str) -> int: ...
```

`copy_from` is the hot path for large data files.  Each backend provides an
optimised implementation (e.g. S3 uses `boto3` streaming copy rather than
downloading to memory).

`FileInfo` carries `uri`, `relative_path`, `size_bytes`, and an optional
`etag`.

**Factory:**

```python
from iceberg_sync.storage import create_storage

# S3 / MinIO
src = create_storage("s3://my-bucket/iceberg/",
                      region_name="eu-west-2")

# MinIO (S3-compatible)
tgt = create_storage("s3a://warehouse/",
                      endpoint_url="http://localhost:9000",
                      aws_access_key_id="minioadmin",
                      aws_secret_access_key="minioadmin")

# Azure ADLS Gen2
adls = create_storage("abfss://iceberg@acct.dfs.core.windows.net/",
                       storage_account_name="acct",
                       storage_account_key="<key>")
```

**In-memory backend (tests):**

```python
from iceberg_sync.storage.memory import MemoryStorageBackend

mem = MemoryStorageBackend("mem://test-bucket/")
mem.write_bytes("mem://test-bucket/data/file.parquet", b"...")
assert mem.exists("mem://test-bucket/data/file.parquet")
```

---

### 4.3 CatalogSync

**File:** `src/iceberg_sync/sync/catalog_sync.py`

The main orchestrator.  Constructed once and reused for multiple tables.

```python
from iceberg_sync.sync import CatalogSync

sync = CatalogSync(
    translator=translator,          # PathTranslator
    source_storage=src_backend,     # StorageBackend
    target_storage=tgt_backend,     # StorageBackend
    max_parallel_copies=4,          # thread pool size
    rewrite_all_snapshots=False,    # True = time-travel support
    write_version_hint=True,        # False when REST catalog owns pointer
)
```

**sync_table()**

```
table_root (e.g. s3://bucket/iceberg/gold/top_customers/)
   │
   ├─ find_latest_metadata()            # version-hint.text → or scan
   ├─ read_bytes(metadata.json)         # JSON parse
   ├─ read_snapshot_data_files()        # walk manifest chain
   ├─ list_objects(target table_root)   # existing target files
   │
   ├─ [diff] files_to_copy = source_files - target_files
   │
   ├─ _copy_files_by_uri()             # parallel thread pool
   │   └─ target.copy_from(source, ...)
   │
   ├─ MetadataRewriter.rewrite_table() # translate all URIs
   └─ returns SyncResult
```

**sync_namespace()**

Scans the namespace root for subdirectories containing a `metadata/`
directory, then calls `sync_table()` for each one — in parallel if
`max_parallel_copies > 1`.

**cleanup_orphan_files()**

Reads all snapshots (not just current) to build the full referenced-file
set, then deletes any target file not in that set.  Always dry-run first.

**SyncResult fields:**

| Field | Type | Description |
|-------|------|-------------|
| `table` | str | source table root URI |
| `files_copied` | int | data files actually transferred |
| `files_skipped` | int | already present on target |
| `bytes_copied` | int | total bytes transferred |
| `duration_seconds` | float | wall-clock time |
| `target_metadata_uri` | str | rewritten metadata.json URI on target |
| `rewrite_stats` | RewriteStats | metadata rewrite counters |
| `errors` | List[str] | non-fatal per-file errors |
| `success` | property | True if no errors |

---

### 4.4 MetadataReader

**File:** `src/iceberg_sync/metadata/reader.py`

Reads the Iceberg manifest chain **without modifying anything**.

```python
from iceberg_sync.metadata.reader import read_snapshot_data_files

files = read_snapshot_data_files(
    storage=source_backend,
    metadata=metadata_dict,          # parsed metadata.json
    table_root="s3://bucket/gold/top_customers/",
)
# Returns: Dict[relative_path → FileInfo]
```

**Iceberg v1 vs v2 handling:**

- v1 manifests: `data_file` is a **flat** record at the top level.
- v2 manifests: `data_file` is a **nested struct** inside each row.

The reader handles both formats automatically by inspecting `format-version`
in `metadata.json`.

**Content types:**

Pass `content_types={0}` to restrict to DATA files only (skip positional /
equality delete files).  Default includes all content types.

---

### 4.5 MetadataRewriter

**File:** `src/iceberg_sync/metadata/rewriter.py`

Reads metadata from source, rewrites all URIs, writes to target.

```python
from iceberg_sync.metadata.rewriter import MetadataRewriter

rw = MetadataRewriter(
    translator=translator,
    source_storage=src,
    target_storage=tgt,
    rewrite_all_snapshots=False,
    write_version_hint_flag=True,
)
stats = rw.rewrite_table(source_metadata_uri)
```

**Rewrite chain (in order):**

1. `_rewrite_manifest(source_avro_uri)` — translate `data_file.file_path`
   for every row; write result to target at translated URI.
2. `_rewrite_manifest_list(source_avro_uri)` — translate `manifest_path`
   for every row; write result to target.
3. `rewrite_table(source_metadata_uri)` — translate `location` and
   `snapshots[].manifest-list`; capture `metadata-log` entries; write
   `metadata.json` to target.
4. `_rewrite_historical_metadata()` — called for each `metadata-log` entry
   so Iceberg 1.4+ can reconstruct `lastAddedSchemaId`.  Skipped if the
   translated target already exists.
5. `_write_version_hint()` — writes `version-hint.text` to target (skipped
   when `write_version_hint_flag=False`).

**Why fastavro, not PyIceberg?**

PyIceberg requires the Iceberg Java spec version to match the server.
fastavro reads and writes raw Avro bytes with no version coupling — the
rewriter acts as a byte-level URI substitution engine, not a catalog client.

---

### 4.6 NessieCatalog

**File:** `src/iceberg_sync/catalog/nessie.py`

Thin REST client for the **Nessie native v2 API** (`/api/v2`).

> **Important:** Do not use `/iceberg/v1` — it requires additional
> configuration in Nessie `latest` and returns 404 on most deployments.
> All operations go through `/api/v2`.

**Commit model (optimistic concurrency):**

Every write is a two-step operation:
1. `GET /api/v2/trees/{ref}` — fetch current branch hash.
2. `POST /api/v2/trees/{ref}@{hash}/history/commit` — commit with the hash
   as part of the URL path.  Nessie rejects the commit if another writer
   changed the branch between steps 1 and 2 (returns 409).

**Content ID requirement:**

When updating an existing key, the PUT payload **must** include the
existing content's `id` (a UUID Nessie assigns on first insert).  If the
`id` is missing, Nessie returns `400 Bad Request: no content ID`.

`register_table()` handles this automatically by fetching the current content
before committing the update.

**Key URL format:**

Table keys use **dot-separated** elements, not slashes:
```
GET /api/v2/trees/main/contents/gold.top_customers   ✓
GET /api/v2/trees/main/contents/gold/top_customers   ✗ (404)
```

---

### 4.7 CLI

**File:** `src/iceberg_sync/cli.py`

Click group with two commands: `table` and `namespace`.

```
iceberg-sync [--verbose]
    table     --table <path>     [common options]
    namespace --namespace <path> [common options]
```

**Common options of note:**

| Option | Purpose |
|--------|---------|
| `--source-root` | Source warehouse root URI |
| `--target-root` | Target warehouse root URI |
| `--metadata-location` | Skip filesystem discovery; use this metadata.json |
| `--nessie-uri` | Register in Nessie after sync; also disables version-hint.text |
| `--all-snapshots` | Rewrite all historical snapshots (time-travel) |
| `--dry-run` | Plan only — no writes |

**Scheme-based dispatch in `_build_sync()`:**

`source_kwargs` and `target_kwargs` are built differently depending on the
URI scheme:

- `s3 / s3a / s3n` → `region_name`, `endpoint_url`, `aws_access_key_id`, etc.
- `abfss / abfs` → `storage_account_name` (parsed from URI), `storage_account_key`
- `gs` → `project` (optional)

This is why adding `region_name` to an ADLS source causes a `TypeError` — the
key check must come before building kwargs.

---

### 4.8 Airflow Operators

**File:** `src/iceberg_sync/airflow/operators.py`

Four operators, all inheriting from `airflow.models.BaseOperator`:

| Operator | execute() returns | XCom key |
|----------|-------------------|----------|
| `IcebergTableSyncOperator` | dict summary | `sync_result` |
| `IcebergNamespaceSyncOperator` | list of dicts | `sync_results` |
| `IcebergHealthCheckOperator` | dict with `leaked_uris` count | `health_result` |
| `NessieCatalogRegisterOperator` | dict with `action` field | `nessie_result` |

**XCom chaining example:**

```python
sync = IcebergTableSyncOperator(task_id="sync", ...)
health = IcebergHealthCheckOperator(task_id="health", ...)
register = NessieCatalogRegisterOperator(
    task_id="nessie",
    # pulls target_metadata_uri from sync task's XCom automatically
    metadata_location="{{ task_instance.xcom_pull('sync')['target_metadata_uri'] }}",
    ...
)
sync >> health >> register
```

**DAG factory (`airflow_dags/iceberg_sync_dag.py`):**

`make_iceberg_sync_dag(cfg: SyncPipelineConfig)` generates a complete DAG
with parallel per-table chains.  Add a new pipeline by appending to the
`SYNC_PIPELINES` list — no boilerplate DAG code needed.

---

## 5. Full Data Flow

```
User / Airflow
    │
    ▼
CLI: iceberg-sync table --table gold/top_customers ...
    │
    ├─ _build_sync()
    │   ├─ PathTranslator([(source_root, target_root)])
    │   ├─ create_storage(source_root) → S3StorageBackend
    │   └─ create_storage(target_root) → S3StorageBackend (MinIO)
    │
    └─ CatalogSync.sync_table(table_root)
        │
        ├─ 1. DISCOVER
        │   └─ find_latest_metadata(source, table_root)
        │       ├─ read version-hint.text  (fast path)
        │       └─ scan metadata/ directory (fallback)
        │
        ├─ 2. DIFF
        │   ├─ read_snapshot_data_files(source, metadata)
        │   │   └─ source.read_bytes(manifest-list.avro)
        │   │       └─ source.read_bytes(manifest.avro)  [for each]
        │   └─ target.list_objects(table_root) → existing set
        │
        ├─ 3. COPY  [parallel thread pool]
        │   └─ target.copy_from(source, src_uri, tgt_uri)  [for each new file]
        │
        ├─ 4. REWRITE
        │   └─ MetadataRewriter.rewrite_table(metadata_uri)
        │       ├─ for each manifest:   read Avro → translate URIs → write Avro
        │       ├─ for each manifest-list: read Avro → translate → write Avro
        │       ├─ translate metadata.json (location, snapshot pointers)
        │       ├─ rewrite historical metadata (metadata-log entries)
        │       └─ write version-hint.text  (skipped for REST catalog targets)
        │
        ├─ 5. RETURN SyncResult
        │   └─ target_metadata_uri = translated metadata.json URI
        │
        └─ 6. REGISTER  [only if --nessie-uri provided]
            └─ NessieCatalog.register_or_update(namespace, table, metadata_uri)
                ├─ GET /api/v2/trees/main  (fetch hash)
                ├─ GET /api/v2/trees/main/contents/gold.top_customers
                └─ POST /api/v2/trees/main@{hash}/history/commit
```

---

## 6. Local Development Setup

```bash
# 1. Clone
git clone <repo> iceberg-catalog-sync
cd iceberg-catalog-sync

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install in editable mode with all dev extras
pip install -e ".[nessie,dev]"

# 4. Verify CLI
iceberg-sync --help

# 5. Start the local stack (Nessie + MinIO)
docker compose -f docker/docker-compose.yml up -d

# 6. Verify stack
curl -s http://localhost:19120/q/health/ready | python -m json.tool
curl -s http://localhost:9000/minio/health/live    # empty 200 OK
```

**MinIO console:** http://localhost:9001 (minioadmin / minioadmin)

**Warehouse layout on MinIO:**

```
s3a://warehouse/
├── aws/          ← target root for AWS S3 → MinIO syncs
│   └── gold/
│       └── top_customers/
│           ├── data/
│           └── metadata/
└── azure/        ← target root for ADLS → MinIO syncs
    └── gold/
        └── top_customers/
            ├── data/
            └── metadata/
```

---

## 7. Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=iceberg_sync --cov-report=term-missing

# Single module
pytest tests/test_path_translator.py -v

# Single test
pytest tests/test_metadata_rewrite.py::test_full_rewrite_chain -v
```

Tests use `MemoryStorageBackend` — no cloud credentials or network required.

**Test structure:**

```
tests/
├── test_path_translator.py    # URI translation logic
└── test_metadata_rewrite.py   # full metadata chain rewrite
```

`test_metadata_rewrite.py` builds a realistic in-memory Iceberg table
(metadata.json + manifest-list + manifest + fake Parquet) and verifies every
URI is correctly translated after `rewrite_table()`.

---

## 8. Adding a New Storage Backend

1. Create `src/iceberg_sync/storage/<name>.py`.
2. Subclass `StorageBackend` and implement all six abstract methods.
3. Register the scheme in `storage/factory.py`:

```python
# storage/factory.py
from iceberg_sync.storage.mybackend import MyStorageBackend

_SCHEME_MAP = {
    ...
    "myscheme": MyStorageBackend,
}
```

4. Add any new dependencies to `pyproject.toml` under `[project.dependencies]`
   or a new optional group.
5. Add scheme-based kwargs dispatch in `cli.py → _build_sync()`.
6. Add tests using `MemoryStorageBackend` as a reference.

**Checklist:**

- [ ] `list_objects()` must yield every object recursively, not just top-level.
- [ ] `copy_from()` should stream bytes, not buffer the whole file in memory.
- [ ] Use retries/backoff for transient network errors (see S3 backend for example).
- [ ] Normalise the root URI the same way as other backends (trailing slash).

---

## 9. Adding a New Catalog Integration

1. Create `src/iceberg_sync/catalog/<name>.py`.
2. Implement at minimum:
   - `register_or_update(namespace, table, metadata_location) → dict`
   - `ping() → bool`
3. Add a CLI option in `cli.py` (similar to `--nessie-uri`).
4. Call your catalog client after `sync.sync_table()` returns a successful
   result, passing `result.target_metadata_uri`.

No base class is required — duck typing is used intentionally so different
catalog APIs (REST, gRPC, JDBC) don't share an artificial interface.

---

## 10. Key Design Decisions

### No PyIceberg / No JVM

PyIceberg ties Iceberg client versions to Iceberg spec versions.  This caused
`NessieCatalog 0.79.0` vs `Nessie server latest` incompatibilities in
practice.  Using `fastavro` directly gives full control over Avro I/O with
zero version coupling.

### Manifest-Based Diff (Not Directory Diff)

The diff compares files referenced in the **manifest chain** against files
present on the target.  This correctly handles:
- Tables with a custom `write.data.path` outside the table root.
- Iceberg v2 equality/positional delete files.
- Files that exist in the target directory but belong to a different table.

### Optimistic Metadata Write

Data files are copied first.  The metadata chain is rewritten last.  If the
copy fails halfway, the target retains its previous valid metadata state —
it never points at files that don't exist.

### Separate MinIO Prefixes for Multi-Source

When syncing from both Azure and AWS to the same MinIO instance, using
separate prefixes (`warehouse/azure/` and `warehouse/aws/`) avoids namespace
collisions and lets Spark launch two separate Hadoop catalogs pointing at
each prefix independently.

### Nessie Native API, Not Iceberg REST

`/iceberg/v1` on Nessie `latest` returns 404 unless the Iceberg REST
compatibility layer is explicitly enabled via QUARKUS config.  The native
`/api/v2` API works out of the box and is more expressive (commit operations
with full message metadata, branch management, etc.).

---

## 11. Common Pitfalls

### `TypeError: unexpected keyword argument 'region_name'`

Adding `region_name` unconditionally to `source_kwargs` breaks ADLS sources.
Always check the source URI scheme before building kwargs — see
`_build_sync()` in `cli.py`.

### `'bool' object is not callable`

Naming a stored flag the same as an existing method (e.g.
`self._write_version_hint = True` shadows `def _write_version_hint()`).
Append `_flag` to stored booleans that share a name with methods.

### Nessie `400 Bad Request: no content ID`

When updating an existing table key in Nessie, the PUT payload must include
the `id` field from the current content object.  Fetch the current content
first with `GET /contents/{key}` and copy the `id` into the new payload.

### Nessie `404` on table contents URL with slashes

Nessie content keys are **dot-separated**, not slash-separated:
```
/api/v2/trees/main/contents/gold.top_customers   ✓
/api/v2/trees/main/contents/gold/top_customers   ✗
```

### `Cannot set last added schema: no schema has been added` (Spark 1.4+)

Iceberg 1.4+ reads the `metadata-log` entries in `metadata.json` to
reconstruct `lastAddedSchemaId`.  If the historical `v1.metadata.json` /
`v2.metadata.json` files are not on the target, Spark throws this error.
`MetadataRewriter._rewrite_historical_metadata()` handles this — do not skip
`metadata-log` processing.

### Spark warehouse trailing slash

Spark's Hadoop catalog strips trailing slashes from the warehouse URI then
concatenates namespace names directly, giving paths like
`s3a://warehouseaws/gold` instead of `s3a://warehouse/aws/gold`.

Always set warehouse **without** a trailing slash:
```
spark.sql.catalog.aws.warehouse = s3a://warehouse/aws    ✓
spark.sql.catalog.aws.warehouse = s3a://warehouse/aws/   ✗
```

### ADLS Cross-Tenant Auth (`AADSTS500212`)

`DefaultAzureCredential` is blocked by some Azure AD admin policies when
accessing storage from a different tenant.  Pass `--source-secret-key` with
the storage account key to use account-key auth instead of OAuth.

### ADLS `--source-root` Missing Inner Subfolder

If blobs live at `container/iceberg/gold/...` inside the `iceberg`
container, the root must include the inner path:
```
abfss://iceberg@acct.dfs.core.windows.net/iceberg   ✓
abfss://iceberg@acct.dfs.core.windows.net/           ✗  (misses the iceberg/ prefix)
```
