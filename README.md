# iceberg-catalog-sync

Platform-agnostic Iceberg table replication across cloud storage backends.
Solves the fundamental problem: **Iceberg metadata contains absolute storage URIs** —
copying files from S3 to ADLS without rewriting metadata leaves the target pointing
back at the source.

This tool handles the full Iceberg metadata chain:

```
metadata.json  →  manifest-list (Avro)  →  manifest (Avro)  →  data files (Parquet)
     ↓                   ↓                       ↓
  "location":        "manifest_path":         "file_path":
  "s3://..."         "s3://..."               "s3://..."
     ↓                   ↓                       ↓
  translated to      translated to            translated to
  "abfss://..."      "abfss://..."            "abfss://..."
```

## Supported Platforms

| Source | Target | Status |
|--------|--------|--------|
| AWS S3 (`s3://`) | Azure ADLS Gen2 (`abfss://`) | Supported |
| AWS S3 (`s3://`) | Google Cloud Storage (`gs://`) | Supported |
| AWS S3 (`s3://`) | MinIO (`s3a://`) | Supported |
| Azure ADLS Gen2 | AWS S3 | Supported (reverse) |
| Any | Any | Works via PathTranslator mappings |

## How It Works

### The Problem

When your pipeline writes `gold.top_customers` on AWS, the Iceberg metadata looks like:

```json
{
  "location": "s3://warehouse/iceberg/gold/top_customers",
  "snapshots": [{
    "manifest-list": "s3://warehouse/iceberg/gold/top_customers/metadata/snap-123.avro"
  }]
}
```

Inside `snap-123.avro` (manifest-list), each manifest path is `s3://...`.
Inside each manifest, every data file path is `s3://...`.

If you `azcopy` this to Azure, the files are on ADLS but every internal reference
still says `s3://`. A Spark reader on Azure can't find anything.

### The Solution

`iceberg-catalog-sync` performs an **incremental sync with metadata rewrite**:

1. **Diff**: Compare data files between source and target (only new files need copying).
2. **Copy**: Transfer new Parquet files (immutable — never need re-copying).
3. **Rewrite**: Parse the full metadata chain, translate every absolute URI, write
   new metadata files on the target.
4. **Commit**: Write `version-hint.text` last (atomic pointer — target is only
   readable once everything is in place).

## Installation

```bash
pip install iceberg-catalog-sync

# With Airflow operators
pip install iceberg-catalog-sync[airflow]
```

Or from source:
```bash
git clone <repo-url>
cd iceberg-catalog-sync
pip install -e ".[dev]"
```

## Quick Start

### CLI — Sync a table from S3 to ADLS

```bash
iceberg-sync table \
    --source-root "s3://my-warehouse/iceberg/" \
    --target-root "abfss://iceberg@mystorageacct.dfs.core.windows.net/iceberg/" \
    --table "gold/top_customers" \
    --source-region eu-west-2 \
    --target-account-name mystorageacct
```

Output:
```
  Status           ✓ SUCCESS
  Table            s3://my-warehouse/iceberg/gold/top_customers/
  Files copied     42
  Files skipped    138  (already synced)
  Bytes copied     26.4 MB
  Duration         8.3s
  Metadata rewritten  1
  Manifests rewritten 3
  Paths translated    180
```

### CLI — Dry run (see what would happen)

```bash
iceberg-sync table --dry-run \
    --source-root "s3://my-warehouse/iceberg/" \
    --target-root "abfss://iceberg@account.dfs.core.windows.net/iceberg/" \
    --table "gold/revenue_by_order_date" \
    --source-region eu-west-2 \
    --target-account-name mystorageacct
```

### CLI — Sync entire namespace

```bash
iceberg-sync namespace \
    --source-root "s3://my-warehouse/iceberg/" \
    --target-root "abfss://iceberg@account.dfs.core.windows.net/iceberg/" \
    --namespace "gold" \
    --source-region eu-west-2 \
    --target-account-name mystorageacct
```

### CLI — S3 to MinIO (local testing)

```bash
iceberg-sync table \
    --source-root "s3://my-warehouse/iceberg/" \
    --target-root "s3a://local-warehouse/iceberg/" \
    --table "gold/top_customers" \
    --source-region eu-west-2 \
    --target-endpoint "http://localhost:9000" \
    --target-access-key minioadmin \
    --target-secret-key minioadmin
```

### Python API

```python
from iceberg_sync.path_translator import PathTranslator
from iceberg_sync.storage import create_storage
from iceberg_sync.sync import CatalogSync

translator = PathTranslator([
    ("s3://warehouse/iceberg/", "abfss://iceberg@acct.dfs.core.windows.net/iceberg/"),
])

sync = CatalogSync(
    translator=translator,
    source_storage=create_storage("s3", region_name="eu-west-2"),
    target_storage=create_storage("abfss", storage_account_name="acct"),
)

# Single table
result = sync.sync_table("s3://warehouse/iceberg/gold/top_customers/")
print(f"Copied {result.files_copied} files, translated {result.rewrite_stats.data_file_paths_translated} paths")

# Entire namespace
results = sync.sync_namespace("s3://warehouse/iceberg/gold/")
```

### Airflow DAG

```python
from iceberg_sync.airflow.operators import (
    IcebergTableSyncOperator,
    IcebergHealthCheckOperator,
)

sync_revenue = IcebergTableSyncOperator(
    task_id="sync_gold_revenue",
    source_root="s3://warehouse/iceberg/",
    target_root="abfss://iceberg@acct.dfs.core.windows.net/iceberg/",
    table="gold/revenue_by_order_date",
    source_storage_kwargs={"region_name": "eu-west-2"},
    target_storage_kwargs={"storage_account_name": "acct"},
)

validate = IcebergHealthCheckOperator(
    task_id="validate_revenue",
    target_root="abfss://iceberg@acct.dfs.core.windows.net/iceberg/",
    table="gold/revenue_by_order_date",
    source_scheme="s3",
    target_storage_kwargs={"storage_account_name": "acct"},
)

sync_revenue >> validate
```

See `airflow_dags/iceberg_sync_dag.py` for a complete production DAG with
tiered sync, health checks, and failover monitoring.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    CatalogSync                            │
│                                                          │
│  1. find_latest_metadata()    ← Reads version-hint.text  │
│  2. diff data files           ← Incremental (new only)   │
│  3. copy_files()              ← Parallel, cross-cloud    │
│  4. MetadataRewriter          ← Full chain rewrite       │
│     ├── rewrite metadata.json (JSON)                     │
│     ├── rewrite manifest-lists (Avro: manifest_path)     │
│     └── rewrite manifests (Avro: data_file.file_path)    │
│  5. write version-hint.text   ← Atomic commit pointer    │
└──────────────────────────────────────────────────────────┘
         │                              │
    PathTranslator              StorageBackend (abstract)
    s3:// → abfss://            ├── S3StorageBackend
    s3:// → gs://               ├── ADLSStorageBackend
    (any → any)                 └── GCSStorageBackend
```

## Project Structure

```
iceberg-catalog-sync/
├── src/iceberg_sync/
│   ├── __init__.py
│   ├── path_translator.py      Core: URI translation
│   ├── cli.py                  CLI entry point
│   ├── storage/
│   │   ├── base.py             Abstract StorageBackend
│   │   ├── factory.py          Create backend from URI
│   │   ├── s3.py               AWS S3 / MinIO
│   │   ├── adls.py             Azure ADLS Gen2
│   │   └── gcs.py              Google Cloud Storage
│   ├── metadata/
│   │   └── rewriter.py         Iceberg metadata chain rewriter
│   ├── sync/
│   │   └── catalog_sync.py     Orchestrator: diff → copy → rewrite
│   └── airflow/
│       └── operators.py        Airflow operators + health check
├── tests/
│   └── test_path_translator.py
├── airflow_dags/
│   └── iceberg_sync_dag.py     Production DAG with failover
├── pyproject.toml
└── README.md
```

## Key Design Decisions

**Why not PyIceberg?** PyIceberg's catalog API is designed for reading and writing
tables, not for rewriting metadata at the file level. We need byte-level access to
Avro manifests to rewrite embedded paths. `fastavro` gives us that without pulling
in the full Iceberg runtime (and its JVM dependency in some configurations).

**Why rewrite, not custom FileIO?** A custom Iceberg `FileIO` that translates paths
at read time would avoid the rewrite step, but it requires every consumer (Spark,
Athena, Trino, Fabric) to load a custom plugin. Managed services like Athena and
Fabric don't support custom FileIO. Rewriting produces standard Iceberg metadata
that any reader can consume without modification.

**Why version-hint.text last?** This is the atomic commit mechanism. Iceberg's Hadoop
catalog reads `version-hint.text` to find the current `metadata.json`. By writing it
last, we ensure the target table only becomes readable once all data files and
rewritten metadata are in place. If sync fails mid-way, the target retains its
previous valid state.

**Why incremental?** Iceberg is append-only at the file level — new commits create
new data files and new metadata files; existing files are never mutated. This means
"files that exist on target with the same relative path are guaranteed identical."
We only need to copy new files, making sync proportional to change volume, not
total table size.

## Consistency Guarantees

| Guarantee | Detail |
|-----------|--------|
| **Atomic visibility** | Target table is readable only after `version-hint.text` is written. Partial syncs are invisible. |
| **No data loss** | Source is never modified. Target gets a point-in-time snapshot. |
| **Idempotent** | Re-running sync copies only new files and overwrites metadata (same result). |
| **RPO** | Equal to sync interval. Continuous sync → minutes of data loss. Nightly → up to 24 hours. |

## Limitations

- **Active-passive only**: This tool replicates from source to target. It does not
  handle bidirectional sync or conflict resolution.
- **Hadoop catalog only**: Designed for Iceberg Hadoop catalog (metadata-as-files).
  REST catalog and Glue catalog have their own sync mechanisms.
- **No schema evolution during sync**: The target gets an exact copy of the source
  metadata. Schema changes should be made on the source and synced.
- **Manifest rewrite uses fastavro**: If Iceberg changes its Avro schema for manifests,
  the rewriter may need updating. This is rare (Iceberg v2 format has been stable).
