# iceberg-catalog-sync

Platform-agnostic incremental replication of Apache Iceberg tables across cloud storage
and catalogs — with full metadata chain rewrite and optional REST catalog registration.

> The core problem: **Iceberg metadata embeds absolute storage URIs at every level.**
> Copying Parquet files from Azure to MinIO without rewriting metadata leaves the
> on-prem table pointing back at the cloud source. This tool solves that completely.

---

## Table of Contents

1. [The Problem](#1-the-problem)
2. [How It Works](#2-how-it-works)
3. [Platform Support](#3-platform-support)
4. [End-to-End Sync Flow](#4-end-to-end-sync-flow)
5. [Metadata Discovery](#5-metadata-discovery)
6. [Quick Start](#6-quick-start)
   - [Example A — Azure Fabric / ADLS → On-Prem Nessie](#example-a--azure-fabric--adls--on-prem-nessie)
   - [Example B — AWS S3 → On-Prem Nessie](#example-b--aws-s3--on-prem-nessie)
   - [Example C — AWS S3 → Azure ADLS Gen2](#example-c--aws-s3--azure-adls-gen2)
   - [Python API](#python-api)
7. [Airflow Setup](#7-airflow-setup)
8. [Architecture](#8-architecture)
9. [Consistency Guarantee](#9-consistency-guarantee)
10. [Enterprise Grade](#10-enterprise-grade)
11. [Design Decisions](#11-design-decisions)
12. [Limitations](#12-limitations)
13. [Project Structure](#13-project-structure)

---

## 1. The Problem

When Azure Fabric (or Spark, AWS Glue, etc.) writes an Iceberg table, the metadata
chain contains the source cloud URI at every level:

```mermaid
flowchart LR
    subgraph Source["Source — Azure ADLS Gen2"]
        M1["metadata.json\nlocation: abfss://iceberg@acct.dfs.core.windows.net/gold/top_customers"]
        ML1["snap-1000.avro  manifest-list\nmanifest_path: abfss://.../m-001.avro"]
        MF1["m-001.avro  manifest\nfile_path: abfss://.../data/00001.parquet"]
        D1["00001.parquet"]
        M1 --> ML1 --> MF1 --> D1
    end

    subgraph Target["Target — MinIO  after naive file copy"]
        M2["metadata.json\nlocation: abfss://... ❌ still Azure"]
        ML2["snap-1000.avro\nmanifest_path: abfss://... ❌"]
        MF2["m-001.avro\nfile_path: abfss://... ❌"]
        D2["00001.parquet ✅"]
        M2 --> ML2 --> MF2 -.->|"resolves back\nto Azure source"| D1
    end

    Source -- "naive file copy only" --> Target
```

The Parquet files land on MinIO but every internal pointer still says `abfss://`.
An on-prem Spark or Trino cluster cannot reach Azure storage — the table is broken.

---

## 2. How It Works

`iceberg-catalog-sync` performs an **incremental sync with full metadata chain rewrite**:

```mermaid
flowchart LR
    SRC["Source\nADLS / S3 / GCS"]

    subgraph Engine["iceberg-catalog-sync"]
        direction TB
        S1["① Discover\nfind_latest_metadata()"]
        S2["② Diff\nread_snapshot_data_files()\nmanifest-based — any data layout\nincludes v2 delete files"]
        S3["③ Copy\nParquet files in parallel\nAbort if any copy fails"]
        S4["④ Rewrite\nPathTranslator + fastavro\ntranslate every URI"]
        S5["⑤ Commit\nversion-hint.text\nHadoop catalog targets only"]
        S6["⑥ Register\nNessieCatalog.register_or_update()\nPOST /iceberg/v1/namespaces/{ns}/register"]
        S1 --> S2 --> S3 --> S4 --> S5 --> S6
    end

    TGT["Target Storage\nMinIO / ADLS / S3"]
    CAT["REST Catalog\nNessie / Polaris / Glue"]

    SRC --> S1
    S3 --> TGT
    S4 --> TGT
    S6 --> CAT
```

**What gets rewritten** — every absolute URI in every file in the metadata chain:

| File | Field rewritten |
|------|----------------|
| `metadata.json` (JSON) | `location`, `snapshot.manifest-list`, `metadata-log` |
| `snap-NNN.avro` manifest-list (Avro) | `manifest_path` per record |
| `m-NNN.avro` manifest (Avro) | `data_file.file_path` per record |

The result is a fully self-consistent Iceberg table on the target — readable by any
engine (Spark, Trino, DuckDB, PyIceberg) without any custom FileIO plugin.

---

## 3. Platform Support

```mermaid
flowchart LR
    subgraph sources["Source Platforms"]
        A["Azure Fabric Lakehouse\nADLS Gen2  abfss://\nFabric Iceberg REST endpoint"]
        B["AWS S3  s3://\nGlue Iceberg REST endpoint"]
        C["Google Cloud Storage\ngs://"]
        D["MinIO  s3a://"]
    end

    subgraph engine["iceberg-catalog-sync"]
        PT["PathTranslator\nany scheme → any scheme\nreversible for failback"]
        MR["MetadataRewriter\nfastavro byte-level Avro rewrite\nno Spark / JVM dependency"]
        NC["NessieCatalog\nIceberg REST API client\noptional — catalog layer only"]
    end

    subgraph targets["Target Storage"]
        T1["On-prem MinIO  s3a://"]
        T2["Azure ADLS Gen2  abfss://"]
        T3["AWS S3  s3://"]
        T4["Google Cloud  gs://"]
    end

    subgraph catalogs["Target Catalogs"]
        N["On-prem Nessie\nIceberg REST"]
        G["AWS Glue\nnative S3 sync"]
        P["Apache Polaris\nIceberg REST compatible"]
        U["Unity Catalog\nIceberg REST compatible"]
    end

    sources --> engine
    engine --> targets
    engine --> catalogs
```

| Source | Source catalog discovery | Target Storage | Target Catalog | Notes |
|--------|--------------------------|----------------|----------------|-------|
| Azure Fabric / ADLS Gen2 | Fabric Iceberg REST endpoint | MinIO `s3a://` | Nessie | **This POC** |
| Azure Fabric / ADLS Gen2 | Fabric Iceberg REST endpoint | AWS S3 `s3://` | Glue / any | Supported |
| AWS S3 | Glue Iceberg REST endpoint | MinIO `s3a://` | Nessie | Supported |
| AWS S3 | Glue Iceberg REST endpoint | Azure ADLS Gen2 | any | DR / failover |
| AWS S3 | filesystem scan | Google Cloud `gs://` | any | Supported |
| Any | any | Any | Any Iceberg REST | Via `PathTranslator` + `NessieCatalog` |

---

## 4. End-to-End Sync Flow

The full sequence from Airflow trigger to queryable on-prem table:

```mermaid
sequenceDiagram
    participant AF as Airflow DAG
    participant SRC as Source Storage
    participant CS as CatalogSync
    participant TGT as Target MinIO
    participant NS as Nessie Catalog
    participant CL as Query Client

    Note over AF,CL: iceberg_sync_adls_to_nessie — triggered every 15 min

    AF->>CS: IcebergTableSyncOperator.execute()

    rect rgb(20, 30, 55)
    Note over CS,TGT: Phase 1 — Discovery + Manifest-Based Diff
    CS->>SRC: find_latest_metadata()
    Note over CS: Default: filesystem scan — version-hint fast path<br/>then 00NNN-UUID sequence sort, then last-updated-ms fallback<br/>Optional: pass metadata_location from any Iceberg REST endpoint
    SRC-->>CS: 00003-abc.metadata.json
    CS->>SRC: read manifest-list Avro
    CS->>SRC: read manifest Avro files
    Note over CS: read_snapshot_data_files() — walks manifest chain<br/>Returns exact file set from current snapshot<br/>Handles custom write.data.path and v2 delete files
    CS->>TGT: list_objects table root
    Note over CS: Diff — 3 new files to copy, 177 already synced
    end

    rect rgb(20, 30, 55)
    Note over CS,TGT: Phase 2 — Incremental Copy
    loop 3 new Parquet files
        CS->>SRC: read_bytes data/0017N.parquet
        CS->>TGT: write_bytes data/0017N.parquet
    end
    Note over TGT: 180 Parquet files in place
    Note over CS: Abort if any copy failed — target retains previous valid state
    end

    rect rgb(20, 50, 20)
    Note over CS,TGT: Phase 3 — Metadata Rewrite
    CS->>SRC: read manifest Avro
    CS->>CS: translate file_path abfss to s3a
    CS->>TGT: write rewritten manifest

    CS->>SRC: read manifest-list Avro
    CS->>CS: translate manifest_path abfss to s3a
    CS->>TGT: write rewritten manifest-list

    CS->>SRC: read metadata.json
    CS->>CS: translate location and snapshot paths
    CS->>TGT: write rewritten 00003-abc.metadata.json
    end

    rect rgb(11, 61, 11)
    Note over CS,TGT: Phase 4 — Commit
    opt Hadoop catalog target only — skipped for REST catalog targets
        CS->>TGT: write version-hint.text = 3
    end
    Note over TGT: All data and metadata consistent on target
    end

    rect rgb(40, 20, 55)
    Note over AF,TGT: Phase 5 — Health Check
    AF->>TGT: read 00003-abc.metadata.json
    Note over AF: Scan for source scheme abfss — count 0 means clean
    end

    rect rgb(11, 61, 11)
    Note over AF,NS: Phase 6 — Catalog Registration
    AF->>NS: GET /iceberg/v1/namespaces/gold/tables/top_customers
    alt first sync
        NS-->>AF: 404 Not Found
        AF->>NS: POST /iceberg/v1/namespaces/gold/register
        NS-->>AF: 200 OK — registered
    else incremental update
        NS-->>AF: 200 OK — current pointer captured
        Note over AF: Pre-capture current metadata-location for restore safety
        AF->>NS: DELETE /iceberg/v1/namespaces/gold/tables/top_customers
        AF->>NS: POST /iceberg/v1/namespaces/gold/register new metadata URI
        NS-->>AF: 200 OK — updated
        Note over AF: On failure: restore previous pointer automatically
    end
    end

    rect rgb(40, 20, 55)
    Note over NS,CL: Phase 7 — Client Query
    CL->>NS: GET /iceberg/v1/namespaces/gold/tables/top_customers
    NS-->>CL: metadata-location s3a://warehouse/.../00003-abc.metadata.json
    CL->>TGT: read metadata chain and scan Parquet files
    Note over CL: SELECT * FROM nessie.gold.top_customers
    end
```

---

## 5. Metadata Discovery

### Default — Filesystem Scan

The sync engine locates the latest `metadata.json` by scanning the object storage
layer directly. No catalog connection required — works with any Iceberg table
regardless of which catalog wrote it.

```mermaid
flowchart TD
    START["find_latest_metadata()"]
    H["Read version-hint.text\nHadoop catalog fast path"]
    HF{"found and valid?"}
    HR["return v{N}.metadata.json"]
    S["list_objects metadata/\nscan .metadata.json files"]
    M{"modern naming?\n00NNN-UUID.metadata.json"}
    MR["sort by 5-digit prefix\nreturn highest sequence"]
    L{"legacy naming?\nv{N}.metadata.json"}
    LR["sort by version number\nreturn highest"]
    FB["read last-updated-ms\nfrom every file\nreturn most recent"]

    START --> H --> HF
    HF -- yes --> HR
    HF -- no --> S --> M
    M -- yes --> MR
    M -- no --> L
    L -- yes --> LR
    L -- no --> FB
```

This handles all Iceberg naming conventions out of the box:

| Convention | Example filename | Used by |
|------------|-----------------|---------|
| Modern sequence | `00003-550e8400-e29b-41d4-a716-446655440000.metadata.json` | Spark REST catalog, Nessie, Fabric, Glue, Polaris |
| Hadoop legacy | `v3.metadata.json` + `version-hint.text` | HadoopCatalog (older tables) |
| Unknown / custom | any `.metadata.json` | fallback reads `last-updated-ms` from each file |

### Optional — Iceberg REST Endpoint (Hybrid)

If the source catalog exposes a standard **Iceberg REST Catalog** endpoint, you can
ask it directly for the current `metadata_location`. This is more precise than a
filesystem scan and avoids any ambiguity when multiple metadata files exist at the
same sequence number.

Both Azure Fabric and AWS Glue implement the open
[Iceberg REST Catalog spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
The call is identical regardless of which platform you use:

```
GET /iceberg/v1/namespaces/{namespace}/tables/{table}

Response:
{
  "metadata-location": "abfss://iceberg@acct.../gold/top_customers/metadata/00003-abc.metadata.json",
  "metadata": { ... }
}
```

| Platform | Iceberg REST base URL |
|----------|-----------------------|
| **Azure Fabric** | `https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/liveSynapse/api/iceberg/v1` |
| **AWS Glue** | `https://glue.{region}.amazonaws.com/iceberg` |
| **On-prem Nessie** | `http://nessie:19120/iceberg/v1` |
| **Apache Polaris** | `https://polaris.example.com/api/catalog/v1` |

```python
import requests

# Works for Fabric, Glue, Nessie, Polaris — same REST spec
iceberg_rest_base = "https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/liveSynapse/api/iceberg/v1"

resp = requests.get(
    f"{iceberg_rest_base}/namespaces/gold/tables/top_customers",
    headers={"Authorization": f"Bearer {token}"},
)
metadata_location = resp.json()["metadata-location"]

# Pass to sync — skips filesystem scan entirely
result = sync.sync_table(
    table_root="abfss://iceberg@acct.dfs.core.windows.net/gold/top_customers/",
    metadata_location=metadata_location,
)
```

> **When to use which approach**
>
> - **Filesystem (default):** use this always. Zero extra credentials, works offline,
>   handles all naming conventions. Sufficient for 99% of cases.
> - **REST endpoint (hybrid):** use when the source catalog is a live REST service and
>   you want the authoritative pointer without a directory listing — useful at very
>   high table counts or when the metadata directory contains thousands of old files.

---

## 6. Quick Start

### On-prem stack (both examples need this)

```bash
# Start Nessie + MinIO
docker compose -f docker/docker-compose.yml up -d

# Verify
curl http://localhost:19120/q/health/ready   # {"status":"UP"}
curl http://localhost:9000/minio/health/live # OK
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Nessie native API | http://localhost:19120/api/v2 | — |
| Nessie health | http://localhost:19120/q/health/ready | — |
| MinIO S3 API | http://localhost:9000 | minioadmin / minioadmin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

> **Note:** `projectnessie/nessie:latest` does not expose the Iceberg REST
> compatibility layer (`/iceberg/v1`) without additional configuration.
> The sync tool uses the Nessie native v2 API (`/api/v2`) which works
> out of the box on all versions.

```bash
pip install -e ".[nessie]"
```

---

### Example A — Azure Fabric / ADLS → On-Prem Nessie

Source tables live in Azure Fabric Lakehouse (backed by ADLS Gen2). The sync copies
them to on-prem MinIO under the `azure/` prefix and registers each table in the local
Nessie catalog.

> **Why a prefix?** If you also run Example B (AWS → on-prem), both sources write
> the same table names (`gold/top_customers` etc.).  Using `s3a://warehouse/azure/`
> and `s3a://warehouse/aws/` keeps the two copies separate so you can query and
> compare them side-by-side.

**Single table**

```bash
iceberg-sync table \
  --source-root        "abfss://iceberg@mystorageacct.dfs.core.windows.net/iceberg" \
  --source-secret-key  "<storage-account-key>" \
  --target-root        "s3a://warehouse/azure/" \
  --table              "gold/top_customers" \
  --target-endpoint    http://localhost:9000 \
  --target-access-key  minioadmin \
  --target-secret-key  minioadmin \
  --nessie-uri         http://localhost:19120
```

> **ADLS path:** The container name (`iceberg`) and any sub-folder prefix inside
> that container must both be included in `--source-root`.  For an Azure Fabric
> Lakehouse where blobs live at `iceberg/gold/top_customers/...` inside the
> `iceberg` container, the root is
> `abfss://iceberg@mystorageacct.dfs.core.windows.net/iceberg`.
>
> **Authentication:** If cross-tenant access is blocked by your Azure admin
> (`AADSTS500212`), `DefaultAzureCredential` will fail.  Pass
> `--source-secret-key <storage-account-key>` to use account-key auth instead.
> Retrieve the key with:
> ```bash
> az storage account keys list --account-name mystorageacct --query "[0].value" -o tsv
> ```

**Expected output — first run (full load)**

```
Syncing table: abfss://iceberg@mystorageacct.../gold/top_customers/
Nessie catalog: http://localhost:19120  (ref: main)

  Status              ✓ SUCCESS
  Files copied        180
  Files skipped       0
  Bytes copied        412.7 MB
  Duration            34.2s
  Manifests rewritten 6
  Paths translated    180

  Nessie: gold.top_customers registered at http://localhost:19120
```

**Expected output — second run (incremental)**

```
  Files copied        3          ← only new partitions since last sync
  Files skipped       180        ← already on MinIO
  Bytes copied        6.1 MB
  Duration            4.8s

  Nessie: gold.top_customers updated
```

**Entire namespace**

```bash
iceberg-sync namespace \
  --source-root        "abfss://iceberg@mystorageacct.dfs.core.windows.net/iceberg" \
  --source-secret-key  "<storage-account-key>" \
  --target-root        "s3a://warehouse/azure/" \
  --namespace          "gold" \
  --target-endpoint    http://localhost:9000 \
  --target-access-key  minioadmin \
  --target-secret-key  minioadmin \
  --nessie-uri         http://localhost:19120
```

**Query the result (PySpark — Hadoop catalog)**

The Hadoop catalog reads directly from MinIO without requiring Nessie client
compatibility.  Launch pyspark with:

```bash
pyspark \
  --packages \
    org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
    org.apache.hadoop:hadoop-aws:3.3.4,\
    com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.azure=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.azure.type=hadoop" \
  --conf "spark.sql.catalog.azure.warehouse=s3a://warehouse/azure" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://localhost:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
```

```python
# Catalog name matches the prefix: azure.gold.<table>
spark.sql("SHOW TABLES IN azure.gold").show()
spark.sql("SELECT * FROM azure.gold.top_customers LIMIT 10").show()
```

> **version-hint.text warning:** You may see a `WARN HadoopTableOperations: Error
> reading version hint file` on first query.  This is harmless — Spark falls back
> to scanning the metadata directory and finds the correct metadata automatically.
> To silence it, write the hint manually after syncing:
> ```python
> import boto3
> s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
>                   aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
> s3.put_object(Bucket="warehouse",
>               Key="azure/gold/top_customers/metadata/version-hint.text", Body=b"3")
> ```

**Query the result (PyIceberg)**

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("local", **{
    "type": "sql",
    "uri": "sqlite:///pyiceberg_catalog.db",
    "warehouse": "s3a://warehouse/azure",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.path-style-access": "true",
})

df = catalog.load_table(("gold", "top_customers")).scan().to_arrow().to_pandas()
```

---

### Example B — AWS S3 → On-Prem Nessie + MinIO

**What this does:** The multicloud pipeline (`spark_job.py`) runs on AWS Glue and writes
`gold/top_customers` and `gold/revenue_by_order_date` to S3. This walkthrough syncs those
tables to on-prem MinIO and registers them in a local Nessie catalog so any on-prem engine
(Spark, Trino, DuckDB, PyIceberg) can query them without cloud access.

```
AWS Glue → S3 (s3://)
                └─── iceberg-catalog-sync ───► MinIO (s3a://) + Nessie
                     PathTranslator: s3:// → s3a://
```

---

#### Prerequisites checklist

| | Requirement |
|---|---|
| ✅ | AWS credentials configured (`aws configure` or env vars below) |
| ✅ | Glue pipeline has run at least once (`spark_job.py` on Glue) |
| ✅ | Docker + Docker Compose installed on the on-prem host |
| ✅ | Python 3.9+ |

---

#### Step 1 — Set environment variables

```bash
# AWS credentials — choose one method:

# Method A: env vars (CI/CD, EC2 instance role, or local dev)
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=eu-west-2

# Method B: named profile (local dev)
aws configure --profile iceberg-sync
export AWS_PROFILE=iceberg-sync

# Verify — should print your S3 buckets
aws s3 ls
```

---

#### Step 2 — Install iceberg-catalog-sync

```bash
git clone <this-repo> iceberg-catalog-sync
cd iceberg-catalog-sync

pip install -e ".[nessie]"

# Verify
iceberg-sync --help
```

---

#### Step 3 — Start the on-prem stack

```bash
# Starts: MinIO (:9000), MinIO Console (:9001), Nessie (:19120)
docker compose -f docker/docker-compose.yml up -d

# Wait ~10s then verify both services are healthy
curl -s http://localhost:19120/q/health/ready | python -m json.tool
# Expected: {"status":"UP", ...}

curl -s http://localhost:9000/minio/health/live
# Expected: (empty 200 OK)
```

---

#### Step 4 — Confirm source tables exist on S3

```bash
# These are written by spark_job.py on Glue (Bronze → Silver → Gold pipeline)
aws s3 ls s3://my-warehouse/iceberg/gold/ --recursive | grep "metadata.json"

# Expected — at least one metadata file per table:
# 2026-03-10 12:34:56   4821  gold/top_customers/metadata/00001-abc123.metadata.json
# 2026-03-10 12:34:57   5102  gold/revenue_by_order_date/metadata/00001-def456.metadata.json
```

---

#### Step 5 — Dry run (no writes — always do this first)

```bash
iceberg-sync table --dry-run \
  --source-root   "s3://my-warehouse/iceberg/" \
  --target-root   "s3a://warehouse/aws/" \
  --table         "gold/top_customers" \
  --source-region eu-west-2 \
  --target-endpoint    http://localhost:9000 \
  --target-access-key  minioadmin \
  --target-secret-key  minioadmin
```

Expected output:
```
Syncing table: s3://my-warehouse/iceberg/gold/top_customers/
DRY RUN — no changes will be made

  Status        ✓ SUCCESS
  Files copied  180  (would copy)
  Files skipped 0
  Bytes copied  412.7 MB  (would copy)
  Manifests     6  (would rewrite)
```

---

#### Step 6 — Live sync: single table

```bash
iceberg-sync table \
  --source-root   "s3://my-warehouse/iceberg/" \
  --target-root   "s3a://warehouse/aws/" \
  --table         "gold/top_customers" \
  --source-region eu-west-2 \
  --target-endpoint    http://localhost:9000 \
  --target-access-key  minioadmin \
  --target-secret-key  minioadmin \
  --nessie-uri    http://localhost:19120
```

```bash
# Second table
iceberg-sync table \
  --source-root   "s3://my-warehouse/iceberg/" \
  --target-root   "s3a://warehouse/aws/" \
  --table         "gold/revenue_by_order_date" \
  --source-region eu-west-2 \
  --target-endpoint    http://localhost:9000 \
  --target-access-key  minioadmin \
  --target-secret-key  minioadmin \
  --nessie-uri    http://localhost:19120
```

Or sync the entire namespace in one command:

```bash
iceberg-sync namespace \
  --source-root   "s3://my-warehouse/iceberg/" \
  --target-root   "s3a://warehouse/aws/" \
  --namespace     "gold" \
  --source-region eu-west-2 \
  --target-endpoint    http://localhost:9000 \
  --target-access-key  minioadmin \
  --target-secret-key  minioadmin \
  --nessie-uri    http://localhost:19120
```

Expected output (first run):
```
  Status              ✓ SUCCESS
  Files copied        180
  Files skipped       0
  Bytes copied        412.7 MB
  Duration            34.2s
  Manifests rewritten 6
  Paths translated    180
  Nessie: gold.top_customers registered
```

---

#### Step 7 — Verify the sync result

```bash
# 1. List files on MinIO — should have metadata/ and data/ subdirs
aws s3 ls s3://warehouse/aws/gold/top_customers/ \
  --endpoint-url http://localhost:9000 \
  --no-sign-request

# 2. Confirm no s3://my-warehouse URIs leaked into MinIO metadata
# (all should be s3a:// after translation)
aws s3 cp s3://warehouse/aws/gold/top_customers/metadata/ . \
  --endpoint-url http://localhost:9000 \
  --no-sign-request --recursive --exclude "*" --include "*.metadata.json"

grep -l "s3://my-warehouse" *.metadata.json   # should print nothing

# 3. Confirm Nessie has the table registered (uses native v2 API)
curl -s "http://localhost:19120/api/v2/trees/main/contents/gold.top_customers" \
  | python -m json.tool | grep metadataLocation
# Expected: "metadataLocation": "s3a://warehouse/aws/gold/top_customers/metadata/..."
```

---

#### Step 8 — Query on-prem (PyIceberg)

Use the `HadoopCatalog` (reads MinIO directly, no Nessie client needed):

```python
# query_onprem.py
import pyarrow as pa
from pyiceberg.catalog.hadoop import HadoopCatalog

catalog = HadoopCatalog("local", {
    "warehouse": "s3a://warehouse/aws",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.path-style-access": "true",
})

top_customers = catalog.load_table(("gold", "top_customers")).scan().to_arrow().to_pandas()
revenue = catalog.load_table(("gold", "revenue_by_order_date")).scan().to_arrow().to_pandas()

print("=== top_customers ===")
print(top_customers.head(5))
print(f"\nRow count: {len(top_customers)}")

print("\n=== revenue_by_order_date ===")
print(revenue.head(5))
```

```bash
pip install pyiceberg[s3fs]
python query_onprem.py
```

---

#### Step 9 — Incremental sync (re-run after Glue writes new data)

Run the same commands again. The engine reads the manifest chain to determine exactly
which files are new — only those are copied:

```
  Files copied  3          ← new partitions appended by Glue since last sync
  Files skipped 180        ← already on MinIO, not re-copied
  Bytes copied  6.1 MB
  Duration      4.8s
  Nessie: gold.top_customers updated
```

---

### Cross-Cloud Comparison — Azure vs AWS

Once both Example A and Example B have been run, both sources land on the same
MinIO instance under separate prefixes:

```
s3a://warehouse/azure/gold/top_customers/   ← from Azure Fabric
s3a://warehouse/aws/gold/top_customers/     ← from AWS Glue
```

Launch pyspark with two Hadoop catalog entries pointing at each prefix:

```bash
pyspark \
  --packages \
    org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
    org.apache.hadoop:hadoop-aws:3.3.4,\
    com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.azure=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.azure.type=hadoop" \
  --conf "spark.sql.catalog.azure.warehouse=s3a://warehouse/azure" \
  --conf "spark.sql.catalog.aws=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.aws.type=hadoop" \
  --conf "spark.sql.catalog.aws.warehouse=s3a://warehouse/aws" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://localhost:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
```

**Row counts**

```python
print("Azure rows:", spark.sql("SELECT COUNT(*) FROM azure.gold.top_customers").collect()[0][0])
print("AWS rows:  ", spark.sql("SELECT COUNT(*) FROM aws.gold.top_customers").collect()[0][0])
```

**Revenue diff — rows where the two sources disagree**

```python
spark.sql("""
  SELECT
    COALESCE(a.customer_key, b.customer_key) AS customer_key,
    a.total_revenue  AS azure_revenue,
    b.total_revenue  AS aws_revenue,
    a.total_revenue - b.total_revenue AS diff
  FROM azure.gold.top_customers a
  FULL OUTER JOIN aws.gold.top_customers b ON a.customer_key = b.customer_key
  WHERE a.total_revenue != b.total_revenue
     OR a.customer_key IS NULL
     OR b.customer_key IS NULL
  ORDER BY ABS(a.total_revenue - b.total_revenue) DESC
""").show(20)
```

**Write comparison result as a new on-prem Iceberg table**

```python
spark.sql("""
  CREATE TABLE IF NOT EXISTS azure.gold.top_customers_vs_aws
  USING iceberg AS
  SELECT
    COALESCE(a.customer_key, b.customer_key) AS customer_key,
    a.total_revenue  AS azure_revenue,
    b.total_revenue  AS aws_revenue,
    a.total_revenue - b.total_revenue AS diff,
    CASE
      WHEN b.customer_key IS NULL THEN 'azure_only'
      WHEN a.customer_key IS NULL THEN 'aws_only'
      WHEN a.total_revenue != b.total_revenue THEN 'mismatch'
      ELSE 'match'
    END AS status
  FROM azure.gold.top_customers a
  FULL OUTER JOIN aws.gold.top_customers b ON a.customer_key = b.customer_key
""")

spark.sql("SELECT status, COUNT(*) FROM azure.gold.top_customers_vs_aws GROUP BY status").show()
```

---

#### Full Python script (copy-paste runnable)

```python
# examples/sync_s3_to_onprem.py
"""
Syncs AWS S3 Iceberg gold tables → on-prem MinIO + Nessie.
Produced by: spark_job.py running on AWS Glue (multicloud pipeline).
Target: on-prem MinIO + Nessie for local query engines.
"""
from __future__ import annotations

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
S3_SOURCE_ROOT   = "s3://my-warehouse/iceberg/"
S3_REGION        = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")
MINIO_TARGET     = "s3a://warehouse/aws"
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",    "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
NESSIE_URI       = os.getenv("NESSIE_URI",         "http://localhost:19120")

# Gold tables produced by spark_job.py (multicloud pipeline)
TABLES = [
    "gold/top_customers",
    "gold/revenue_by_order_date",
]

# ── Main ───────────────────────────────────────────────────────────────────────
def main(dry_run: bool = False) -> int:
    from iceberg_sync.path_translator import PathTranslator
    from iceberg_sync.storage import create_storage
    from iceberg_sync.sync import CatalogSync
    from iceberg_sync.catalog.nessie import NessieCatalog

    translator = PathTranslator([(S3_SOURCE_ROOT, MINIO_TARGET)])

    source = create_storage(S3_SOURCE_ROOT, region_name=S3_REGION)
    target = create_storage(
        MINIO_TARGET,
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    sync = CatalogSync(
        translator=translator,
        source_storage=source,
        target_storage=target,
    )

    nessie = NessieCatalog(uri=NESSIE_URI)
    if not dry_run and not nessie.ping():
        log.error("Cannot reach Nessie at %s — is the stack running?", NESSIE_URI)
        log.error("  docker compose -f docker/docker-compose.yml up -d")
        return 1

    errors = 0
    for table_path in TABLES:
        table_root = f"{S3_SOURCE_ROOT}{table_path}/"
        namespace, table_name = table_path.split("/")

        log.info("─" * 60)
        log.info("Syncing: %s", table_root)

        result = sync.sync_table(table_root, dry_run=dry_run)

        log.info("  Files copied   : %d", result.files_copied)
        log.info("  Files skipped  : %d", result.files_skipped)
        log.info("  Bytes copied   : %.2f MB", result.bytes_copied / 1024 / 1024)
        log.info("  Duration       : %.1fs", result.duration_seconds)
        if result.rewrite_stats:
            log.info("  Paths translated: %d", result.rewrite_stats.data_file_paths_translated)

        if not result.success:
            log.error("  FAILED: %s", result.errors)
            errors += 1
            continue

        if dry_run:
            log.info("  DRY RUN — no changes made")
            continue

        # Register / update in Nessie
        nessie.register_or_update(
            namespace=namespace,
            table=table_name,
            metadata_location=result.target_metadata_uri,
        )
        log.info("  Nessie: %s.%s registered/updated", namespace, table_name)

    log.info("─" * 60)
    if errors:
        log.error("Finished with %d error(s)", errors)
        return 1

    if dry_run:
        log.info("DRY RUN complete — no changes made")
    else:
        log.info("SUCCESS — tables queryable via Nessie at %s", NESSIE_URI)
    return 0


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    sys.exit(main(dry_run=dry_run))
```

```bash
# Dry run
python examples/sync_s3_to_onprem.py --dry-run

# Live sync
python examples/sync_s3_to_onprem.py
```

---

### Example C — AWS S3 → Azure ADLS Gen2

**What this does:** Syncs the same AWS Glue gold tables to Azure ADLS Gen2. The tables
become available to Azure Fabric Lakehouse, Synapse Spark, or a Nessie deployed in Azure —
without re-running the pipeline. No on-prem infrastructure needed.

```
AWS Glue → S3 (s3://)
                └─── iceberg-catalog-sync ───► ADLS Gen2 (abfss://)
                     PathTranslator: s3:// → abfss://
```

> **Best placement for the sync engine:** Run it in AWS (Glue Python Shell job or ECS task)
> so S3 reads are free within the same region. The engine writes out to Azure over the
> public ADLS endpoint.

---

#### Prerequisites checklist

| | Requirement |
|---|---|
| ✅ | AWS credentials configured (same as Example B) |
| ✅ | Azure Storage account with ADLS Gen2 (hierarchical namespace enabled) |
| ✅ | `iceberg` container created in the storage account |
| ✅ | `AZURE_STORAGE_KEY` or `AZURE_CLIENT_ID/SECRET/TENANT_ID` set |
| ✅ | Glue pipeline has run at least once |

---

#### Step 1 — Set environment variables

```bash
# AWS (same as Example B)
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=eu-west-2

# Azure — choose one:

# Method A: storage account key (simplest)
export AZURE_STORAGE_ACCOUNT=mystorageacct
export AZURE_STORAGE_KEY=<your-storage-key>

# Method B: service principal (CI/CD, production)
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<client-secret>
export AZURE_TENANT_ID=<tenant-id>
```

---

#### Step 2 — Install

```bash
pip install -e ".[nessie]"
pip install azure-storage-file-datalake azure-identity
```

---

#### Step 3 — Confirm source tables exist on S3

```bash
aws s3 ls s3://my-warehouse/iceberg/gold/ --recursive | grep "metadata.json"
```

---

#### Step 4 — Dry run

```bash
iceberg-sync table --dry-run \
  --source-root        "s3://my-warehouse/iceberg/" \
  --target-root        "abfss://iceberg@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/" \
  --table              "gold/top_customers" \
  --source-region      eu-west-2 \
  --target-account-name  "${AZURE_STORAGE_ACCOUNT}" \
  --target-account-key   "${AZURE_STORAGE_KEY}"
```

---

#### Step 5 — Live sync: both tables

```bash
# top_customers
iceberg-sync table \
  --source-root        "s3://my-warehouse/iceberg/" \
  --target-root        "abfss://iceberg@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/" \
  --table              "gold/top_customers" \
  --source-region      eu-west-2 \
  --target-account-name  "${AZURE_STORAGE_ACCOUNT}" \
  --target-account-key   "${AZURE_STORAGE_KEY}"

# revenue_by_order_date
iceberg-sync table \
  --source-root        "s3://my-warehouse/iceberg/" \
  --target-root        "abfss://iceberg@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/" \
  --table              "gold/revenue_by_order_date" \
  --source-region      eu-west-2 \
  --target-account-name  "${AZURE_STORAGE_ACCOUNT}" \
  --target-account-key   "${AZURE_STORAGE_KEY}"
```

Or entire namespace:

```bash
iceberg-sync namespace \
  --source-root        "s3://my-warehouse/iceberg/" \
  --target-root        "abfss://iceberg@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/" \
  --namespace          "gold" \
  --source-region      eu-west-2 \
  --target-account-name  "${AZURE_STORAGE_ACCOUNT}" \
  --target-account-key   "${AZURE_STORAGE_KEY}"
```

> No `--nessie-uri` needed. The table lands on ADLS as a fully self-consistent
> Iceberg table — all internal URIs point to `abfss://`. Azure Fabric Lakehouse
> reads it directly via its own Iceberg REST endpoint.

---

#### Step 6 — Verify the sync result

```bash
# List metadata files on ADLS
az storage blob list \
  --account-name "${AZURE_STORAGE_ACCOUNT}" \
  --account-key  "${AZURE_STORAGE_KEY}" \
  --container-name iceberg \
  --prefix "gold/top_customers/metadata/" \
  --query "[].name" \
  --output tsv

# Download one metadata file and confirm all URIs are abfss://
az storage blob download \
  --account-name "${AZURE_STORAGE_ACCOUNT}" \
  --account-key  "${AZURE_STORAGE_KEY}" \
  --container-name iceberg \
  --name "gold/top_customers/metadata/00001-$(ls *.metadata.json | head -1)" \
  --file top_customers.metadata.json

grep -c "abfss://" top_customers.metadata.json   # should be > 0
grep -c "s3://"    top_customers.metadata.json   # should be 0
```

---

#### Step 7 — Query from Azure Fabric notebook

```python
# Paste into a Fabric Spark notebook (Lakehouse must have the ADLS container as shortcut)

# Option A: via Fabric Lakehouse shortcut — Fabric auto-discovers the metadata
df = spark.read.format("iceberg") \
    .load("abfss://iceberg@mystorageacct.dfs.core.windows.net/gold/top_customers")
df.show(5)

# Option B: direct SparkSession pointing at the table root
spark.conf.set(
    "spark.sql.catalog.raw_iceberg",
    "org.apache.iceberg.spark.SparkCatalog"
)
spark.conf.set(
    "spark.sql.catalog.raw_iceberg.type", "hadoop"
)
spark.conf.set(
    "spark.sql.catalog.raw_iceberg.warehouse",
    "abfss://iceberg@mystorageacct.dfs.core.windows.net/"
)
spark.sql("SELECT * FROM raw_iceberg.gold.top_customers LIMIT 10").show()
```

---

#### Full Python script (copy-paste runnable)

```python
# examples/sync_s3_to_adls.py
"""
Syncs AWS S3 Iceberg gold tables → Azure ADLS Gen2.
Source: spark_job.py on AWS Glue (multicloud pipeline).
Target: Azure ADLS Gen2 for Fabric Lakehouse / Synapse consumers.
"""
from __future__ import annotations

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
S3_SOURCE_ROOT    = "s3://my-warehouse/iceberg/"
S3_REGION         = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")

AZURE_ACCOUNT     = os.environ["AZURE_STORAGE_ACCOUNT"]   # required
AZURE_KEY         = os.environ["AZURE_STORAGE_KEY"]        # required
ADLS_TARGET       = f"abfss://iceberg@{AZURE_ACCOUNT}.dfs.core.windows.net/"

# Gold tables produced by spark_job.py (multicloud pipeline)
TABLES = [
    "gold/top_customers",
    "gold/revenue_by_order_date",
]

# ── Main ───────────────────────────────────────────────────────────────────────
def main(dry_run: bool = False) -> int:
    from iceberg_sync.path_translator import PathTranslator
    from iceberg_sync.storage import create_storage
    from iceberg_sync.sync import CatalogSync

    translator = PathTranslator([(S3_SOURCE_ROOT, ADLS_TARGET)])

    source = create_storage(S3_SOURCE_ROOT, region_name=S3_REGION)
    target = create_storage(
        ADLS_TARGET,
        storage_account_name=AZURE_ACCOUNT,
        storage_account_key=AZURE_KEY,
    )

    sync = CatalogSync(
        translator=translator,
        source_storage=source,
        target_storage=target,
    )

    errors = 0
    for table_path in TABLES:
        table_root = f"{S3_SOURCE_ROOT}{table_path}/"

        log.info("─" * 60)
        log.info("Syncing: %s  →  %s%s/", table_root, ADLS_TARGET, table_path)

        result = sync.sync_table(table_root, dry_run=dry_run)

        log.info("  Files copied    : %d", result.files_copied)
        log.info("  Files skipped   : %d", result.files_skipped)
        log.info("  Bytes copied    : %.2f MB", result.bytes_copied / 1024 / 1024)
        log.info("  Duration        : %.1fs", result.duration_seconds)
        if result.rewrite_stats:
            log.info("  Paths translated: %d",
                     result.rewrite_stats.data_file_paths_translated)

        if not result.success:
            log.error("  FAILED: %s", result.errors)
            errors += 1
            continue

        if dry_run:
            log.info("  DRY RUN — no changes made")
        else:
            log.info("  Target: %s%s/", ADLS_TARGET, table_path)

    log.info("─" * 60)
    if errors:
        log.error("Finished with %d error(s)", errors)
        return 1

    if dry_run:
        log.info("DRY RUN complete — no changes made")
    else:
        log.info("SUCCESS — tables available at %s", ADLS_TARGET)
        log.info("Fabric notebook: spark.read.format('iceberg')"
                 ".load('%sgold/top_customers')", ADLS_TARGET)
    return 0


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    sys.exit(main(dry_run=dry_run))
```

```bash
# Dry run
AZURE_STORAGE_ACCOUNT=mystorageacct \
AZURE_STORAGE_KEY=<key> \
python examples/sync_s3_to_adls.py --dry-run

# Live sync
AZURE_STORAGE_ACCOUNT=mystorageacct \
AZURE_STORAGE_KEY=<key> \
python examples/sync_s3_to_adls.py
```

**What changes per example — summary**

| | Example A | Example B | Example C |
|---|---|---|---|
| Data producer | Azure Fabric / Synapse | AWS Glue `spark_job.py` | AWS Glue `spark_job.py` |
| Tables synced | `gold/top_customers` | `gold/top_customers`<br/>`gold/revenue_by_order_date` | `gold/top_customers`<br/>`gold/revenue_by_order_date` |
| `--source-root` | `abfss://` | `s3://` | `s3://` |
| `--target-root` | `s3a://` | `s3a://` | `abfss://` |
| `--nessie-uri` | yes | yes | optional |
| On-prem stack needed | MinIO + Nessie | MinIO + Nessie | no |
| Best sync engine placement | on-prem Airflow | on-prem Airflow | AWS Glue Job / MWAA |

---

### Python API

```python
from iceberg_sync.path_translator import PathTranslator
from iceberg_sync.storage import create_storage
from iceberg_sync.sync import CatalogSync
from iceberg_sync.catalog.nessie import NessieCatalog

# Works identically for ADLS or S3 — only the URIs differ
translator = PathTranslator([
    # Example A: ADLS → MinIO
    ("abfss://iceberg@acct.dfs.core.windows.net/", "s3a://warehouse/"),
    # Example B: S3 → MinIO  (swap line above for this one)
    # ("s3://my-warehouse/iceberg/", "s3a://warehouse/"),
])

sync = CatalogSync(
    translator=translator,
    source_storage=create_storage(
        "abfss",
        storage_account_name="acct",
        # For S3: create_storage("s3", region_name="eu-west-2")
    ),
    target_storage=create_storage(
        "s3a",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    ),
    write_version_hint=False,   # target is Nessie REST catalog — no version-hint.text needed
)

result = sync.sync_table(
    "abfss://iceberg@acct.dfs.core.windows.net/gold/top_customers/",
)
print(f"Copied {result.files_copied} files, translated {result.rewrite_stats.data_file_paths_translated} paths")

# Register in Nessie
nessie = NessieCatalog("http://localhost:19120")
nessie.register_or_update(
    namespace="gold",
    table="top_customers",
    metadata_location=result.target_metadata_uri,
)
```

---

## 7. Airflow Setup

### Docker (recommended)

```bash
docker compose \
  -f docker/docker-compose.yml \
  -f docker/docker-compose.airflow.yml \
  up -d

# First-time only
docker compose \
  -f docker/docker-compose.yml \
  -f docker/docker-compose.airflow.yml \
  run --rm airflow-init
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Nessie | http://localhost:19120 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

**Set Airflow Variables** (Admin → Variables or CLI):

```bash
docker exec airflow-webserver airflow variables set \
  ICEBERG_ADLS_SOURCE_ROOT "abfss://iceberg@mystorageacct.dfs.core.windows.net/"
docker exec airflow-webserver airflow variables set \
  ICEBERG_NESSIE_URI "http://nessie:19120"
docker exec airflow-webserver airflow variables set \
  ICEBERG_MINIO_ENDPOINT "http://minio:9000"
```

| Variable | Example | Used by |
|----------|---------|---------|
| `ICEBERG_ADLS_SOURCE_ROOT` | `abfss://iceberg@acct.dfs.core.windows.net/` | ADLS→Nessie DAG |
| `ICEBERG_S3_SOURCE_ROOT` | `s3://warehouse/iceberg/` | S3→Nessie DAG |
| `ICEBERG_MINIO_ENDPOINT` | `http://minio:9000` | all MinIO DAGs |
| `ICEBERG_MINIO_ACCESS_KEY` | `minioadmin` | all MinIO DAGs |
| `ICEBERG_MINIO_SECRET_KEY` | `minioadmin` | all MinIO DAGs |
| `ICEBERG_NESSIE_URI` | `http://nessie:19120` | Nessie DAGs |
| `ICEBERG_ADLS_TARGET_ROOT` | `abfss://iceberg@acct.../` | S3→ADLS DR DAG |
| `AZURE_STORAGE_ACCOUNT` | `mystorageacct` | S3→ADLS DR DAG |
| `AWS_DEFAULT_REGION` | `eu-west-2` | S3 source DAGs |

### DAG Factory Pattern

One factory function generates a fully-wired DAG for any source → target pair.
Adding a platform is a single config block in [`airflow_dags/iceberg_sync_dag.py`](airflow_dags/iceberg_sync_dag.py):

```python
SyncPipelineConfig(
    dag_id       = "iceberg_sync_gcs_to_nessie",
    description  = "GCS → on-prem Nessie",
    schedule     = "*/30 * * * *",
    source_root  = "gs://my-warehouse/iceberg/",
    target_root  = "s3a://warehouse/",
    tables       = ["gold/orders", "gold/customers"],
    source_kwargs = {"project": "my-gcp-project"},
    target_kwargs = _minio_kwargs(),
    source_scheme = "gs",
    nessie_uri   = "http://nessie:19120",
)
```

```mermaid
flowchart TB
    subgraph configs["SYNC_PIPELINES — one entry per platform"]
        C1["adls_to_nessie\nsource: abfss://\ntarget: s3a://\nnessie_uri: http://nessie:19120\nschedule: every 15 min"]
        C2["s3_to_nessie\nsource: s3://\ntarget: s3a://\nnessie_uri: http://nessie:19120\nschedule: every 15 min"]
        C3["s3_to_adls\nsource: s3://\ntarget: abfss://\nnessie_uri: none — DR pipeline\nschedule: trigger only"]
    end

    FAC["make_iceberg_sync_dag(config)\nglobals()[config.dag_id] = dag\nAirflow auto-discovers all DAGs"]

    configs --> FAC

    subgraph dag1["iceberg_sync_adls_to_nessie  generated"]
        direction LR
        ST1["start"] --> SY1A["sync_gold_top_customers"] --> HE1A["health_gold_top_customers"] --> NE1A["nessie_gold_top_customers"] --> DO1["all_done"]
        ST1 --> SY1B["sync_gold_revenue"] --> HE1B["health_gold_revenue"] --> NE1B["nessie_gold_revenue"] --> DO1
    end

    subgraph dag3["iceberg_sync_s3_to_adls  no Nessie step"]
        direction LR
        ST3["start"] --> SY3["sync_gold_revenue"] --> HE3["health_gold_revenue"] --> DO3["all_done"]
    end

    FAC --> dag1
    FAC --> dag3
```

Generated DAGs:

| DAG ID | Schedule | Pipeline |
|--------|----------|----------|
| `iceberg_sync_adls_to_nessie` | every 15 min | Azure Fabric ADLS → MinIO + Nessie |
| `iceberg_sync_s3_to_nessie` | every 15 min | AWS S3 → MinIO + Nessie |
| `iceberg_sync_s3_to_adls` | trigger only | AWS S3 → Azure ADLS (DR) |
| `iceberg_failover_monitor` | every 5 min | AWS health check — sets `FAILOVER_ACTIVE` |

---

## 8. Architecture

### Module Structure

```mermaid
flowchart TB
    subgraph entry["Entry Points"]
        CLI["cli.py\niceberg-sync table / namespace\n--nessie-uri --nessie-ref"]
        OPS["airflow/operators.py\nIcebergTableSyncOperator\nIcebergHealthCheckOperator\nNessieCatalogRegisterOperator"]
        API["Python API\nCatalogSync  NessieCatalog"]
    end

    CS["sync/catalog_sync.py\nCatalogSync\ndiff → copy → rewrite → commit"]

    subgraph core["Core Libraries"]
        PT["path_translator.py\nPathTranslator\npure string prefix mapping\nreversible for failback"]
        MR["metadata/rewriter.py\nMetadataRewriter + find_latest_metadata\nJSON rewrite + fastavro Avro rewrite\nno Spark / JVM dependency"]
        RD["metadata/reader.py\nread_snapshot_data_files()\nmanifest-chain walk\nall content types — DATA / POSITION / EQUALITY deletes"]
    end

    subgraph storage["storage/ — pluggable backends"]
        SB["base.py\nStorageBackend abstract\nlist / read / write / exists"]
        S3["s3.py\nAWS S3 + MinIO\ns3:// s3a://"]
        ADLS["adls.py\nAzure ADLS Gen2\nabfss://"]
        GCS["gcs.py\nGoogle Cloud\ngs://"]
        MEM["memory.py\ntests"]
        SB --- S3 & ADLS & GCS & MEM
    end

    subgraph catalog["catalog/ — REST catalog layer"]
        NC["nessie.py\nNessieCatalog\nregister_or_update()\nPOST /iceberg/v1/namespaces/{ns}/register\nDELETE + re-register for updates"]
    end

    CLI & OPS & API --> CS
    CLI & OPS & API --> NC
    CS --> PT & MR & RD & storage
    MR --> PT & storage
    RD --> storage
    NC -->|"Iceberg REST API"| NSV["Nessie / Polaris / Glue REST endpoint"]
```

### Docker Stack

```mermaid
flowchart LR
    subgraph host["On-Premises Host"]
        subgraph base["docker-compose.yml"]
            N["Nessie :19120\nIceberg REST Catalog\n/iceberg/v1"]
            MI["MinIO :9000\nS3-compatible storage\nbucket: warehouse"]
            MC["mc-init\ncreates bucket on first start"]
            MI --> MC
        end
        subgraph airflow["docker-compose.airflow.yml"]
            AW["Airflow Webserver :8080"]
            AS["Airflow Scheduler"]
            PG["PostgreSQL\nAirflow metadata DB"]
            PG --> AW & AS
        end
    end

    SRC["Azure ADLS\nor AWS S3\nsource only — read"]
    QC["Spark / Trino\nPyIceberg / DuckDB"]

    SRC -->|"source reads"| AS
    AS -->|"Parquet + rewritten metadata"| MI
    AS -->|"register_or_update"| N
    N -->|"load_table"| QC
    MI -->|"s3a:// file reads"| QC
```

---

## 9. Consistency Guarantee

```mermaid
sequenceDiagram
    participant S as Source
    participant CS as CatalogSync
    participant T as Target MinIO
    participant N as Nessie Catalog

    Note over S,N: Target may have v2 or be empty — either is safe

    CS->>S: find_latest_metadata()
    Note over CS: Filesystem scan by default — works for all catalogs<br/>Optional: pass metadata_location from any Iceberg REST endpoint
    S-->>CS: 00003-abc.metadata.json

    CS->>S: read manifest-list + manifests
    Note over CS: read_snapshot_data_files() — walks manifest chain<br/>Returns exact file set including v2 delete files
    CS->>T: list_objects table root
    Note over CS: Only new files need copying — v2 still active on target

    CS->>S: read new Parquet files
    CS->>T: write new Parquet files (parallel, thread-safe counters)
    Note over T: Data files in place — target still reads v2
    Note over CS: Abort if any copy failed — target retains v2 state

    CS->>S: read and rewrite manifest
    CS->>T: write rewritten manifest
    CS->>S: read and rewrite manifest-list
    CS->>T: write rewritten manifest-list
    CS->>S: read and rewrite metadata.json
    CS->>T: write rewritten 00003-abc.metadata.json

    opt Hadoop catalog target only
        CS->>T: write version-hint.text = 3
    end

    Note over T: All data and metadata consistent — v3 now readable

    rect rgb(11, 61, 11)
    Note over T: MinIO table is self-consistent at s3a:// paths
    end

    CS->>N: register_or_update gold.top_customers
    Note over CS: Pre-capture current metadata-location before any change
    alt first sync
        CS->>N: POST /iceberg/v1/namespaces/gold/register
        N-->>CS: 200 OK — registered
    else incremental update
        CS->>N: DELETE catalog entry
        CS->>N: POST /iceberg/v1/namespaces/gold/register new pointer
        N-->>CS: 200 OK — updated
        Note over CS: On failure: restore previous pointer automatically<br/>Log CRITICAL + manual recovery command if restore also fails
    end

    rect rgb(11, 61, 11)
    Note over N: Table queryable via any Iceberg REST engine
    end
```

| Guarantee | Detail |
|-----------|--------|
| **Atomic visibility** | Hadoop catalog targets: readable only after `version-hint.text`. REST catalog targets: Nessie registration is the switch. Partial syncs are invisible. |
| **Abort on copy failure** | If any data file copy fails, metadata rewrite is skipped entirely. Target retains its previous valid state. |
| **Manifest-accurate diff** | File discovery reads the Iceberg manifest chain — not a directory listing. Handles custom `write.data.path` and Iceberg v2 delete files. |
| **Nessie safety net** | Current metadata pointer is captured before drop. If re-register fails, the previous pointer is restored automatically. |
| **No data loss** | Source never modified. Target gets a point-in-time snapshot. |
| **Idempotent** | Re-running copies only new files and overwrites metadata — same result. |
| **RPO** | Equal to sync interval. 15-min Airflow schedule = up to 15 min lag during failover. |
| **Failback** | `PathTranslator.reverse()` produces the inverse mapping for syncing data back after recovery. |

---

## 10. Enterprise Grade

### 1. Persistent Nessie Storage

```yaml
# docker/docker-compose.yml
environment:
  NESSIE_VERSION_STORE_TYPE: ROCKSDB
  NESSIE_VERSION_STORE_PERSIST_ROCKS_DB_DB_PATH: /nessie/rocks
volumes:
  - nessie-data:/nessie/rocks
```

For full HA use the JDBC (PostgreSQL) backend with a managed database.

### 2. Zero-Downtime Catalog Updates

Replace drop + re-register with the Iceberg REST **CommitTableRequest** protocol —
no window where the table is absent from the catalog:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog("nessie", uri="http://nessie:19120/iceberg")
table = catalog.load_table(("gold", "top_customers"))
table.refresh()   # picks up new snapshot committed by sync
```

### 3. Secure Nessie with OIDC

```yaml
environment:
  QUARKUS_OIDC_ENABLED: "true"
  QUARKUS_OIDC_AUTH_SERVER_URL: https://your-idp/realms/data
  NESSIE_SERVER_AUTHENTICATION_ENABLED: "true"
```

Pass `--nessie-token $TOKEN` or set `NESSIE_TOKEN` env var to the sync CLI.

### 4. Nessie Branching — Write-Audit-Publish

Rather than syncing directly to `main`, sync to a staging branch, validate, then merge.
This is the WAP (Write-Audit-Publish) pattern and it is a first-class feature of Nessie:

```python
# Sync to staging branch
nessie = NessieCatalog("http://nessie:19120", ref="staging")
nessie.register_or_update("gold", "top_customers", new_metadata_uri)

# Validate on staging before promoting
spark.sql("SELECT count(*) FROM nessie_staging.gold.top_customers").show()

# Merge staging → main via Nessie API
import requests
requests.post("http://localhost:19120/api/v2/trees/main/history/transplant",
    json={"fromRefName": "staging", "keepIndividualCommits": False})
```

### 5. Multi-Source → Single On-Prem Catalog

```
abfss://gold@azure/    → MinIO → nessie.azure_gold.*
s3://gold/             → MinIO → nessie.aws_gold.*
gs://gold/             → MinIO → nessie.gcp_gold.*
```

```python
spark.sql(
    "SELECT * FROM nessie.azure_gold.revenue "
    "UNION ALL "
    "SELECT * FROM nessie.aws_gold.revenue"
).show()
```

### 6. Replication Tiers

| Tier | Tables | Sync frequency | RPO |
|------|--------|---------------|-----|
| Hot (Gold) | `gold.*` — reporting, revenue | after every pipeline run | ~15 min |
| Warm (Silver) | `silver.*` — cleansed orders | nightly 2 AM | ~24 h |
| Cold (Bronze) | `bronze.*` — raw ingest | weekly / on-demand | ~7 days |

### 7. Platform Expansion

Adding a new source or target is a single file in `storage/` implementing the
`StorageBackend` abstract interface — the sync engine and metadata rewriter are unchanged.

| New source/target | Add | Notes |
|-------------------|-----|-------|
| HDFS | `storage/hdfs.py` using `pyarrow.fs.HadoopFileSystem` | — |
| Delta Lake | `metadata/delta_rewriter.py` | Different metadata format |
| Apache Polaris | point `nessie_uri` at Polaris endpoint | Same Iceberg REST spec |
| Databricks Unity Catalog | point `nessie_uri` at UC endpoint | Same Iceberg REST spec |

---

## 11. Design Decisions

**Why not PyIceberg?**
PyIceberg's catalog API is designed for reading and writing tables, not for
byte-level metadata rewriting. We need direct Avro record manipulation to rewrite
embedded paths. `fastavro` gives that without pulling in the full Iceberg runtime.

**Why rewrite, not a custom FileIO?**
A custom Iceberg `FileIO` that translates URIs at read time would avoid rewriting,
but every consumer (Spark, Trino, Flink) would need to load a custom plugin.
Managed services don't support custom FileIO. Rewriting produces standard Iceberg
metadata that any reader can consume with zero configuration.

**Why is `version-hint.text` optional?**
`version-hint.text` is a Hadoop catalog artifact — a single integer file pointing
to `v{N}.metadata.json`. Modern REST catalog tables (Fabric, Glue, Nessie, Polaris)
do not use it; the catalog server is the authoritative pointer. Writing it to a
REST-catalog target would be misleading and wasteful. The engine sets
`write_version_hint=False` automatically when `--nessie-uri` is supplied.

**Why Nessie for the on-prem catalog?**
Nessie implements the standard Iceberg REST Catalog spec — Spark, Trino, Flink,
DuckDB, and PyIceberg all connect to it without custom drivers. It also provides
Git-like branching for the WAP pattern (sync to staging branch, validate, merge to main).

**Why drop + re-register for Nessie updates?**
For the POC it is the simplest correct approach: the new `metadata.json` already
contains full snapshot history, so re-registering gives Nessie everything in one step.
See the zero-downtime CommitTableRequest alternative in Enterprise Grade above.

---

## 12. Limitations

- **Active-passive only.** Replicates source → target. Simultaneous writes to both
  sides produce divergent snapshot sequences that cannot be merged without custom
  conflict resolution. There is no native cross-catalog conflict resolution in the
  Iceberg ecosystem.
- **Filesystem metadata discovery.** The default discovery scans the `metadata/`
  directory on object storage. For REST catalog sources (Fabric, Glue) that do not
  guarantee metadata files are always browseable, pass `metadata_location` directly
  from the catalog API.
- **No independent DDL on the target.** Running `ALTER TABLE` on the Nessie-side
  table independently from the source risks Field ID divergence. DDL changes should
  be made on the source and synced.
- **Brief drop window in Nessie updates.** The drop + re-register approach has a short
  window where the catalog entry is absent. If re-register fails, the previous pointer is
  automatically restored. For true zero-downtime commits, use CommitTableRequest (see
  Enterprise Grade above).

---

## 13. Project Structure

```
iceberg-catalog-sync/
├── src/iceberg_sync/
│   ├── path_translator.py          URI translation — any scheme → any scheme
│   ├── cli.py                      iceberg-sync CLI (--nessie-uri, --dry-run, ...)
│   ├── storage/
│   │   ├── base.py                 StorageBackend abstract interface
│   │   ├── factory.py              create_storage() — scheme → backend
│   │   ├── s3.py                   AWS S3 + MinIO  (s3://, s3a://)
│   │   ├── adls.py                 Azure ADLS Gen2  (abfss://)
│   │   └── gcs.py                  Google Cloud Storage  (gs://)
│   ├── metadata/
│   │   ├── rewriter.py             MetadataRewriter + find_latest_metadata()
│   │   └── reader.py               read_snapshot_data_files() — manifest-chain walk
│   ├── sync/
│   │   └── catalog_sync.py         CatalogSync orchestrator
│   ├── catalog/
│   │   └── nessie.py               NessieCatalog — Iceberg REST client
│   └── airflow/
│       └── operators.py            Airflow operators + health check
├── airflow_dags/
│   └── iceberg_sync_dag.py         DAG factory — all platforms, one codebase
├── docker/
│   ├── docker-compose.yml          On-prem stack: Nessie + MinIO
│   ├── docker-compose.airflow.yml  Airflow + PostgreSQL
│   └── .env.example                Environment template
├── examples/
│   └── adls_to_nessie_poc.py       End-to-end runnable POC script
└── tests/
    ├── test_path_translator.py
    └── test_metadata_rewrite.py
```
