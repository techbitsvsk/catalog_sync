# OPA Policy Guide — Iceberg Catalog Access Control

Open Policy Agent (OPA) evaluates every table access request. The catalog-gateway
queries OPA before forwarding to Nessie. This document covers the Rego policy structure,
how enforcement works, and how to add new principals or rules.

---

## Architecture

```
Client JWT  →  catalog-gateway:8083  →  POST /v1/data/iceberg/policy  →  opa:8181
                      │                        ↑
                      │                   iceberg.rego (hot-reload)
                      ↓
              Enforce: 403 / schema rewrite / virtual manifest-list / table property
                      │                              │
                      │           (filtered manifests written to MinIO .gateway/)
                      ↓
                  nessie:19120  (admin token, never client token)
```

OPA runs as a standalone server. The gateway calls it synchronously on every table
request. Policy files are hot-reloaded from the mounted volume — no restart needed.

---

## Enforcement layers

| Layer | Type | Mechanism | Bypass-proof? |
|-------|------|-----------|---------------|
| **1** Table access | Hard | OPA `allow=false` → gateway returns HTTP 403 | Yes — on the network path |
| **2** Row filter | Hard | **Virtual manifest-list:** gateway writes a filtered copy of the Iceberg manifest-list to MinIO and replaces the snapshot URL in the GET table response. Spark downloads only allowed partition files. Also enforced via PyIceberg scan planning on `POST /scan`. | Yes — Spark reads from the manifest-list the gateway provides |
| **3** Column masking | Advisory | Stored as `gateway.column-masks` table property | No — query engine must apply it |
| **4** Column exclusion | Hard | Gateway strips columns from table schema response | Yes — Spark never sees excluded fields |

---

## OPA query interface

The gateway queries OPA once per table request:

```
POST http://opa:8181/v1/data/iceberg/policy

Input:
{
  "input": {
    "principal": "analytics-client",   ← extracted from Bearer JWT (client_id or sub)
    "namespace": "gold",
    "table":     "orders",
    "operation": "READ"                ← READ | SCAN | LIST | WRITE | DROP
  }
}

Output:
{
  "result": {
    "allow":            true,
    "excluded_columns": ["ssn", "credit_card_number"],
    "row_filter":       "region = 'EMEA'",
    "column_masks":     {
      "customer_email": "regexp_replace(customer_email, '@.*$', '@***.com')",
      "ip_address":     "regexp_replace(ip_address, '[0-9]+$', 'xxx')"
    }
  }
}
```

All four values are returned in a single call — one round-trip per request.

---

## Rego policy structure

**File:** `opa/policies/iceberg.rego`

```rego
package iceberg
import rego.v1

# ── Output document ───────────────────────────────────────────────────────────
# Single document returned per query — all enforcement data in one response.

policy := {
    "allow":            allow,
    "excluded_columns": excluded_columns,
    "row_filter":       row_filter,
    "column_masks":     column_masks,
}

# ── Layer 1: allow / deny ────────────────────────────────────────────────────

default allow := false

# Trusted internal principals — unrestricted
allow if input.principal in {"admin-client", "sync-service", "catalog-gateway"}

# analytics-client — gold namespace, read-only
allow if {
    input.principal == "analytics-client"
    input.namespace == "gold"
    input.operation in {"READ", "LIST", "SCAN"}
}

# data-scientist — silver + bronze, read-only
allow if {
    input.principal == "data-scientist"
    input.namespace in {"silver", "bronze"}
    input.operation in {"READ", "LIST", "SCAN"}
}

# ── Layer 4: column exclusions ───────────────────────────────────────────────
# Use else := chaining to avoid Rego "complete rules conflict" errors.

excluded_columns := ["ssn", "credit_card_number"] if {
    input.principal == "analytics-client"
    input.namespace == "gold"
} else := ["ssn", "credit_card_number", "date_of_birth"] if {
    input.principal == "data-scientist"
} else := []

# ── Layer 2: row filter ──────────────────────────────────────────────────────

row_filter := "region = 'EMEA'" if {
    input.principal == "analytics-client"
    input.namespace == "gold"
    input.table == "orders"
} else := null

# ── Layer 3: column masks ────────────────────────────────────────────────────

column_masks := {
    "customer_email": "regexp_replace(customer_email, '@.*$', '@***.com')",
    "ip_address":     "regexp_replace(ip_address, '[0-9]+$', 'xxx')",
} if {
    input.principal == "analytics-client"
    input.namespace == "gold"
} else := {}
```

---

## Default access matrix

| Principal | Namespace | Operations | Row filter | Excluded columns |
|-----------|-----------|-----------|-----------|-----------------|
| `admin-client` | any | READ SCAN LIST WRITE DROP | none | none |
| `sync-service` | any | READ SCAN LIST WRITE DROP | none | none |
| `analytics-client` | `gold` | READ LIST SCAN | `region = 'EMEA'` (orders only) | `ssn`, `credit_card_number` |
| `data-scientist` | `silver`, `bronze` | READ LIST SCAN | none | `ssn`, `credit_card_number`, `date_of_birth` |
| any | other | any | — | HTTP 403 |

---

## Adding a new principal

Edit `opa/policies/iceberg.rego`. OPA detects the change and reloads within seconds.

**Example: add `reporting-client` with read access to `gold` and `silver`:**

```rego
# reporting-client — gold + silver, read-only, no row filter, no exclusions
allow if {
    input.principal == "reporting-client"
    input.namespace in {"gold", "silver"}
    input.operation in {"READ", "LIST", "SCAN"}
}
```

No `excluded_columns` or `row_filter` rule needed — the `else := []` and `else := null`
defaults in the existing rules will fire for `reporting-client`.

**Verify immediately (no restart):**

```bash
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{"input":{"principal":"reporting-client","namespace":"gold","table":"orders","operation":"READ"}}' \
  | jq .result.allow
# → true
```

---

## Adding a row filter for a new principal

```rego
# Override row_filter for reporting-client on gold.transactions
row_filter := "status = 'PUBLISHED'" if {
    input.principal == "reporting-client"
    input.namespace == "gold"
    input.table == "transactions"
} else := null   # must keep else := null for other cases
```

**Supported SQL filter syntax** (parsed by the gateway's `_sql_to_iceberg_expr`):

| Pattern | Iceberg expression |
|---------|-------------------|
| `col = 'value'` | `{"type": "eq", "term": "col", "value": "value"}` |
| `col IN ('a', 'b', 'c')` | `{"type": "in", "term": "col", "values": ["a","b","c"]}` |

Complex expressions are logged as a warning and skipped (fail-open for that filter).
Keep row filter expressions simple. Complex predicates require Trino OPA integration.

---

## How row-level security actually works (virtual manifest-list)

Spark's Iceberg RESTCatalog client (tested with `iceberg-spark-runtime 1.9.1`) performs
**local scan planning** — it downloads manifest files directly from MinIO via S3A and
never calls `POST /tables/{table}/scan`, even when `rest.scan-planning-enabled=true` is
advertised. Injecting a filter into the scan response body would have no effect.

To enforce RLS transparently, the gateway uses a **virtual manifest-list**:

```
GET /v1/namespaces/gold/tables/orders  (analytics-client)
          │
          ▼ OPA returns: row_filter = "region = 'EMEA'"
          │
          ▼ Gateway reads real manifest-list from MinIO (fastavro)
          │   manifest-list.avro
          │   └─ manifest-0.avro  → data_file entries for EMEA + APAC + AMER
          │
          ▼ For each manifest, keep only entries where partition['region'] == 'EMEA'
          │
          ▼ Write filtered files to MinIO:
          │   s3://warehouse/.gateway/{cache_key}/snap.avro      ← virtual manifest-list
          │   s3://warehouse/.gateway/{cache_key}/m-0.avro       ← EMEA-only manifest
          │
          ▼ Return table metadata with snapshot["manifest-list"] replaced:
              "manifest-list": "s3a://warehouse/.gateway/{cache_key}/snap.avro"
          │
          ▼ Spark downloads virtual manifest-list
              → only EMEA partition files listed
              → Spark reads 2 rows from EMEA Parquet files only
              → No app-level filter() call needed
```

**Cache:** Key = `MD5(manifest_list_path + ":" + filter)[:16]`. Automatically invalidates
when the table is updated (new Nessie commit → new manifest-list path → new cache key).

---

## Testing policies

**Query OPA directly:**

```bash
# Full policy output for a principal
curl -s -X POST http://localhost:8181/v1/data/iceberg/policy \
  -H "Content-Type: application/json" \
  -d '{
    "input": {
      "principal": "analytics-client",
      "namespace": "gold",
      "table": "orders",
      "operation": "SCAN"
    }
  }' | jq .result
```

**Test with OPA CLI (local, no server needed):**

```bash
# Install: https://www.openpolicyagent.org/docs/latest/#1-download-opa
opa eval \
  --data opa/policies/iceberg.rego \
  --input - \
  'data.iceberg.policy' <<EOF
{
  "principal": "analytics-client",
  "namespace": "gold",
  "table": "orders",
  "operation": "READ"
}
EOF
```

**Run the full PySpark end-to-end test:**

```bash
python scripts/test_pyspark_nessie.py
```

This seeds a real Iceberg v2 table in Nessie via the gateway and verifies that:
- `gw_analytics` receives exactly 2 EMEA rows without calling `df.filter()`
- `ssn` and `credit_card_number` are absent from `df.columns` without calling `df.drop()`
- `gw_ds` raises an exception (HTTP 403) when reading `gold.orders`

---

## Enforcement guarantees

| Threat | How mitigated |
|--------|--------------|
| Client skips policy call | Impossible — enforcement is in the gateway, not client code |
| Client accesses Nessie directly | Nessie port 19120 is not exposed to host — only `iceberg-net` internal |
| Client reads excluded columns | Gateway strips them from schema; Spark can't project columns it doesn't know exist |
| Client reads non-EMEA rows | Gateway rewrites snapshot manifest-list URL to a virtual filtered copy in MinIO; Spark only downloads EMEA partition files |
| Client calls `/scan` directly | Gateway's scan handler applies OPA `row_filter` via PyIceberg scan planning |
| OPA is down | Gateway returns HTTP 500 — never fails open |

> **Column masking (Layer 3) is advisory.** The gateway stores masks as a table property.
> True enforcement requires Trino with OPA SystemAccessControl or Spark plugin that reads
> `gateway.column-masks` and applies the expressions. Without this, masked columns are
> returned unmasked to clients.

---

## Hot reload

OPA watches the `/policies` volume mount (`--watch` flag in the container command).
Edit any `.rego` file → OPA reloads within ~2 seconds. No container restart needed.

```bash
# Confirm OPA picked up the change
curl -s http://localhost:8181/v1/policies | jq '.[].id'
# → ["opa/policies/iceberg.rego"]
```
