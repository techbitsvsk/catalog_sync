# iceberg.rego — Catalog Gateway access control policies
#
# Queried by catalog-gateway at:
#   POST /v1/data/iceberg/policy
#   Input:  { "principal": str, "namespace": str, "table": str, "operation": str }
#   Output: { "allow": bool, "excluded_columns": [...], "row_filter": str|null, "column_masks": {...} }
#
# Operations
# ──────────
#   READ   — table metadata fetch (GET /tables/{t})
#   SCAN   — scan planning      (POST /tables/{t}/scan)
#   LIST   — table listing      (GET /tables)
#   WRITE  — create / commit    (POST /tables, PUT /tables/{t})
#   DROP   — delete table       (DELETE /tables/{t})
#
# Enforcement layers
# ──────────────────
#   allow=false         → gateway returns HTTP 403          (hard — Layer 1)
#   excluded_columns    → gateway strips from schema        (hard — Layer 4)
#   row_filter          → gateway injects into scan filter  (hard — Layer 2)
#   column_masks        → stored as table property          (advisory — Layer 3)
#
# To add a new principal or contract: add rules below and reload OPA
# (OPA hot-reloads from the /policies mount automatically).

package iceberg

import rego.v1

# ── Output document ───────────────────────────────────────────────────────────

policy := {
    "allow":            allow,
    "excluded_columns": excluded_columns,
    "row_filter":       row_filter,
    "column_masks":     column_masks,
}

# ── Layer 1: allow / deny ─────────────────────────────────────────────────────

default allow := false

# Trusted internal principals — unrestricted
allow if input.principal in {"admin-client", "sync-service", "catalog-gateway"}

# ops-client — platform ops engineers: full catalog access across all namespaces
# Attack surface: ops engineers can write any table metadata, including descriptions
# and column docs, which feed into downstream RAG pipelines.
allow if {
    input.principal == "ops-client"
    input.operation in {"READ", "LIST", "WRITE", "DROP"}
}

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

# ── Layer 4: column exclusions (hard CLS) ─────────────────────────────────────
# Gateway removes these columns from the table schema response.
# Clients never see these fields — they cannot request them in queries.

excluded_columns := ["ssn", "credit_card_number"] if {
    input.principal == "analytics-client"
    input.namespace == "gold"
} else := ["ssn", "credit_card_number", "date_of_birth"] if {
    input.principal == "data-scientist"
} else := []

# ── Layer 2: row filter (hard RLS) ────────────────────────────────────────────
# Gateway merges this into the Iceberg scan filter expression.
# For partitioned tables (PARTITIONED BY region): file-level pruning —
#   Nessie returns zero files from non-matching partitions.
# For non-partitioned tables: Nessie returns residual filter in scan tasks —
#   Spark applies it row-by-row after reading each file.
# Either way, enforcement does not depend on client cooperation.

row_filter := "region = 'EMEA'" if {
    input.principal == "analytics-client"
    input.namespace == "gold"
    input.table == "orders"
} else := null

# ── Layer 3: column masks (advisory) ──────────────────────────────────────────
# Stored as table property "gateway.column-masks" (JSON).
# The query engine (e.g. Trino with OPA SystemAccessControl) applies these.
# Spark applications should read the property and apply mask expressions.

column_masks := {
    "customer_email": "regexp_replace(customer_email, '@.*$', '@***.com')",
    "ip_address":     "regexp_replace(ip_address, '[0-9]+$', 'xxx')",
} if {
    input.principal == "analytics-client"
    input.namespace == "gold"
} else := {}
