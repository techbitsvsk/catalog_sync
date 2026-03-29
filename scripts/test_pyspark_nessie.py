"""
PySpark end-to-end test: Iceberg REST Catalog via catalog-gateway + OPA enforcement.

Architecture
────────────
  Spark  →  catalog-gateway:8083  →  (OPA:8181 for policy)  →  nessie:19120
                                  →  (MinIO:9000 for data files)

The gateway is the sole enforcement point.  Nessie's port 19120 is NOT exposed
to the host.  Spark uses the standard Iceberg RESTCatalog — no Nessie-specific
extensions or JARs required.

Enforcement is fully transparent to Spark: no manual filter/drop/mask calls.
  Layer 1  Table access      OPA allow/deny → gateway returns HTTP 403
  Layer 4  Column exclusion  Gateway rewrites table schema — Spark never sees excluded cols
  Layer 2  Row filter        Gateway injects Iceberg scan filter — Nessie prunes at file level
  Layer 3  Column masking    Stored as table property; advisory (query engine applies)

Authentication
──────────────
  Each principal is a named Spark catalog with its own OAuth2 client credentials.
  The Iceberg RESTCatalog fetches tokens via client_credentials grant on first use
  and caches / refreshes them automatically.

  gw_admin      → admin-client     (full access, all rows, all columns)
  gw_analytics  → analytics-client (EMEA rows only, ssn + credit_card_number excluded)
  gw_ds         → data-scientist   (gold namespace denied — HTTP 403 from gateway)

What this test does
───────────────────
  1. Starts SparkSession with three named REST catalogs (one per principal).
  2. Seeds gw_admin.gold.orders with 6 rows across EMEA / APAC / AMER,
     including PII columns and PARTITIONED BY (region), format-version=2.
  3. Reads the table via each catalog — enforcement is automatic (no app-level filtering).
  4. Runs assertions to confirm row counts and column visibility.

Prerequisites
─────────────
  - Java 11+ on PATH
  - pip install pyspark
  - Stack running: cd docker && docker compose up -d

Run
───
  python scripts/test_pyspark_nessie.py
"""
from __future__ import annotations

import sys
from pyspark.sql import SparkSession

GATEWAY_URL = "http://localhost:8083"
OAUTH_URL   = "http://localhost:8081"
MINIO_URL   = "http://localhost:9000"
WAREHOUSE   = "s3a://warehouse/"

# Named catalogs: catalog_name → (client_id, client_secret, scope)
# Scope must match what the OAuth service has granted to each client.
# Iceberg REST Catalog defaults to "catalog" which the OAuth service rejects.
CATALOGS = {
    "gw_admin":     ("admin-client",     "admin-secret",     "catalog:read catalog:write catalog:admin catalog:drop"),
    "gw_analytics": ("analytics-client", "analytics-secret", "catalog:read"),
    "gw_ds":        ("data-scientist",   "ds-secret",        "catalog:read"),
}


# ── Spark catalog config ──────────────────────────────────────────────────────

def _catalog_configs(name: str, client_id: str, client_secret: str, scope: str) -> dict:
    """
    Iceberg RESTCatalog config for one principal.

    credential = client_id:client_secret  — triggers client_credentials grant.
    oauth2-server-uri — token endpoint (RESTCatalog fetches + caches tokens).
    scope — must match the OAuth service's allowed scopes; Iceberg defaults to
            "catalog" which this service does not accept.
    No Nessie-specific extensions or JARs needed; pure Iceberg REST protocol.
    """
    p = f"spark.sql.catalog.{name}"
    return {
        f"{p}":                    "org.apache.iceberg.spark.SparkCatalog",
        f"{p}.type":               "rest",
        f"{p}.uri":                GATEWAY_URL,
        f"{p}.credential":         f"{client_id}:{client_secret}",
        f"{p}.oauth2-server-uri":  f"{OAUTH_URL}/token",
        f"{p}.scope":              scope,
        f"{p}.warehouse":          WAREHOUSE,
    }


def build_spark() -> SparkSession:
    """
    SparkSession with one named Iceberg REST catalog per principal.

    JARs (downloaded from Maven Central on first run, ~150 MB, cached in ~/.ivy2):
      iceberg-spark-runtime  — Iceberg table format + REST catalog client for Spark 3.5
      hadoop-aws             — S3A filesystem for MinIO
      aws-java-sdk-bundle    — AWS SDK (S3A dependency)
    """
    packages = ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
        "org.apache.iceberg:iceberg-aws-bundle:1.9.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    builder = (
        SparkSession.builder
        .appName("gateway-policy-test")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # MinIO S3A — shared across all catalogs
        .config("spark.hadoop.fs.s3a.endpoint",                MINIO_URL)
        .config("spark.hadoop.fs.s3a.access.key",              "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",              "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",       "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Use in-memory upload buffer — avoids needing a /tmp/hadoop-*/s3a
        # temp directory (which may not exist on Windows).
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
    )

    for catalog_name, (client_id, client_secret, scope) in CATALOGS.items():
        for key, value in _catalog_configs(catalog_name, client_id, client_secret, scope).items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


# ── Seed ──────────────────────────────────────────────────────────────────────

def seed_table(spark: SparkSession) -> None:
    """
    Create gw_admin.gold.orders (format-version=2, PARTITIONED BY region) and insert 6 rows.

    Partitioning by region means the gateway's row_filter ("region = 'EMEA'") triggers
    Nessie file-level partition pruning — non-EMEA Parquet files are never fetched.
    DELETE + INSERT keeps repeated test runs deterministic.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gw_admin.gold")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gw_admin.gold.orders (
            id                  BIGINT,
            region              STRING,
            status              STRING,
            amount              DOUBLE,
            customer_email      STRING,
            ip_address          STRING,
            ssn                 STRING,
            credit_card_number  STRING
        )
        USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES (
            'format-version'        = '2',
            'write.format.default'  = 'parquet'
        )
    """)

    spark.sql("DELETE FROM gw_admin.gold.orders WHERE 1=1")

    spark.sql("""
        INSERT INTO gw_admin.gold.orders VALUES
          (1, 'EMEA', 'COMPLETED', 1500.0, 'alice@example.com',  '192.168.1.10', '123-45-6789', '4111111111111111'),
          (2, 'EMEA', 'COMPLETED', 2300.0, 'bob@example.com',    '10.0.0.2',     '987-65-4321', '4222222222222222'),
          (3, 'APAC', 'COMPLETED',  900.0, 'carol@example.com',  '172.16.0.5',   '456-78-9012', '4333333333333333'),
          (4, 'APAC', 'PENDING',   4100.0, 'dan@example.com',    '10.1.1.1',     '321-54-9876', '4444444444444444'),
          (5, 'AMER', 'COMPLETED', 3200.0, 'eve@example.com',    '192.168.2.20', '555-12-3456', '4555555555555555'),
          (6, 'AMER', 'PENDING',   1800.0, 'frank@example.com',  '10.2.0.1',     '666-34-5678', '4666666666666666')
    """)
    count = spark.sql("SELECT COUNT(*) FROM gw_admin.gold.orders").collect()[0][0]
    print(f"  gw_admin.gold.orders seeded — {count} rows")


# ── Read via gateway (enforcement transparent) ────────────────────────────────

def read_via_gateway(spark: SparkSession, catalog_name: str) -> None:
    """
    Read gold.orders through the named catalog.

    No application-level filtering — the gateway enforces:
      - Row filter injected into Iceberg scan expression (RLS)
      - Excluded columns stripped from table schema before Spark sees it (CLS)
    """
    print(f"\n{'-'*60}")
    print(f"Catalog: {catalog_name}")
    try:
        df = spark.table(f"{catalog_name}.gold.orders")
        print(f"  Visible columns: {df.columns}")
        print(f"  Row count:       {df.count()}")
        df.show(truncate=False)
    except Exception as exc:
        print(f"  [BLOCKED] Gateway denied access: {exc}")


# ── Assertions ────────────────────────────────────────────────────────────────

def assert_eq(label: str, actual, expected) -> None:
    status = "PASS" if actual == expected else "FAIL"
    print(f"  {status}  {label}")
    if actual != expected:
        print(f"        expected: {expected!r}")
        print(f"        got:      {actual!r}")
        sys.exit(1)


def run_assertions(spark: SparkSession) -> None:
    print(f"\n{'='*60}")
    print("Assertions")
    print(f"{'='*60}")

    # ── admin: unrestricted — all 6 rows, all columns ────────────────────────
    df_admin = spark.table("gw_admin.gold.orders")
    assert_eq("admin: sees all 6 rows",            df_admin.count(), 6)
    assert_eq("admin: ssn column visible",         "ssn" in df_admin.columns, True)
    assert_eq("admin: credit_card_number visible", "credit_card_number" in df_admin.columns, True)

    # ── analytics-client: EMEA only, PII excluded ────────────────────────────
    # Gateway enforces transparently:
    #   - Schema rewrite removes ssn + credit_card_number before Spark builds plan
    #   - Scan filter "region = 'EMEA'" injected → Nessie prunes non-EMEA files
    df_analytics = spark.table("gw_analytics.gold.orders")
    assert_eq("analytics: 2 EMEA rows (gateway RLS, no app filter)",
              df_analytics.count(), 2)
    assert_eq("analytics: ssn excluded from schema (gateway CLS)",
              "ssn" not in df_analytics.columns, True)
    assert_eq("analytics: credit_card_number excluded from schema (gateway CLS)",
              "credit_card_number" not in df_analytics.columns, True)

    regions = {r.region for r in df_analytics.select("region").collect()}
    assert_eq("analytics: only EMEA regions in result",
              regions, {"EMEA"})

    # ── data-scientist: gold namespace denied (HTTP 403 from gateway) ────────
    ds_denied = False
    try:
        spark.table("gw_ds.gold.orders").count()
    except Exception:
        ds_denied = True
    assert_eq("data-scientist: gold access denied (HTTP 403 from gateway)",
              ds_denied, True)

    print(f"\n  All assertions passed.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PySpark + Iceberg REST Catalog via catalog-gateway")
    print("OPA-enforced RLS + CLS (transparent to Spark)")
    print("=" * 60)

    print("\n[1/4] Starting SparkSession with per-principal REST catalogs...")
    print("      (First run downloads ~150 MB of JARs via Maven — subsequent runs use cache)")
    spark = build_spark()
    print(f"      Spark {spark.version} ready")
    print(f"      Catalogs: {', '.join(CATALOGS)}")
    print(f"      Gateway:  {GATEWAY_URL}")

    print("\n[2/4] Seeding gw_admin.gold.orders (format-version=2, PARTITIONED BY region)...")
    seed_table(spark)

    print("\n[3/4] Reading table via each principal's catalog (enforcement is automatic)...")
    for catalog_name in CATALOGS:
        read_via_gateway(spark, catalog_name)

    print("\n[4/4] Running assertions...")
    run_assertions(spark)

    spark.stop()
    print("\nDone.")
