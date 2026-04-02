"""
e2e/airflow_dags/archive_pipeline_dag.py
==========================================
End-to-end Airflow DAG demonstrating the full archive + restore pipeline
for the tpch/orders Iceberg table.

DAG structure:
  check_services
       │
  archive_dry_run   ← print plan, never writes
       │
  archive_execute   ← copy files to cold storage, expire from primary
       │
  list_archived_snapshots
       │
  restore_dry_run   ← print restore plan
       │
  restore_execute   ← copy files back, rewrite metadata
       │
  verify_row_count  ← assert partition is visible again

Schedule: weekly (adjust to match your retention policy)

Prerequisites:
  pip install apache-airflow
  pip install iceberg-catalog-sync[archive]

  Airflow connections required:
    None — credentials are passed inline via environment variables below.
    In production, inject via Airflow Connections or Secrets Backend.

Start Airflow (local — extends docker/docker-compose.yml):
  docker compose -f docker/docker-compose.yml \
                 -f docker/docker-compose.airflow.yml up -d
  open http://localhost:8080  (admin / admin)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Constants ────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")          # inside Docker
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
OAUTH_URL        = os.getenv("OAUTH_URL",        "http://oauth-service:8081/oauth2/token")
NESSIE_URI       = os.getenv("NESSIE_URI",       "http://nessie:19120")

SOURCE_ROOT   = "s3a://warehouse/iceberg"
ARCHIVE_ROOT  = "s3a://warehouse/archive"
TABLE         = "tpch/orders"
RESTORE_MONTH = "2024-03"

_S3_KWARGS = {
    "endpoint":   MINIO_ENDPOINT,
    "access_key": MINIO_ACCESS_KEY,
    "secret_key": MINIO_SECRET_KEY,
    "region":     "us-east-1",
}

_CATALOG_KWARGS = {
    "nessie_uri":          NESSIE_URI,
    "nessie_ref":          "main",
    "oauth_url":           OAUTH_URL,
    "oauth_client_id":     "sync-service",
    "oauth_client_secret": "sync-secret",
}

# ── Helper: build ArchiveJobConfig ────────────────────────────────────────────

def _archive_config(dry_run: bool):
    from iceberg_sync.archive.config import (
        ArchiveJobConfig, CatalogConfig, S3Config, TransferConfig,
    )
    s3 = S3Config(**_S3_KWARGS)
    return ArchiveJobConfig(
        source_root=SOURCE_ROOT,
        table=TABLE,
        archive_root=ARCHIVE_ROOT,
        older_than="150d",
        min_snapshots_to_keep=5,
        delete_after_archive=True,
        dry_run=dry_run,
        source_s3=s3,
        archive_s3=s3,
        catalog=CatalogConfig(**_CATALOG_KWARGS),
        transfer=TransferConfig(parallelism=8),
    )


def _restore_config():
    from iceberg_sync.archive.config import (
        RestoreJobConfig, CatalogConfig, S3Config, TransferConfig,
    )
    s3 = S3Config(**_S3_KWARGS)
    return RestoreJobConfig(
        archive_root=ARCHIVE_ROOT,
        table=TABLE,
        target_root=SOURCE_ROOT,
        as_of="2024-04-01",
        partitions=[{"order_month": RESTORE_MONTH}],
        mode="replace",
        conflict_strategy="skip",
        archive_s3=s3,
        target_s3=s3,
        catalog=CatalogConfig(**_CATALOG_KWARGS),
        transfer=TransferConfig(parallelism=8),
    )


# ── Task functions ────────────────────────────────────────────────────────────

def task_check_services(**context):
    """Smoke-test: verify MinIO and OAuth are reachable."""
    import urllib.request
    import urllib.error

    checks = {
        "MinIO health":  f"{MINIO_ENDPOINT}/minio/health/live",
        "OAuth health":  OAUTH_URL.replace("/oauth2/token", "/health"),
    }
    for name, url in checks.items():
        try:
            with urllib.request.urlopen(url, timeout=5):
                print(f"  {name}: OK ({url})")
        except Exception as exc:
            raise RuntimeError(f"{name} unreachable at {url}: {exc}") from exc


def task_archive_dry_run(**context):
    """Print the archive plan without writing anything."""
    from iceberg_sync.archive.archiver import IcebergArchiver
    cfg = _archive_config(dry_run=True)
    archiver = IcebergArchiver.from_config(cfg)
    result = archiver.archive_table(TABLE, dry_run=True)
    summary = {
        "snapshots_to_archive": result.snapshots_archived,
        "files_to_copy": result.files_copied,
        "bytes_to_copy": result.bytes_copied,
    }
    context["ti"].xcom_push(key="archive_plan", value=summary)
    print(f"Archive plan: {summary}")
    return summary


def task_archive_execute(**context):
    """Execute the archive: copy files to cold storage, expire from primary."""
    from iceberg_sync.archive.archiver import IcebergArchiver
    cfg = _archive_config(dry_run=False)
    archiver = IcebergArchiver.from_config(cfg)
    result = archiver.archive_table(TABLE, dry_run=False)

    if not result.success:
        raise RuntimeError(f"Archive failed: {result.errors}")

    summary = {
        "snapshots_archived": result.snapshots_archived,
        "files_copied": result.files_copied,
        "bytes_copied": result.bytes_copied,
    }
    context["ti"].xcom_push(key="archive_result", value=summary)
    print(f"Archive complete: {summary}")
    return summary


def task_list_archived_snapshots(**context):
    """List available snapshots in the archive index."""
    from iceberg_sync.archive.config import RestoreJobConfig, S3Config, TransferConfig
    from iceberg_sync.archive.restorer import IcebergRestorer
    s3 = S3Config(**_S3_KWARGS)
    cfg = RestoreJobConfig(
        archive_root=ARCHIVE_ROOT,
        table=TABLE,
        target_root=SOURCE_ROOT,
        archive_s3=s3,
        target_s3=s3,
        transfer=TransferConfig(parallelism=8),
    )
    restorer = IcebergRestorer.from_config(cfg)
    snapshots = restorer.list_snapshots()
    snap_ids = [s.snapshot_id for s in snapshots]
    context["ti"].xcom_push(key="snapshot_ids", value=snap_ids)
    print(f"Available archived snapshots: {snap_ids}")
    return snap_ids


def task_restore_dry_run(**context):
    """Print the restore plan without writing anything."""
    from iceberg_sync.archive.restorer import IcebergRestorer
    cfg = _restore_config()
    restorer = IcebergRestorer.from_config(cfg)
    plan = restorer.plan()
    summary = {
        "files_to_restore": plan.total_files,
        "bytes_to_restore": plan.total_bytes,
        "has_conflicts": plan.has_conflicts,
    }
    context["ti"].xcom_push(key="restore_plan", value=summary)
    print(f"Restore plan: {summary}")
    return summary


def task_restore_execute(**context):
    """Execute the restore: copy files back and rewrite metadata."""
    from iceberg_sync.archive.restorer import IcebergRestorer
    cfg = _restore_config()
    restorer = IcebergRestorer.from_config(cfg)
    plan = restorer.plan()
    result = restorer.execute(plan)

    if not result.success:
        raise RuntimeError(f"Restore failed: {result.errors}")

    summary = {
        "files_restored": result.files_copied,
        "bytes_restored": result.bytes_copied,
        "new_metadata_uri": result.new_metadata_uri,
    }
    context["ti"].xcom_push(key="restore_result", value=summary)
    print(f"Restore complete: {summary}")
    return summary


def task_verify_row_count(**context):
    """Assert that the restored partition is visible via the catalog."""
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": os.getenv("GATEWAY_URL", "http://catalog-gateway:8083"),
            "oauth2-server-uri": OAUTH_URL,
            "credential": "admin-client:admin-secret",
            "scope": "catalog:read",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
    table = catalog.load_table("tpch.orders")

    import pyarrow.compute as pc
    arrow_tbl = table.scan(
        row_filter=f"order_month = '{RESTORE_MONTH}'"
    ).to_arrow()

    row_count = len(arrow_tbl)
    assert row_count > 0, (
        f"Restored partition order_month={RESTORE_MONTH} has 0 rows — restore may have failed!"
    )
    print(f"Verification OK: order_month={RESTORE_MONTH} has {row_count:,} rows ✓")
    return {"restored_month": RESTORE_MONTH, "row_count": row_count}


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="iceberg_archive_restore_e2e",
    description="Archive old tpch/orders partitions and restore one to demonstrate E2E workflow",
    schedule="@weekly",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["iceberg", "archive", "e2e"],
) as dag:

    check_services = PythonOperator(
        task_id="check_services",
        python_callable=task_check_services,
    )

    archive_dry_run = PythonOperator(
        task_id="archive_dry_run",
        python_callable=task_archive_dry_run,
    )

    archive_execute = PythonOperator(
        task_id="archive_execute",
        python_callable=task_archive_execute,
    )

    list_snapshots = PythonOperator(
        task_id="list_archived_snapshots",
        python_callable=task_list_archived_snapshots,
    )

    restore_dry_run = PythonOperator(
        task_id="restore_dry_run",
        python_callable=task_restore_dry_run,
    )

    restore_execute = PythonOperator(
        task_id="restore_execute",
        python_callable=task_restore_execute,
    )

    verify = PythonOperator(
        task_id="verify_row_count",
        python_callable=task_verify_row_count,
    )

    # Pipeline wiring
    (
        check_services
        >> archive_dry_run
        >> archive_execute
        >> list_snapshots
        >> restore_dry_run
        >> restore_execute
        >> verify
    )
