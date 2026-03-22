"""
iceberg_sync_dag.py — Production Airflow DAG for Iceberg cross-cloud sync.

This DAG runs after your primary pipeline completes and:
1. Syncs gold-tier Iceberg tables from AWS S3 to Azure ADLS Gen2.
2. Validates the synced tables with a health check.
3. Optionally triggers failover if AWS health check fails.

Schedule: After each pipeline run (trigger via TriggerDagRunOperator)
          or on a fixed schedule (e.g. every 15 minutes for critical tables).

Prerequisites:
    pip install iceberg-catalog-sync apache-airflow

Environment variables (or Airflow Variables / Connections):
    ICEBERG_SYNC_SOURCE_ROOT  = s3://warehouse/iceberg/
    ICEBERG_SYNC_TARGET_ROOT  = abfss://iceberg@account.dfs.core.windows.net/iceberg/
    AZURE_STORAGE_ACCOUNT     = sticebergpipeline
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from iceberg_sync.airflow.operators import (
    IcebergTableSyncOperator,
    IcebergNamespaceSyncOperator,
    IcebergHealthCheckOperator,
)


# ── Configuration ────────────────────────────────────────────────────────────

SOURCE_ROOT = Variable.get("ICEBERG_SYNC_SOURCE_ROOT", "s3://warehouse/iceberg/")
TARGET_ROOT = Variable.get("ICEBERG_SYNC_TARGET_ROOT", "abfss://iceberg@account.dfs.core.windows.net/iceberg/")
AZURE_ACCOUNT = Variable.get("AZURE_STORAGE_ACCOUNT", "sticebergpipeline")

# Tables tiered by criticality
TIER_1_TABLES = [
    "gold/revenue_by_order_date",
    "gold/top_customers",
]

TIER_2_TABLES = [
    "silver/orders",
    "silver/lineitem",
]

# ── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["platform-team@company.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG: Tier 1 — Critical tables (sync after every pipeline run) ────────────

with DAG(
    dag_id="iceberg_sync_tier1",
    default_args=default_args,
    description="Sync critical gold Iceberg tables from AWS to Azure",
    schedule_interval=None,  # Triggered by primary pipeline DAG
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iceberg", "sync", "multi-cloud", "tier-1"],
    max_active_runs=1,
) as dag_tier1:

    start = EmptyOperator(task_id="start")

    # Sync each critical table in parallel
    sync_tasks = []
    health_tasks = []

    for table_path in TIER_1_TABLES:
        task_name = table_path.replace("/", "_")

        sync = IcebergTableSyncOperator(
            task_id=f"sync_{task_name}",
            source_root=SOURCE_ROOT,
            target_root=TARGET_ROOT,
            table=table_path,
            source_storage_kwargs={"region_name": "eu-west-2"},
            target_storage_kwargs={"storage_account_name": AZURE_ACCOUNT},
            max_parallel_copies=4,
            fail_on_error=True,
        )

        health = IcebergHealthCheckOperator(
            task_id=f"health_{task_name}",
            target_root=TARGET_ROOT,
            table=table_path,
            source_scheme="s3",
            target_storage_kwargs={"storage_account_name": AZURE_ACCOUNT},
        )

        start >> sync >> health
        sync_tasks.append(sync)
        health_tasks.append(health)

    done = EmptyOperator(
        task_id="all_synced",
        trigger_rule="all_success",
    )

    for h in health_tasks:
        h >> done


# ── DAG: Tier 2 — Silver tables (nightly batch sync) ────────────────────────

with DAG(
    dag_id="iceberg_sync_tier2_nightly",
    default_args=default_args,
    description="Nightly sync of silver Iceberg tables from AWS to Azure",
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iceberg", "sync", "multi-cloud", "tier-2"],
    max_active_runs=1,
) as dag_tier2:

    sync_silver = IcebergNamespaceSyncOperator(
        task_id="sync_silver_namespace",
        source_root=SOURCE_ROOT,
        target_root=TARGET_ROOT,
        namespace="silver",
        source_storage_kwargs={"region_name": "eu-west-2"},
        target_storage_kwargs={"storage_account_name": AZURE_ACCOUNT},
        max_parallel_copies=8,
        fail_on_error=False,  # Warn but don't fail — tier 2 is not critical path
    )


# ── DAG: Failover Check — periodic AWS health check ─────────────────────────

def _check_aws_health(**context):
    """
    Check if AWS S3 is reachable.  If not, set the FAILOVER_ACTIVE variable.
    Returns the branch task to execute next.
    """
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError

    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        # Try a lightweight operation
        s3.head_bucket(Bucket=SOURCE_ROOT.split("/")[2])
        return "aws_healthy"
    except (ClientError, EndpointConnectionError, Exception) as e:
        context["ti"].xcom_push(key="health_error", value=str(e))
        return "aws_unhealthy"


def _activate_failover(**context):
    """Set the failover flag.  Downstream DAGs check this variable."""
    Variable.set("FAILOVER_ACTIVE", "true")
    Variable.set("FAILOVER_ACTIVATED_AT", datetime.utcnow().isoformat())
    # This is where you'd also trigger alerts:
    # - PagerDuty / OpsGenie
    # - Slack notification
    # - Email to on-call


def _deactivate_failover(**context):
    """Clear the failover flag when AWS is healthy again."""
    current = Variable.get("FAILOVER_ACTIVE", "false")
    if current == "true":
        Variable.set("FAILOVER_ACTIVE", "false")
        Variable.set("FAILOVER_DEACTIVATED_AT", datetime.utcnow().isoformat())


with DAG(
    dag_id="iceberg_failover_monitor",
    default_args=default_args,
    description="Periodic AWS health check for Iceberg failover",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iceberg", "failover", "monitoring"],
    max_active_runs=1,
) as dag_failover:

    check = BranchPythonOperator(
        task_id="check_aws_health",
        python_callable=_check_aws_health,
    )

    healthy = PythonOperator(
        task_id="aws_healthy",
        python_callable=_deactivate_failover,
    )

    unhealthy = PythonOperator(
        task_id="aws_unhealthy",
        python_callable=_activate_failover,
    )

    check >> [healthy, unhealthy]
