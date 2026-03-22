"""
iceberg_sync_dag.py — Platform-agnostic Iceberg sync DAG factory.

ONE factory function generates a DAG for any source → target combination.
To add a new platform, add an entry to SYNC_PIPELINES — no new DAG code needed.

How it works
────────────
1.  Each entry in SYNC_PIPELINES is a SyncPipelineConfig.
2.  make_iceberg_sync_dag() builds a DAG from that config:
        sync_<table> → health_<table> → [nessie_<table>]
    All tables run in parallel inside the DAG.
3.  All generated DAGs are registered in globals() so Airflow picks them up.

Adding a new platform
─────────────────────
Append to SYNC_PIPELINES:

    SyncPipelineConfig(
        dag_id          = "iceberg_sync_gcs_to_nessie",
        description     = "GCS → on-prem Nessie",
        schedule        = "*/30 * * * *",
        source_root     = "gs://my-warehouse/iceberg/",
        target_root     = "s3a://warehouse/",
        tables          = ["gold/orders"],
        source_kwargs   = {"project": "my-gcp-project"},
        target_kwargs   = _minio_kwargs(),
        nessie_uri      = "http://nessie:19120",
        source_scheme   = "gs",
    )

Airflow Variables (set via UI Admin → Variables or CLI)
───────────────────────────────────────────────────────
    ICEBERG_ADLS_SOURCE_ROOT      abfss://iceberg@mystorageacct.dfs.core.windows.net/
    ICEBERG_MINIO_ENDPOINT        http://minio:9000
    ICEBERG_MINIO_ACCESS_KEY      minioadmin
    ICEBERG_MINIO_SECRET_KEY      minioadmin
    ICEBERG_NESSIE_URI            http://nessie:19120
    ICEBERG_S3_SOURCE_ROOT        s3://warehouse/iceberg/
    ICEBERG_ADLS_TARGET_ROOT      abfss://iceberg@account.dfs.core.windows.net/iceberg/
    AZURE_STORAGE_ACCOUNT         sticebergpipeline
    AWS_DEFAULT_REGION            eu-west-2
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from iceberg_sync.airflow.operators import (
    IcebergHealthCheckOperator,
    IcebergTableSyncOperator,
    NessieCatalogRegisterOperator,
)

# ── Helpers to build storage kwargs from Airflow Variables ───────────────────


def _minio_kwargs() -> Dict[str, Any]:
    return {
        "endpoint_url": Variable.get("ICEBERG_MINIO_ENDPOINT", "http://minio:9000"),
        "aws_access_key_id": Variable.get("ICEBERG_MINIO_ACCESS_KEY", "minioadmin"),
        "aws_secret_access_key": Variable.get("ICEBERG_MINIO_SECRET_KEY", "minioadmin"),
        "region_name": "us-east-1",
    }


def _adls_source_kwargs() -> Dict[str, Any]:
    source_root = Variable.get(
        "ICEBERG_ADLS_SOURCE_ROOT",
        "abfss://iceberg@mystorageacct.dfs.core.windows.net/",
    )
    account = source_root.split("@")[1].split(".")[0] if "@" in source_root else ""
    return {"storage_account_name": account}


def _s3_source_kwargs() -> Dict[str, Any]:
    return {"region_name": Variable.get("AWS_DEFAULT_REGION", "eu-west-2")}


def _adls_target_kwargs() -> Dict[str, Any]:
    return {"storage_account_name": Variable.get("AZURE_STORAGE_ACCOUNT", "sticebergpipeline")}


# ── Pipeline config ──────────────────────────────────────────────────────────


@dataclass
class SyncPipelineConfig:
    """
    Full configuration for one source→target sync pipeline.

    Args:
        dag_id:         Unique Airflow DAG ID.
        description:    Human-readable description shown in Airflow UI.
        schedule:       Cron expression or None (trigger-only).
        source_root:    Source warehouse root URI.
        target_root:    Target warehouse root URI.
        tables:         List of table paths (e.g. ["gold/top_customers"]).
                        Each becomes a parallel task group inside the DAG.
        source_kwargs:  Passed to create_storage() for the source backend.
        target_kwargs:  Passed to create_storage() for the target backend.
        source_scheme:  URI scheme of source storage (e.g. "abfss", "s3", "gs").
                        Used by the health check to detect leaked source URIs.
        nessie_uri:     If set, a NessieCatalogRegisterOperator is added after
                        each health check to register the table in Nessie.
        nessie_ref:     Nessie branch (default: "main").
        tags:           Additional Airflow tags.
        max_parallel_copies: Threads per table copy operation.
    """
    dag_id: str
    description: str
    schedule: Optional[str]
    source_root: str
    target_root: str
    tables: List[str]
    source_kwargs: Dict[str, Any]
    target_kwargs: Dict[str, Any]
    source_scheme: str
    nessie_uri: Optional[str] = None
    nessie_ref: str = "main"
    tags: List[str] = field(default_factory=list)
    max_parallel_copies: int = 8


# ── Pipeline registry ─────────────────────────────────────────────────────────
# Add new platforms here — the factory does the rest.

SYNC_PIPELINES: List[SyncPipelineConfig] = [

    # ── Azure Fabric / ADLS → On-prem Nessie (main POC) ─────────────────────
    SyncPipelineConfig(
        dag_id="iceberg_sync_adls_to_nessie",
        description="Incremental sync: Azure Fabric ADLS → on-prem MinIO + Nessie catalog",
        schedule="*/15 * * * *",
        source_root=Variable.get(
            "ICEBERG_ADLS_SOURCE_ROOT",
            "abfss://iceberg@mystorageacct.dfs.core.windows.net/",
        ),
        target_root="s3a://warehouse/",
        tables=[
            "gold/top_customers",
            "gold/revenue_by_order_date",
        ],
        source_kwargs=_adls_source_kwargs(),
        target_kwargs=_minio_kwargs(),
        source_scheme="abfss",
        nessie_uri=Variable.get("ICEBERG_NESSIE_URI", "http://nessie:19120"),
        tags=["iceberg", "adls", "nessie", "on-prem", "poc"],
    ),

    # ── AWS S3 → On-prem Nessie ──────────────────────────────────────────────
    SyncPipelineConfig(
        dag_id="iceberg_sync_s3_to_nessie",
        description="Incremental sync: AWS S3 → on-prem MinIO + Nessie catalog",
        schedule="*/15 * * * *",
        source_root=Variable.get("ICEBERG_S3_SOURCE_ROOT", "s3://warehouse/iceberg/"),
        target_root="s3a://warehouse/",
        tables=[
            "gold/top_customers",
            "gold/revenue_by_order_date",
        ],
        source_kwargs=_s3_source_kwargs(),
        target_kwargs=_minio_kwargs(),
        source_scheme="s3",
        nessie_uri=Variable.get("ICEBERG_NESSIE_URI", "http://nessie:19120"),
        tags=["iceberg", "s3", "nessie", "on-prem"],
    ),

    # ── AWS S3 → Azure ADLS Gen2 (DR / active-passive) ──────────────────────
    SyncPipelineConfig(
        dag_id="iceberg_sync_s3_to_adls",
        description="Critical tables: AWS S3 → Azure ADLS Gen2 (disaster recovery)",
        schedule=None,  # triggered by primary pipeline DAG
        source_root=Variable.get("ICEBERG_S3_SOURCE_ROOT", "s3://warehouse/iceberg/"),
        target_root=Variable.get(
            "ICEBERG_ADLS_TARGET_ROOT",
            "abfss://iceberg@account.dfs.core.windows.net/iceberg/",
        ),
        tables=[
            "gold/revenue_by_order_date",
            "gold/top_customers",
            "silver/orders",
        ],
        source_kwargs=_s3_source_kwargs(),
        target_kwargs=_adls_target_kwargs(),
        source_scheme="s3",
        nessie_uri=None,  # no Nessie registration — ADLS is the target catalog
        tags=["iceberg", "s3", "adls", "dr"],
    ),
]


# ── DAG factory ───────────────────────────────────────────────────────────────

_DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["platform-team@company.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def make_iceberg_sync_dag(cfg: SyncPipelineConfig) -> DAG:
    """
    Build a complete Airflow DAG from a SyncPipelineConfig.

    Generated task graph (per table, all parallel):

        start
          ├─ sync_<table_1>  → health_<table_1>  → [nessie_<table_1>]  ─┐
          ├─ sync_<table_2>  → health_<table_2>  → [nessie_<table_2>]  ─┤→ all_done
          └─ sync_<table_N>  → health_<table_N>  → [nessie_<table_N>]  ─┘
    """
    with DAG(
        dag_id=cfg.dag_id,
        default_args=_DEFAULT_ARGS,
        description=cfg.description,
        schedule_interval=cfg.schedule,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=cfg.tags,
        max_active_runs=1,
    ) as dag:

        start = EmptyOperator(task_id="start")
        all_done = EmptyOperator(task_id="all_done", trigger_rule="all_success")

        for table_path in cfg.tables:
            safe = table_path.replace("/", "_").replace(".", "_")
            parts = table_path.strip("/").split("/")
            ns = ".".join(parts[:-1]) if len(parts) > 1 else "default"
            tbl = parts[-1]

            sync = IcebergTableSyncOperator(
                task_id=f"sync_{safe}",
                source_root=cfg.source_root,
                target_root=cfg.target_root,
                table=table_path,
                source_storage_kwargs=cfg.source_kwargs,
                target_storage_kwargs=cfg.target_kwargs,
                max_parallel_copies=cfg.max_parallel_copies,
                fail_on_error=True,
            )

            health = IcebergHealthCheckOperator(
                task_id=f"health_{safe}",
                target_root=cfg.target_root,
                table=table_path,
                source_scheme=cfg.source_scheme,
                target_storage_kwargs=cfg.target_kwargs,
            )

            chain_end = health

            if cfg.nessie_uri:
                register = NessieCatalogRegisterOperator(
                    task_id=f"nessie_{safe}",
                    nessie_uri=cfg.nessie_uri,
                    namespace=ns,
                    table=tbl,
                    nessie_ref=cfg.nessie_ref,
                    sync_task_id=f"sync_{safe}",
                    fail_on_error=True,
                )
                health >> register
                chain_end = register

            start >> sync >> health
            chain_end >> all_done

    return dag


# ── Generate all DAGs and register them in module globals ─────────────────────
# Airflow discovers DAGs by scanning module globals for DAG objects.

for _pipeline in SYNC_PIPELINES:
    globals()[_pipeline.dag_id] = make_iceberg_sync_dag(_pipeline)


# ── Failover monitor DAG (standalone — not generated by factory) ──────────────

def _check_aws_health(**context):
    """Branch: returns 'aws_healthy' or 'aws_unhealthy'."""
    import boto3  # type: ignore[import-untyped]
    from botocore.exceptions import ClientError, EndpointConnectionError  # type: ignore[import-untyped]

    source_root = Variable.get("ICEBERG_S3_SOURCE_ROOT", "s3://warehouse/iceberg/")
    bucket = source_root.split("/")[2]
    region = Variable.get("AWS_DEFAULT_REGION", "eu-west-2")

    try:
        boto3.client("s3", region_name=region).head_bucket(Bucket=bucket)
        return "aws_healthy"
    except (ClientError, EndpointConnectionError, Exception) as e:
        context["ti"].xcom_push(key="health_error", value=str(e))
        return "aws_unhealthy"


def _activate_failover(**_):
    Variable.set("FAILOVER_ACTIVE", "true")
    Variable.set("FAILOVER_ACTIVATED_AT", datetime.now(timezone.utc).isoformat())


def _deactivate_failover(**_):
    if Variable.get("FAILOVER_ACTIVE", "false") == "true":
        Variable.set("FAILOVER_ACTIVE", "false")
        Variable.set("FAILOVER_DEACTIVATED_AT", datetime.now(timezone.utc).isoformat())


with DAG(
    dag_id="iceberg_failover_monitor",
    default_args=_DEFAULT_ARGS,
    description="Periodic AWS health check — sets FAILOVER_ACTIVE variable",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iceberg", "failover", "monitoring"],
    max_active_runs=1,
) as iceberg_failover_monitor:

    check = BranchPythonOperator(task_id="check_aws_health", python_callable=_check_aws_health)
    healthy = PythonOperator(task_id="aws_healthy", python_callable=_deactivate_failover)
    unhealthy = PythonOperator(task_id="aws_unhealthy", python_callable=_activate_failover)
    check >> [healthy, unhealthy]
