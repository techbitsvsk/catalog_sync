"""
iceberg_sync.archive — Iceberg table archival, retention, and partition restore.

Provides:
  IcebergArchiver   — periodically copies snapshots to cold storage and expires
                      them from the primary catalog.
  IcebergRestorer   — restores a full table or specific partitions from cold
                      storage back into the live catalog, with a mandatory
                      dry-run plan step before any writes occur.

Typical usage
─────────────
Archive (scheduled nightly via Airflow or cron):

    from iceberg_sync.archive import IcebergArchiver
    from iceberg_sync.archive.config import ArchiveJobConfig

    cfg = ArchiveJobConfig.from_yaml("archive-job.yaml")
    archiver = IcebergArchiver.from_config(cfg)
    result = archiver.archive_table("gold/orders", dry_run=False)

Restore (on-demand, always plan first):

    from iceberg_sync.archive import IcebergRestorer
    from iceberg_sync.archive.config import RestoreJobConfig

    cfg = RestoreJobConfig.from_yaml("restore-job.yaml")
    restorer = IcebergRestorer.from_config(cfg)

    plan = restorer.plan()           # dry-run — prints plan, returns RestorePlan
    result = restorer.execute(plan)  # only runs after plan is reviewed
"""

from iceberg_sync.archive.archiver import IcebergArchiver
from iceberg_sync.archive.restorer import IcebergRestorer

__all__ = ["IcebergArchiver", "IcebergRestorer"]
