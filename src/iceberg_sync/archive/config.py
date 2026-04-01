"""
archive/config.py — Configuration dataclasses and YAML loader for archive/restore jobs.

Environment variable substitution is supported in YAML values using ${VAR} syntax.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


def storage_kwargs_for_uri(uri: str, s3: "S3Config", adls: "ADLSConfig") -> Dict[str, Any]:
    """
    Return the correct credential kwargs for the given URI scheme.

    Selects S3 or ADLS credentials based on the URI prefix so that
    credentials for one backend are never accidentally passed to another.
    """
    scheme = uri.split("://")[0].lower()
    if scheme in ("s3", "s3a", "s3n"):
        return s3.to_storage_kwargs()
    if scheme in ("abfss", "abfs"):
        return adls.to_storage_kwargs()
    return {}


# Shorter alias used inside archiver / restorer
_storage_kwargs = storage_kwargs_for_uri


def _expand_env(value: Any) -> Any:
    """Recursively substitute ${VAR} placeholders with environment variable values."""
    if isinstance(value, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


# ──────────────────────────────────────────────────────────────────────────────
# Shared sub-configs
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class S3Config:
    region: Optional[str] = None
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Dict) -> "S3Config":
        return cls(
            region=d.get("region"),
            endpoint=d.get("endpoint"),
            access_key=d.get("access_key"),
            secret_key=d.get("secret_key"),
        )

    def to_storage_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if self.region:
            kwargs["region_name"] = self.region
        if self.endpoint:
            kwargs["endpoint_url"] = self.endpoint
        if self.access_key:
            kwargs["aws_access_key_id"] = self.access_key
        if self.secret_key:
            kwargs["aws_secret_access_key"] = self.secret_key
        return kwargs


@dataclass
class ADLSConfig:
    account_name: Optional[str] = None
    account_key: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Dict) -> "ADLSConfig":
        return cls(
            account_name=d.get("account_name"),
            account_key=d.get("account_key"),
        )

    def to_storage_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if self.account_name:
            kwargs["storage_account_name"] = self.account_name
        if self.account_key:
            kwargs["storage_account_key"] = self.account_key
        return kwargs


@dataclass
class CatalogConfig:
    nessie_uri: Optional[str] = None
    nessie_ref: str = "main"
    oauth_url: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Dict) -> "CatalogConfig":
        return cls(
            nessie_uri=d.get("nessie_uri"),
            nessie_ref=d.get("nessie_ref", "main"),
            oauth_url=d.get("oauth_url"),
            oauth_client_id=d.get("oauth_client_id"),
            oauth_client_secret=d.get("oauth_client_secret"),
        )


@dataclass
class TransferConfig:
    parallelism: int = 8
    verify_checksum: bool = True

    @classmethod
    def from_dict(cls, d: Dict) -> "TransferConfig":
        return cls(
            parallelism=int(d.get("parallelism", 8)),
            verify_checksum=bool(d.get("verify_checksum", True)),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Archive job config
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ArchiveJobConfig:
    """Configuration for a periodic archive run (primary → cold storage)."""

    # Source (primary / hot storage)
    source_root: str
    table: Optional[str] = None          # mutually exclusive with namespace
    namespace: Optional[str] = None

    # Destination (cold / archive storage)
    archive_root: str = ""

    # Retention policy applied before archiving
    older_than: str = "30d"              # snapshots older than this are archived
    min_snapshots_to_keep: int = 1       # never archive below this count

    # Whether to delete archived snapshots from primary after copy
    delete_after_archive: bool = True

    dry_run: bool = True

    # Storage credentials
    source_s3: S3Config = field(default_factory=S3Config)
    source_adls: ADLSConfig = field(default_factory=ADLSConfig)
    archive_s3: S3Config = field(default_factory=S3Config)
    archive_adls: ADLSConfig = field(default_factory=ADLSConfig)

    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    transfer: TransferConfig = field(default_factory=TransferConfig)

    @classmethod
    def from_yaml(cls, path: str) -> "ArchiveJobConfig":
        with open(path) as f:
            raw = _expand_env(yaml.safe_load(f))

        source = raw.get("source", {})
        archive = raw.get("archive", {})
        policy = raw.get("policy", {})

        return cls(
            source_root=source["root"],
            table=source.get("table"),
            namespace=source.get("namespace"),
            archive_root=archive["root"],
            older_than=policy.get("older_than", "30d"),
            min_snapshots_to_keep=int(policy.get("min_snapshots_to_keep", 1)),
            delete_after_archive=bool(policy.get("delete_after_archive", True)),
            dry_run=bool(raw.get("dry_run", True)),
            source_s3=S3Config.from_dict(source.get("s3", {})),
            source_adls=ADLSConfig.from_dict(source.get("adls", {})),
            archive_s3=S3Config.from_dict(archive.get("s3", {})),
            archive_adls=ADLSConfig.from_dict(archive.get("adls", {})),
            catalog=CatalogConfig.from_dict(raw.get("catalog", {})),
            transfer=TransferConfig.from_dict(raw.get("transfer", {})),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Restore job config
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class RestoreJobConfig:
    """Configuration for a restore run (cold storage → primary table)."""

    # Where archived data lives
    archive_root: str
    table: str                            # table path, e.g. "gold/orders"

    # Where to restore to (defaults to same path in target_root)
    target_root: str
    restore_as: Optional[str] = None      # if set, restore to a different table name

    # What to restore
    as_of: Optional[str] = None           # ISO date, e.g. "2025-12-01"
    snapshot_id: Optional[int] = None     # explicit snapshot id

    # Partition selection (empty list = full table restore)
    partitions: List[Dict[str, Any]] = field(default_factory=list)

    # Behaviour when restored partition already exists in live table
    mode: str = "replace"                 # replace | append | new_table
    conflict_strategy: str = "fail"       # fail | skip | overwrite

    dry_run: bool = True

    # Storage credentials
    archive_s3: S3Config = field(default_factory=S3Config)
    archive_adls: ADLSConfig = field(default_factory=ADLSConfig)
    target_s3: S3Config = field(default_factory=S3Config)
    target_adls: ADLSConfig = field(default_factory=ADLSConfig)

    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    transfer: TransferConfig = field(default_factory=TransferConfig)

    @classmethod
    def from_yaml(cls, path: str) -> "RestoreJobConfig":
        with open(path) as f:
            raw = _expand_env(yaml.safe_load(f))

        source = raw.get("source", {})
        target = raw.get("target", {})
        restore = raw.get("restore", {})

        partitions = restore.get("partitions", [])
        if isinstance(partitions, list):
            partitions = [dict(p) for p in partitions]

        return cls(
            archive_root=source["archive_root"],
            table=source["table"],
            target_root=target["root"],
            restore_as=target.get("restore_as"),
            as_of=restore.get("as_of"),
            snapshot_id=restore.get("snapshot_id"),
            partitions=partitions,
            mode=restore.get("mode", "replace"),
            conflict_strategy=restore.get("conflict_strategy", "fail"),
            dry_run=bool(raw.get("dry_run", True)),
            archive_s3=S3Config.from_dict(source.get("s3", {})),
            archive_adls=ADLSConfig.from_dict(source.get("adls", {})),
            target_s3=S3Config.from_dict(target.get("s3", {})),
            target_adls=ADLSConfig.from_dict(target.get("adls", {})),
            catalog=CatalogConfig.from_dict(raw.get("catalog", {})),
            transfer=TransferConfig.from_dict(raw.get("transfer", {})),
        )

    @property
    def effective_target_table(self) -> str:
        return self.restore_as or self.table
