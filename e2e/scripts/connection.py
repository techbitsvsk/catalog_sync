"""
e2e/scripts/connection.py
===========================
IcebergConnectionBuilder — fluent builder that produces an IcebergConnection
configured for either S3/MinIO or ADLS Gen2/Fabric.

The builder separates connection concerns from workflow logic.  Every script
in e2e/scripts/ imports this module instead of managing credential dicts inline.

Usage:

    from connection import IcebergConnectionBuilder

    # ── MinIO / local S3 ──────────────────────────────────────────────────
    conn = (
        IcebergConnectionBuilder()
        .s3(endpoint="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin")
        .catalog_gateway("http://localhost:8083")
        .oauth("http://localhost:8081/token",
               client_id="admin-client",
               client_secret="admin-secret")
        .warehouse("s3a://warehouse/iceberg")
        .archive("s3a://warehouse/archive")
        .build()
    )

    # ── ADLS Gen2 / Microsoft Fabric ──────────────────────────────────────
    conn = (
        IcebergConnectionBuilder()
        .adls(account_name="myfabricaccount", account_key="<key>")
        .fabric_lakehouse(workspace_id="<ws-guid>", lakehouse_id="<lh-guid>")
        .nessie("http://nessie:19120")
        .oauth("<oauth-url>", client_id="sync-service", client_secret="<secret>")
        .archive("abfss://archive@myfabricaccount.dfs.core.windows.net/iceberg-cold")
        .build()
    )

    # ── Common API regardless of backend ─────────────────────────────────
    catalog  = conn.get_catalog()
    archive_cfg = conn.make_archive_config("tpch/orders", older_than="150d",
                                           min_snapshots_to_keep=5)
    restore_cfg = conn.make_restore_config("tpch/orders",
                                           partitions=[{"order_month": "2024-03"}],
                                           as_of="2024-04-01")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────────────
# IcebergConnection  — the product of the builder
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IcebergConnection:
    """
    Holds all resolved connection parameters for a single backend.
    Exposes factory methods that produce PyIceberg catalogs and
    iceberg_sync archive/restore configs.
    """

    backend: str                    # "s3" | "adls"
    warehouse_root: str             # primary Iceberg warehouse root URI
    archive_root: str               # cold storage root URI

    # Storage credentials — source backend
    _s3_kwargs:   Dict[str, Any] = field(default_factory=dict, repr=False)
    _adls_kwargs: Dict[str, Any] = field(default_factory=dict, repr=False)

    # Archive-specific credentials (overrides source backend when set)
    _archive_adls_kwargs: Dict[str, Any] = field(default_factory=dict, repr=False)

    # Catalog / auth
    _gateway_url:       Optional[str] = field(default=None,  repr=False)
    _nessie_uri:        Optional[str] = field(default=None,  repr=False)
    _nessie_ref:        str           = field(default="main", repr=False)
    _oauth_url:         Optional[str] = field(default=None,  repr=False)
    _oauth_client_id:   Optional[str] = field(default=None,  repr=False)
    _oauth_secret:      Optional[str] = field(default=None,  repr=False)
    _oauth_scope:       str           = field(default="catalog:read catalog:write", repr=False)

    # ── PyIceberg catalog ─────────────────────────────────────────────────────

    @property
    def pyiceberg_kwargs(self) -> Dict[str, Any]:
        """
        Keyword arguments for PyIceberg's load_catalog() — REST catalog via
        catalog-gateway when gateway_url is set, SQLite-backed local catalog
        otherwise (useful for ADLS without a REST gateway).
        """
        if self._gateway_url:
            kwargs: Dict[str, Any] = {
                "type": "rest",
                "uri": self._gateway_url,
            }
            if self._oauth_url and self._oauth_client_id and self._oauth_secret:
                kwargs["oauth2-server-uri"] = self._oauth_url
                kwargs["credential"] = f"{self._oauth_client_id}:{self._oauth_secret}"
                kwargs["scope"] = self._oauth_scope

            # Inject storage properties
            if self.backend == "s3":
                kwargs.update({
                    f"s3.{k}": v
                    for k, v in {
                        "endpoint":         self._s3_kwargs.get("endpoint"),
                        "access-key-id":    self._s3_kwargs.get("access_key"),
                        "secret-access-key":self._s3_kwargs.get("secret_key"),
                        "region":           self._s3_kwargs.get("region", "us-east-1"),
                        "path-style-access":"true",
                    }.items()
                    if v is not None
                })
            elif self.backend == "adls":
                if self._adls_kwargs.get("account_key"):
                    kwargs["adls.account-name"] = self._adls_kwargs["account_name"]
                    kwargs["adls.account-key"]  = self._adls_kwargs["account_key"]
                elif self._adls_kwargs.get("use_default_credential"):
                    kwargs["adls.account-name"] = self._adls_kwargs["account_name"]
                    # DefaultAzureCredential — no key needed

            return kwargs

        # No gateway → SQLite-backed local catalog (ADLS direct write)
        kwargs = {"uri": "sqlite:///e2e/fabric_catalog.db"}
        if self.backend == "adls":
            kwargs["adls.account-name"] = self._adls_kwargs.get("account_name", "")
            if self._adls_kwargs.get("account_key"):
                kwargs["adls.account-key"] = self._adls_kwargs["account_key"]
            if self._adls_kwargs.get("tenant_id"):
                kwargs["adls.tenant-id"]    = self._adls_kwargs["tenant_id"]
                kwargs["adls.client-id"]    = self._adls_kwargs.get("client_id", "")
                kwargs["adls.client-secret"]= self._adls_kwargs.get("client_secret", "")
        return kwargs

    def get_catalog(self, name: str = "default"):
        """Return a configured PyIceberg catalog instance."""
        if self._gateway_url:
            from pyiceberg.catalog import load_catalog
            return load_catalog(name, **self.pyiceberg_kwargs)
        # ADLS without gateway: use SqlCatalog backed by SQLite
        from pyiceberg.catalog.sql import SqlCatalog
        return SqlCatalog(name, **self.pyiceberg_kwargs)

    # ── iceberg_sync archive/restore configs ──────────────────────────────────

    def _source_storage_config(self):
        """Return (S3Config | None, ADLSConfig | None) for the source/warehouse."""
        from iceberg_sync.archive.config import S3Config, ADLSConfig
        if self.backend == "s3":
            return (
                S3Config(
                    endpoint=self._s3_kwargs.get("endpoint"),
                    access_key=self._s3_kwargs.get("access_key"),
                    secret_key=self._s3_kwargs.get("secret_key"),
                    region=self._s3_kwargs.get("region", "us-east-1"),
                ),
                None,
            )
        return (
            None,
            ADLSConfig(
                account_name=self._adls_kwargs.get("account_name", ""),
                account_key=self._adls_kwargs.get("account_key"),
            ),
        )

    def _archive_storage_config(self):
        """Return (S3Config | None, ADLSConfig | None) for the archive storage.
        Falls back to _source_storage_config() when no archive-specific backend is set."""
        if self._archive_adls_kwargs:
            from iceberg_sync.archive.config import ADLSConfig
            return (
                None,
                ADLSConfig(
                    account_name=self._archive_adls_kwargs.get("account_name", ""),
                    account_key=self._archive_adls_kwargs.get("account_key"),
                ),
            )
        return self._source_storage_config()

    def _catalog_config(self):
        from iceberg_sync.archive.config import CatalogConfig
        return CatalogConfig(
            nessie_uri=self._nessie_uri or "",
            nessie_ref=self._nessie_ref,
            catalog_rest_uri=self._gateway_url or "",
            oauth_url=self._oauth_url or "",
            oauth_client_id=self._oauth_client_id or "",
            oauth_client_secret=self._oauth_secret or "",
        )

    def make_archive_config(
        self,
        table: str,
        *,
        older_than: str = "30d",
        min_snapshots_to_keep: int = 1,
        delete_after_archive: bool = True,
        parallelism: int = 8,
    ):
        """Return an ArchiveJobConfig ready for IcebergArchiver.from_config()."""
        from iceberg_sync.archive.config import ArchiveJobConfig, TransferConfig
        source_s3, source_adls = self._source_storage_config()
        archive_s3, archive_adls = self._archive_storage_config()
        kwargs = dict(
            source_root=self.warehouse_root,
            table=table,
            archive_root=self.archive_root,
            older_than=older_than,
            min_snapshots_to_keep=min_snapshots_to_keep,
            delete_after_archive=delete_after_archive,
            catalog=self._catalog_config(),
            transfer=TransferConfig(parallelism=parallelism),
        )
        if source_s3:
            kwargs["source_s3"] = source_s3
        if source_adls:
            kwargs["source_adls"] = source_adls
        if archive_s3:
            kwargs["archive_s3"] = archive_s3
        if archive_adls:
            kwargs["archive_adls"] = archive_adls
        return ArchiveJobConfig(**kwargs)

    def make_restore_config(
        self,
        table: str,
        *,
        partitions: List[Dict[str, Any]],
        as_of: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        mode: str = "replace",
        conflict_strategy: str = "skip",
        restore_as: Optional[str] = None,
        parallelism: int = 8,
    ):
        """Return a RestoreJobConfig ready for IcebergRestorer.from_config()."""
        from iceberg_sync.archive.config import RestoreJobConfig, TransferConfig
        archive_s3, archive_adls = self._archive_storage_config()
        target_s3, target_adls = self._source_storage_config()
        kwargs = dict(
            archive_root=self.archive_root,
            table=table,
            target_root=self.warehouse_root,
            as_of=as_of,
            snapshot_id=snapshot_id,
            partitions=partitions,
            mode=mode,
            conflict_strategy=conflict_strategy,
            restore_as=restore_as,
            catalog=self._catalog_config(),
            transfer=TransferConfig(parallelism=parallelism),
        )
        if archive_s3:
            kwargs["archive_s3"] = archive_s3
        if archive_adls:
            kwargs["archive_adls"] = archive_adls
        if target_s3:
            kwargs["target_s3"] = target_s3
        if target_adls:
            kwargs["target_adls"] = target_adls
        return RestoreJobConfig(**kwargs)

    def __repr__(self) -> str:
        return (
            f"IcebergConnection(backend={self.backend!r}, "
            f"warehouse={self.warehouse_root!r}, "
            f"archive={self.archive_root!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# IcebergConnectionBuilder  — fluent DSL
# ─────────────────────────────────────────────────────────────────────────────

class IcebergConnectionBuilder:
    """
    Fluent builder for IcebergConnection.

    Call storage methods (.s3 / .adls) first, then catalog methods
    (.catalog_gateway / .nessie / .oauth), then .warehouse / .archive,
    and finally .build().

    Every method returns self so calls can be chained.
    """

    def __init__(self) -> None:
        self._backend:             Optional[str] = None
        self._s3_kwargs:           Dict[str, Any] = {}
        self._adls_kwargs:         Dict[str, Any] = {}
        self._archive_adls_kwargs: Dict[str, Any] = {}
        self._warehouse_root:      Optional[str] = None
        self._archive_root:        Optional[str] = None
        self._gateway_url:         Optional[str] = None
        self._nessie_uri:          Optional[str] = None
        self._nessie_ref:          str = "main"
        self._oauth_url:           Optional[str] = None
        self._oauth_client_id:     Optional[str] = None
        self._oauth_secret:        Optional[str] = None
        self._oauth_scope:         str = "catalog:read catalog:write"

    # ── Storage backend methods ───────────────────────────────────────────────

    def s3(
        self,
        *,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
    ) -> "IcebergConnectionBuilder":
        """
        Configure S3-compatible storage (AWS S3, MinIO, etc.).

        Example (MinIO):
            .s3(endpoint="http://localhost:9000",
                access_key="minioadmin",
                secret_key="minioadmin")

        Example (AWS S3 — credentials from env / IAM role):
            .s3(region="us-east-1")
        """
        self._backend = "s3"
        self._s3_kwargs = {
            "endpoint":   endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "region":     region,
        }
        return self

    def adls(
        self,
        *,
        account_name: str,
        account_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        use_default_credential: bool = False,
    ) -> "IcebergConnectionBuilder":
        """
        Configure Azure Data Lake Storage Gen2.

        Three auth modes (supply exactly one):
          account_key         — storage account key (simplest for dev)
          tenant_id + client_id + client_secret — service principal (prod)
          use_default_credential=True — managed identity / az login / env SP

        Example (account key):
            .adls(account_name="myfabricaccount", account_key="<key>")

        Example (service principal):
            .adls(account_name="prod",
                  tenant_id="<tenant>",
                  client_id="<app-id>",
                  client_secret="<secret>")

        Example (managed identity):
            .adls(account_name="prod", use_default_credential=True)
        """
        self._backend = "adls"
        self._adls_kwargs = {
            "account_name":          account_name,
            "account_key":           account_key,
            "tenant_id":             tenant_id,
            "client_id":             client_id,
            "client_secret":         client_secret,
            "use_default_credential":use_default_credential,
        }
        return self

    # ── Fabric Lakehouse shortcut ─────────────────────────────────────────────

    def fabric_lakehouse(
        self,
        *,
        workspace_id: str,
        lakehouse_id: str,
        account_name: Optional[str] = None,
    ) -> "IcebergConnectionBuilder":
        """
        Derive the OneLake warehouse_root and set the backend to ADLS.

        Builds:
            abfss://fabric@<account>.dfs.core.windows.net
            /workspaces/<workspace>/lakehouses/<lakehouse>/Tables

        Call .adls() before or after .fabric_lakehouse() — the account_name
        is taken from the .adls() call when account_name is omitted here.

        Example:
            .adls(account_name="myfabricaccount", account_key="<key>")
            .fabric_lakehouse(workspace_id="<ws-guid>", lakehouse_id="<lh-guid>")
        """
        acct = account_name or self._adls_kwargs.get("account_name", "")
        if not acct:
            raise ValueError(
                "fabric_lakehouse() requires account_name — "
                "call .adls(account_name=...) first or pass account_name here."
            )
        self._warehouse_root = (
            f"abfss://fabric@{acct}.dfs.core.windows.net"
            f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/Tables"
        )
        if self._backend is None:
            self._backend = "adls"
        return self

    # ── Catalog methods ───────────────────────────────────────────────────────

    def catalog_gateway(self, url: str) -> "IcebergConnectionBuilder":
        """
        Set the Iceberg REST catalog endpoint (catalog-gateway or any
        Iceberg REST server).

        Example:
            .catalog_gateway("http://localhost:8083")
        """
        self._gateway_url = url.rstrip("/")
        return self

    def nessie(
        self,
        uri: str,
        ref: str = "main",
    ) -> "IcebergConnectionBuilder":
        """
        Set the Nessie catalog URI for metadata pointer commits.
        Used by IcebergArchiver / IcebergRestorer — not by PyIceberg's
        load_catalog() when catalog_gateway is also set.

        Example:
            .nessie("http://localhost:19120")
        """
        self._nessie_uri = uri
        self._nessie_ref = ref
        return self

    def oauth(
        self,
        url: str,
        *,
        client_id: str,
        client_secret: str,
        scope: str = "catalog:read catalog:write",
    ) -> "IcebergConnectionBuilder":
        """
        Configure OAuth 2.0 client credentials.
        Used for both catalog-gateway auth (PyIceberg) and Nessie auth
        (IcebergArchiver / IcebergRestorer).

        Example:
            .oauth("http://localhost:8081/token",
                   client_id="sync-service",
                   client_secret="sync-secret")
        """
        self._oauth_url       = url
        self._oauth_client_id = client_id
        self._oauth_secret    = client_secret
        self._oauth_scope     = scope
        return self

    # ── Storage root methods ──────────────────────────────────────────────────

    def warehouse(self, root_uri: str) -> "IcebergConnectionBuilder":
        """
        Set the primary Iceberg warehouse root URI.
        (Not needed after .fabric_lakehouse() — that sets it automatically.)

        Example:
            .warehouse("s3a://warehouse/iceberg")
        """
        self._warehouse_root = root_uri.strip("\"'").rstrip("/")
        return self

    def archive(self, root_uri: str) -> "IcebergConnectionBuilder":
        """
        Set the cold storage root URI for archived snapshots.

        Example (MinIO):
            .archive("s3a://warehouse/archive")

        Example (ADLS):
            .archive("abfss://archive@account.dfs.core.windows.net/iceberg-cold")
        """
        self._archive_root = root_uri.strip("\"'").rstrip("/")
        return self

    def archive_adls(
        self,
        *,
        account_name: str,
        account_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        use_default_credential: bool = False,
    ) -> "IcebergConnectionBuilder":
        """
        Use a different ADLS account for cold storage (split-backend).
        Call this when the source is S3/MinIO but the archive target is Azure.

        Example:
            .s3(endpoint="http://localhost:9000", ...)
            .archive_adls(account_name="sticebergpipeline", account_key="<key>")
            .archive("abfss://archive@sticebergpipeline.dfs.core.windows.net/iceberg-cold")
        """
        self._archive_adls_kwargs = {
            "account_name":           account_name,
            "account_key":            account_key,
            "tenant_id":              tenant_id,
            "client_id":              client_id,
            "client_secret":          client_secret,
            "use_default_credential": use_default_credential,
        }
        return self

    # ── Terminal method ───────────────────────────────────────────────────────

    def build(self) -> IcebergConnection:
        """
        Validate and return the configured IcebergConnection.

        Raises ValueError for missing required fields.
        """
        if self._backend is None:
            raise ValueError(
                "No storage backend configured. "
                "Call .s3(...) or .adls(...) before .build()."
            )
        if not self._warehouse_root:
            raise ValueError(
                "No warehouse root set. "
                "Call .warehouse('s3a://...') or .fabric_lakehouse(...) before .build()."
            )
        if not self._archive_root:
            raise ValueError(
                "No archive root set. "
                "Call .archive('s3a://...') before .build()."
            )
        return IcebergConnection(
            backend=self._backend,
            warehouse_root=self._warehouse_root,
            archive_root=self._archive_root,
            _s3_kwargs=dict(self._s3_kwargs),
            _adls_kwargs=dict(self._adls_kwargs),
            _archive_adls_kwargs=dict(self._archive_adls_kwargs),
            _gateway_url=self._gateway_url,
            _nessie_uri=self._nessie_uri,
            _nessie_ref=self._nessie_ref,
            _oauth_url=self._oauth_url,
            _oauth_client_id=self._oauth_client_id,
            _oauth_secret=self._oauth_secret,
            _oauth_scope=self._oauth_scope,
        )

    # ── Class-level convenience constructors ──────────────────────────────────

    @classmethod
    def from_env_s3(cls) -> "IcebergConnectionBuilder":
        """
        Build an S3 connection from environment variables.

        Required:
          MINIO_ENDPOINT or AWS_ENDPOINT_URL   (optional — omit for AWS S3)
          MINIO_ACCESS_KEY or AWS_ACCESS_KEY_ID
          MINIO_SECRET_KEY or AWS_SECRET_ACCESS_KEY
          GATEWAY_URL
          OAUTH_URL, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET
          WAREHOUSE_ROOT   (default: s3a://warehouse/iceberg)
          ARCHIVE_ROOT     (default: s3a://warehouse/archive)
        """
        import os
        return (
            cls()
            .s3(
                endpoint=  os.getenv("MINIO_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL"),
                access_key=os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID"),
                secret_key=os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"),
                region=    os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            )
            .catalog_gateway(os.getenv("GATEWAY_URL", "http://localhost:8083"))
            .nessie(
                os.getenv("NESSIE_URI", "http://localhost:19120"),
                ref=os.getenv("NESSIE_REF", "main"),
            )
            .oauth(
                os.getenv("OAUTH_URL", "http://localhost:8081/token"),
                client_id=    os.getenv("OAUTH_CLIENT_ID", "admin-client"),
                client_secret=os.getenv("OAUTH_CLIENT_SECRET", "admin-secret"),
            )
            .warehouse(os.getenv("WAREHOUSE_ROOT", "s3a://warehouse/iceberg"))
            .archive(  os.getenv("ARCHIVE_ROOT",   "s3a://warehouse/archive"))
        )

    @classmethod
    def from_env_adls(cls) -> "IcebergConnectionBuilder":
        """
        Build an ADLS / Fabric connection from environment variables.

        Required:
          AZURE_STORAGE_ACCOUNT
          AZURE_STORAGE_KEY        (or set AZURE_TENANT_ID + AZURE_CLIENT_ID +
                                    AZURE_CLIENT_SECRET for service principal)
          FABRIC_WORKSPACE_ID
          FABRIC_LAKEHOUSE_ID
          ARCHIVE_ROOT             cold container URI
          NESSIE_URI, OAUTH_URL, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET  (optional)
        """
        import os
        builder = (
            cls()
            .adls(
                account_name=          os.environ["AZURE_STORAGE_ACCOUNT"],
                account_key=           os.getenv("AZURE_STORAGE_KEY"),
                tenant_id=             os.getenv("AZURE_TENANT_ID"),
                client_id=             os.getenv("AZURE_CLIENT_ID"),
                client_secret=         os.getenv("AZURE_CLIENT_SECRET"),
                use_default_credential=not os.getenv("AZURE_STORAGE_KEY")
                                       and not os.getenv("AZURE_CLIENT_SECRET"),
            )
            .fabric_lakehouse(
                workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
                lakehouse_id=os.environ["FABRIC_LAKEHOUSE_ID"],
            )
            .archive(os.environ["ARCHIVE_ROOT"])
        )
        if os.getenv("GATEWAY_URL"):
            builder.catalog_gateway(os.environ["GATEWAY_URL"])
        if os.getenv("NESSIE_URI"):
            builder.nessie(os.environ["NESSIE_URI"],
                           ref=os.getenv("NESSIE_REF", "main"))
        if os.getenv("OAUTH_URL"):
            builder.oauth(
                os.environ["OAUTH_URL"],
                client_id=    os.getenv("OAUTH_CLIENT_ID", "sync-service"),
                client_secret=os.getenv("OAUTH_CLIENT_SECRET", ""),
            )
        return builder
