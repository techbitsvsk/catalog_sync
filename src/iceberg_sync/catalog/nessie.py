"""
catalog/nessie.py — Nessie REST Catalog integration for incremental table sync.

Uses the Nessie native v2 API (/api/v2) which is available in all Nessie
versions >= 0.50.  This avoids the Iceberg REST compatibility layer
(/iceberg/v1) which requires additional configuration in newer Nessie
images and is absent in older ones.

Nessie v2 API endpoints used
────────────────────────────
    GET  /api/v2/trees/{ref}                        — get branch + current hash
    GET  /api/v2/trees/{ref}/contents/{key...}      — read table entry
    POST /api/v2/trees/{ref}@{hash}/history/commit  — commit PUT / DELETE ops
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)


class NessieCatalog:
    """
    Thin client for the Nessie v2 native API.

    Registers and updates Iceberg table metadata pointers in Nessie after
    file-level sync has placed Parquet + metadata files on target storage.

    Args:
        uri:   Nessie base URL (e.g. http://localhost:19120)
        ref:   Nessie branch to commit to (default: "main")
        token: Bearer token for authenticated Nessie deployments

    Usage:
        nessie = NessieCatalog("http://localhost:19120")
        nessie.register_or_update(
            namespace="gold",
            table="top_customers",
            metadata_location="s3a://warehouse/gold/top_customers/metadata/v3.metadata.json",
        )
    """

    def __init__(
        self,
        uri: str,
        ref: str = "main",
        token: Optional[str] = None,
        **_kwargs,  # absorb unused kwargs (prefix, warehouse) from old callers
    ):
        self._base = uri.rstrip("/")
        self._ref = ref
        self._session = self._build_session(token)

    @staticmethod
    def _build_session(token: Optional[str]) -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 502, 503, 504])
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        if token:
            session.headers["Authorization"] = f"Bearer {token}"
        return session

    # ── Connectivity ──────────────────────────────────────────────────────────

    def ping(self) -> bool:
        """Return True if the Nessie server is reachable and healthy."""
        try:
            r = self._session.get(f"{self._base}/q/health/ready", timeout=5)
            return r.status_code == 200
        except requests.RequestException:
            return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_hash(self) -> str:
        """Return the current HEAD hash of self._ref."""
        r = self._session.get(f"{self._base}/api/v2/trees/{self._ref}", timeout=10)
        r.raise_for_status()
        return r.json()["reference"]["hash"]

    def _commit(self, operations: list, message: str) -> dict:
        """
        Post a commit to Nessie using optimistic concurrency control.
        The current hash is fetched immediately before the commit.
        """
        hash_ = self._get_hash()
        body = {
            "commitMeta": {"message": message},
            "operations": operations,
        }
        r = self._session.post(
            f"{self._base}/api/v2/trees/{self._ref}@{hash_}/history/commit",
            json=body,
            timeout=30,
        )
        if not r.ok:
            raise requests.HTTPError(
                f"{r.status_code} {r.reason} — {r.text}", response=r
            )
        return r.json()

    def _key_elements(self, namespace: str, table: Optional[str] = None) -> list:
        """Split dotted namespace and optional table into a key-elements list."""
        parts = namespace.split(".")
        if table:
            parts.append(table)
        return parts

    def _content_url(self, namespace: str, table: str) -> str:
        """URL for reading a single table entry via the v2 contents endpoint."""
        key = ".".join(self._key_elements(namespace, table))
        return f"{self._base}/api/v2/trees/{self._ref}/contents/{key}"

    # ── Namespaces ────────────────────────────────────────────────────────────

    def ensure_namespace(self, namespace: str) -> None:
        """Create namespace if it does not already exist."""
        elements = namespace.split(".")
        try:
            self._commit(
                operations=[{
                    "type": "PUT",
                    "key": {"elements": elements},
                    "content": {
                        "type": "NAMESPACE",
                        "elements": elements,
                        "properties": {},
                    },
                }],
                message=f"ensure namespace {namespace}",
            )
            log.info(f"Created namespace: {namespace}")
        except requests.HTTPError as e:
            # 409 = already exists; 400 with "no content ID" = already exists too
            log.debug(f"Namespace already exists (or ensure skipped): {namespace}")

    def list_namespaces(self) -> List[str]:
        """List top-level namespaces in the catalog."""
        r = self._session.get(f"{self._base}/api/v2/trees/{self._ref}/entries", timeout=15)
        r.raise_for_status()
        entries = r.json().get("entries", [])
        return [
            ".".join(e["name"]["elements"])
            for e in entries
            if e.get("type") == "NAMESPACE"
        ]

    # ── Tables ────────────────────────────────────────────────────────────────

    def table_exists(self, namespace: str, table: str) -> bool:
        """Return True if the table is registered in Nessie."""
        r = self._session.get(self._content_url(namespace, table), timeout=15)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        r.raise_for_status()
        return False

    def get_metadata_location(self, namespace: str, table: str) -> Optional[str]:
        """Return the current metadata-location registered in Nessie, or None."""
        r = self._session.get(self._content_url(namespace, table), timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("content", {}).get("metadataLocation")

    def register_table(self, namespace: str, table: str, metadata_location: str) -> Dict:
        """Register a new Iceberg table in Nessie, or update its metadata pointer."""
        self.ensure_namespace(namespace)

        # If the key already exists Nessie requires the existing content ID in
        # the PUT payload — otherwise it rejects the commit with 400.
        content: Dict = {
            "type": "ICEBERG_TABLE",
            "metadataLocation": metadata_location,
            "snapshotId": -1,
            "schemaId": -1,
            "specId": -1,
            "sortOrderId": -1,
        }
        existing = self.get_metadata_location(namespace, table)
        if existing is not None:
            # Re-read the full content object to get the Nessie-assigned ID
            r = self._session.get(self._content_url(namespace, table), timeout=15)
            if r.ok:
                existing_id = r.json().get("content", {}).get("id")
                if existing_id:
                    content["id"] = existing_id

        result = self._commit(
            operations=[{
                "type": "PUT",
                "key": {"elements": self._key_elements(namespace, table)},
                "content": content,
            }],
            message=f"register {namespace}.{table}",
        )
        log.info(f"Registered {namespace}.{table} → {metadata_location}")
        return result

    def drop_table(self, namespace: str, table: str) -> None:
        """Remove a table's catalog entry from Nessie (data files are NOT deleted)."""
        try:
            self._commit(
                operations=[{
                    "type": "DELETE",
                    "key": {"elements": self._key_elements(namespace, table)},
                }],
                message=f"drop {namespace}.{table}",
            )
            log.info(f"Dropped catalog entry: {namespace}.{table}")
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.debug(f"Table not found during drop (already absent): {namespace}.{table}")
            else:
                raise

    def update_table(self, namespace: str, table: str, new_metadata_location: str) -> Dict:
        """Point an existing Nessie table at a new metadata.json."""
        log.info(f"Updating {namespace}.{table} → {new_metadata_location}")
        previous_location = self.get_metadata_location(namespace, table)

        self.drop_table(namespace, table)

        try:
            return self.register_table(namespace, table, new_metadata_location)
        except Exception as register_exc:
            log.error(f"Re-register failed for {namespace}.{table}: {register_exc}.")
            if previous_location:
                try:
                    self.register_table(namespace, table, previous_location)
                    log.warning(f"Restored {namespace}.{table} to {previous_location}")
                except Exception as restore_exc:
                    log.error(
                        f"CRITICAL: could not restore {namespace}.{table}. "
                        f"Manual recovery: commit PUT ICEBERG_TABLE with "
                        f"metadataLocation={previous_location}"
                    )
            raise register_exc

    def register_or_update(self, namespace: str, table: str, metadata_location: str) -> Dict:
        """Register a new table or update the metadata pointer for an existing one."""
        current = self.get_metadata_location(namespace, table)
        if current == metadata_location:
            log.info(f"{namespace}.{table} already at {metadata_location} — skipping")
            return {"metadata-location": metadata_location, "skipped": True}
        return self.register_table(namespace, table, metadata_location)

    def list_tables(self, namespace: str) -> List[str]:
        """List all table names in a namespace."""
        r = self._session.get(f"{self._base}/api/v2/trees/{self._ref}/entries", timeout=15)
        r.raise_for_status()
        ns_elements = namespace.split(".")
        return [
            e["name"]["elements"][-1]
            for e in r.json().get("entries", [])
            if e.get("type") == "ICEBERG_TABLE"
            and e["name"]["elements"][:-1] == ns_elements
        ]
