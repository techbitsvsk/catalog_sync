"""
path_translator.py — Bidirectional URI translation between cloud storage schemes.

This is the heart of the sync solution.  Iceberg metadata contains absolute URIs
like s3://bucket/iceberg/gold/data/00001.parquet.  When that metadata is replicated
to Azure, every such URI must become
abfss://container@account.dfs.core.windows.net/iceberg/gold/data/00001.parquet.

The PathTranslator holds a mapping of root prefixes and translates any absolute URI
from one scheme to another while preserving the relative path below the root.

Design decisions
────────────────
•  Translation is pure string manipulation — no I/O, no network calls.
•  Multiple root mappings are supported (e.g. raw bucket → raw container,
   warehouse bucket → warehouse container).
•  Reversibility: the same translator can translate in both directions.
•  Unknown prefixes raise immediately — fail fast, never silently pass through
   an untranslated URI into metadata.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class StorageRoot:
    """
    A storage root is the prefix that differs between clouds.

    Examples:
        s3://iceberg-warehouse/iceberg/
        abfss://iceberg@storageaccount.dfs.core.windows.net/iceberg/
        gs://iceberg-warehouse/iceberg/
        s3a://iceberg-warehouse/iceberg/   (MinIO)
    """
    uri: str

    def __post_init__(self):
        # Normalise: ensure trailing slash for clean prefix matching
        if not self.uri.endswith("/"):
            object.__setattr__(self, "uri", self.uri + "/")

    @property
    def scheme(self) -> str:
        return self.uri.split("://")[0]


@dataclass
class PathTranslator:
    """
    Translates absolute storage URIs between source and target clouds.

    Usage:
        translator = PathTranslator(
            mappings=[
                ("s3://iceberg-warehouse/iceberg/",
                 "abfss://iceberg@storageaccount.dfs.core.windows.net/iceberg/"),
                ("s3://raw-data/",
                 "abfss://raw@storageaccount.dfs.core.windows.net/"),
            ]
        )

        translated = translator.translate(
            "s3://iceberg-warehouse/iceberg/gold/top_customers/data/00001.parquet"
        )
        # → "abfss://iceberg@storageaccount.dfs.core.windows.net/iceberg/gold/top_customers/data/00001.parquet"
    """

    # List of (source_root, target_root) tuples
    _forward_map: List[Tuple[StorageRoot, StorageRoot]] = field(default_factory=list)

    def __init__(self, mappings: List[Tuple[str, str]]):
        """
        Args:
            mappings: List of (source_root_uri, target_root_uri) pairs.
                      Order matters — first match wins.  Put more specific
                      prefixes before broader ones.
        """
        self._forward_map = [
            (StorageRoot(src), StorageRoot(tgt))
            for src, tgt in mappings
        ]

    def translate(self, uri: str, *, strict: bool = True) -> str:
        """
        Translate a single absolute URI from source scheme to target scheme.

        Args:
            uri:    The absolute URI to translate.
            strict: If True, raise ValueError for URIs that don't match any
                    mapping.  If False, return the URI unchanged (useful for
                    non-path fields in metadata).
        """
        for source_root, target_root in self._forward_map:
            if uri.startswith(source_root.uri):
                relative = uri[len(source_root.uri):]
                return target_root.uri + relative

        if strict:
            known = [src.uri for src, _ in self._forward_map]
            raise ValueError(
                f"URI '{uri}' does not match any known source root.\n"
                f"Known roots: {known}\n"
                f"Ensure your path mappings cover all storage prefixes used "
                f"by this Iceberg table."
            )
        return uri

    def translate_many(self, uris: List[str], *, strict: bool = True) -> List[str]:
        """Translate a list of URIs."""
        return [self.translate(u, strict=strict) for u in uris]

    def reverse(self) -> "PathTranslator":
        """
        Return a new translator that maps in the opposite direction.
        Useful for failback (Azure → AWS).
        """
        reversed_mappings = [
            (tgt.uri, src.uri) for src, tgt in self._forward_map
        ]
        return PathTranslator(reversed_mappings)

    def source_roots(self) -> List[str]:
        return [src.uri for src, _ in self._forward_map]

    def target_roots(self) -> List[str]:
        return [tgt.uri for _, tgt in self._forward_map]

    def relative_path(self, uri: str) -> str:
        """
        Extract the relative path below the matching source root.
        Used for data file copy operations.
        """
        for source_root, _ in self._forward_map:
            if uri.startswith(source_root.uri):
                return uri[len(source_root.uri):]
        raise ValueError(f"URI '{uri}' does not match any source root.")

    def target_uri(self, relative: str) -> str:
        """
        Construct a target URI from a relative path.
        Uses the first mapping's target root.
        """
        if not self._forward_map:
            raise ValueError("No mappings configured.")
        return self._forward_map[0][1].uri + relative


# ── Factory for common patterns ──────────────────────────────────────────────

def aws_to_azure(
    s3_warehouse: str,
    adls_warehouse: str,
    additional_mappings: Optional[List[Tuple[str, str]]] = None,
) -> PathTranslator:
    """Convenience factory for AWS S3 → Azure ADLS Gen2."""
    mappings = [(s3_warehouse, adls_warehouse)]
    if additional_mappings:
        mappings.extend(additional_mappings)
    return PathTranslator(mappings)


def aws_to_gcs(
    s3_warehouse: str,
    gcs_warehouse: str,
    additional_mappings: Optional[List[Tuple[str, str]]] = None,
) -> PathTranslator:
    """Convenience factory for AWS S3 → Google Cloud Storage."""
    mappings = [(s3_warehouse, gcs_warehouse)]
    if additional_mappings:
        mappings.extend(additional_mappings)
    return PathTranslator(mappings)


def aws_to_minio(
    s3_warehouse: str,
    minio_warehouse: str,
    additional_mappings: Optional[List[Tuple[str, str]]] = None,
) -> PathTranslator:
    """Convenience factory for AWS S3 → MinIO (s3a://)."""
    mappings = [(s3_warehouse, minio_warehouse)]
    if additional_mappings:
        mappings.extend(additional_mappings)
    return PathTranslator(mappings)
