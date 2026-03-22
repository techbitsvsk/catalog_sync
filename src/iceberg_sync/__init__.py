"""
iceberg-catalog-sync — Platform-agnostic Iceberg table replication across clouds.

Solves the fundamental problem: Iceberg metadata contains absolute storage URIs.
Copying files from S3 to ADLS without rewriting metadata leaves the target catalog
pointing back at the source. This library handles the full metadata chain:

    metadata.json → manifest-list (Avro) → manifest (Avro) → data files (Parquet)

Every absolute path at every level is translated from source to target scheme.
"""

__version__ = "0.1.0"
