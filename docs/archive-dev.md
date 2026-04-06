# Archive Module — Developer Guide

Design rationale, class diagrams, data flows, and extension points for the
`iceberg_sync.archive` module.

---

## Design Goals

| Goal | Decision |
|------|----------|
| **Reuse existing infrastructure** | `StorageBackend`, `create_storage()`, `NessieCatalog`, `OAuthClient` — zero new cloud SDK calls. |
| **Manifest-based accuracy** | File lists are derived from Avro manifests, not directory listings.  Handles custom `write.data.path` and Iceberg v2 delete files. |
| **Plan before execute** | `RestorePlan` is built once (dry-run), printed for human review, then passed verbatim to `execute()`. What you see is what runs. |
| **Fail-safe writes** | Data files are copied before metadata is rewritten.  A copy failure aborts before touching metadata — the primary table stays valid. |
| **No JVM / PyIceberg dependency** | `fastavro` for Avro I/O, `orjson` for JSON, consistent with the rest of the project. |
| **Config-first** | YAML config with `${ENV_VAR}` substitution for scheduling.  CLI flags override config for ad-hoc use. |

---

## Module Structure

```
src/iceberg_sync/
├── archive/
│   ├── __init__.py            Exports IcebergArchiver, IcebergRestorer
│   ├── config.py              Dataclasses + YAML loader (ArchiveJobConfig, RestoreJobConfig)
│   ├── archive_index.py       ArchiveIndex — .archive-manifest.json read/write
│   ├── snapshot_manager.py    Retention policy: which snapshots to archive/keep
│   ├── partition_scanner.py   Avro manifest walker + partition filter
│   ├── restore_planner.py     RestorePlan builder — dry-run, conflict detection
│   ├── archiver.py            IcebergArchiver orchestrator
│   ├── restorer.py            IcebergRestorer orchestrator (plan → execute)
│   └── metadata_editor.py     metadata.json reconstruction for restore + expiry
└── archive_cli.py             Click CLI: iceberg-archive entry point
```

---

## Class Diagram

```mermaid
classDiagram

    class IcebergArchiver {
        +archive_table(table, dry_run) ArchiveResult
        +archive_namespace(namespace, dry_run) List~ArchiveResult~
        +from_config(cfg) IcebergArchiver
        -_source StorageBackend
        -_archive StorageBackend
        -_archive_snapshot(metadata, table, snapshot_id, dry_run)
        -_commit_metadata(table, root, uri)
    }

    class IcebergRestorer {
        +list_snapshots() List~ArchivedSnapshotEntry~
        +plan() RestorePlan
        +execute(plan) RestoreResult
        +from_config(cfg) IcebergRestorer
        -_archive StorageBackend
        -_target StorageBackend
        -_load_archived_metadata(table, snapshot_id) dict
        -_load_target_metadata(table) dict
        -_make_path_rewriter(source_root) Callable
        -_copy_manifests(archived_metadata, snapshot_id, archive_root, target_table_root, path_rewriter)
        -_commit_metadata(table, root, uri)
    }

    class ArchiveIndex {
        +source_root str
        +table str
        +archive_root str
        +snapshots List~ArchivedSnapshotEntry~
        +save(storage)
        +load(storage, archive_root, table) ArchiveIndex
        +load_or_new(...) ArchiveIndex
        +find_snapshot(snapshot_id, as_of_iso) ArchivedSnapshotEntry
        +snapshot_ids() List~int~
    }

    class ArchivedSnapshotEntry {
        +snapshot_id int
        +timestamp_ms int
        +operation str
        +added_data_files int
        +data_files_count int
        +size_bytes int
        +timestamp_iso str
    }

    class RestorePlan {
        +table str
        +snapshot_id int
        +files_to_copy List~ScannedFile~
        +partition_summary List~PartitionRestoreInfo~
        +conflicts List~PartitionRestoreInfo~
        +total_files int
        +total_bytes int
        +has_conflicts bool
        +print()
    }

    class RestorePlanner {
        +build_plan(...) RestorePlan
        -_archive StorageBackend
        -_target StorageBackend
    }

    class SnapshotDecision {
        +snapshot_id int
        +keep bool
        +reason str
    }

    class ScannedFile {
        +file_path str
        +partition Dict
        +file_size_bytes int
        +content int
    }

    IcebergArchiver --> ArchiveIndex : writes
    IcebergArchiver --> SnapshotDecision : uses decide_snapshots()
    IcebergArchiver --> ScannedFile : uses scan_snapshot()
    IcebergRestorer --> ArchiveIndex : reads
    IcebergRestorer --> RestorePlanner : creates plan
    IcebergRestorer --> RestorePlan : executes
    RestorePlanner --> RestorePlan : builds
    RestorePlanner --> ScannedFile : from partition_scanner
    ArchiveIndex --> ArchivedSnapshotEntry : contains
    RestorePlan --> ScannedFile : contains
```

---

## Archive Data Flow

```mermaid
sequenceDiagram
    participant A  as IcebergArchiver
    participant SM as snapshot_manager
    participant PS as partition_scanner
    participant SS as Source Storage
    participant CS as Archive Storage
    participant ME as metadata_editor
    participant AI as ArchiveIndex

    A->>SS: read version-hint.text
    A->>SS: read metadata.json (vN)
    A->>SM: decide_snapshots(metadata, older_than, min_keep)
    SM-->>A: List[SnapshotDecision]

    loop For each snapshot to archive
        A->>PS: scan_snapshot(source, metadata, snapshot_id, [])
        PS->>SS: read manifest-list Avro
        PS->>SS: read each manifest Avro
        PS-->>A: List[ScannedFile]

        par Parallel file copy
            A->>CS: copy_from(source, data_file_1, archive_uri_1)
            A->>CS: copy_from(source, data_file_2, archive_uri_2)
            A->>CS: copy_from(source, manifest_list, archive_uri)
            A->>CS: copy_from(source, manifests, archive_uris)
        end

        A->>AI: append ArchivedSnapshotEntry
    end

    A->>CS: ArchiveIndex.save() → .archive-manifest.json
    A->>ME: expire_snapshots_in_metadata(metadata, expired_ids)
    ME-->>A: new_meta_uri (vN+1)
    A->>SS: write new metadata.json
    A->>SS: write version-hint.text (or update Nessie)
```

---

## Restore Data Flow

```mermaid
sequenceDiagram
    participant R  as IcebergRestorer
    participant AI as ArchiveIndex
    participant PS as partition_scanner
    participant RP as RestorePlanner
    participant CS as Archive Storage
    participant TS as Target Storage
    participant ME as metadata_editor

    Note over R,ME: plan() — dry-run, no writes

    R->>AI: ArchiveIndex.load(archive, table)
    AI-->>R: ArchiveIndex (source_root, snapshots)
    R->>AI: find_snapshot(as_of / snapshot_id)
    AI-->>R: ArchivedSnapshotEntry
    R->>R: _make_path_rewriter(index.source_root)
    Note right of R: translates s3a://bucket/... URIs<br/>to archive abfss://... equivalents
    R->>CS: _load_archived_metadata(table, snap_id)
    Note right of R: scans all *.metadata.json files,<br/>merges snapshots, falls back to<br/>snap-{id}-*.avro scan if needed
    R->>PS: scan_snapshot(archive, metadata, snap_id, partitions, path_rewriter)
    PS->>CS: read manifest-list Avro (archive URI)
    PS->>CS: read manifest Avro files (archive URI via path_rewriter)
    PS-->>R: List[ScannedFile] (filtered by partition)
    R->>RP: build_plan(files, conflicts, mode)
    RP->>TS: exists() checks for conflict detection
    RP-->>R: RestorePlan
    R-->>User: plan.print()

    Note over R,ME: execute(plan) — after user reviews plan

    R->>AI: ArchiveIndex.load (rebuild path_rewriter for execute)
    R->>R: _make_path_rewriter(index.source_root)

    loop Parallel copy
        R->>TS: copy_from(archive, data_file, target_uri)
    end

    R->>TS: copy manifest Avro files (path_rewriter → archive → target)

    alt mode = new_table OR full replace
        R->>ME: write_table_metadata(archived_meta, target_root)
    else mode = partial partition replace/append
        R->>TS: read live metadata.json
        R->>ME: splice_manifests(live_meta, restored_manifests, mode)
        Note right of ME: new manifest-list entry includes<br/>all Iceberg v2 fields (content,<br/>sequence_number, min_sequence_number)
    end

    ME-->>R: new_meta_uri
    R->>TS: write version-hint.text (or update Nessie)
```

---

## Partition Scanning Logic

```mermaid
flowchart TD
    A([scan_snapshot\nstorage · metadata · snap_id\npartitions · path_rewriter]) --> B[Find snapshot in metadata.json]
    B --> C[Read manifest-list Avro\nfrom archive storage]
    C --> D[For each manifest path\nin manifest-list]
    D --> RW{path_rewriter\nprovided?}
    RW -- Yes --> RW2[Rewrite manifest_path\nsource URI → archive URI]
    RW -- No --> E
    RW2 --> E[Read manifest Avro file\nfrom archive storage]
    E --> F[For each record in manifest]
    F --> G{status == DELETED?}
    G -- Yes --> SKIP[Skip]
    G -- No --> H{include_delete_files\n= False AND content != 0?}
    H -- Yes --> SKIP
    H -- No --> I{Partition matches\nany requested?}
    I -- "No\n(or no partitions requested)" --> SKIP
    I -- Yes / Full table --> N[Normalize partition values\nbytes → str]
    N --> J[Emit ScannedFile\nfile_path · partition\nfile_size · content]
    J --> K{More records?}
    K -- Yes --> F
    K -- No --> L{More manifests?}
    L -- Yes --> D
    L -- No --> Z([Return List of ScannedFile])

    style A fill:#dbeafe
    style Z fill:#dcfce7
    style SKIP fill:#f3f4f6
    style RW2 fill:#fef3c7
    style N fill:#fef3c7
```

### Partial key matching

A requested partition `{"year": 2025}` matches any file whose partition record
contains `year=2025`, regardless of other fields (e.g. `month`, `day`).

```python
def _normalize_partition_value(v):
    """Decode bytes partition values to str (Iceberg stores strings as binary in Avro)."""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8")
    return v

def _partition_matches(file_partition, requested):
    for key, value in requested.items():
        actual = _normalize_partition_value(file_partition.get(key))
        if actual != value:
            return False
    return True
```

> **Note:** Iceberg encodes string partition values as raw bytes in Avro.
> `_normalize_partition_value` decodes them to `str` before comparison so callers
> can always pass plain Python strings (e.g. `{"order_month": "2024-03"}`).

This means you can restore an entire year's worth of data across all months
by specifying `--partition year=2025` rather than listing every month.

---

## Metadata Reconstruction

```mermaid
flowchart LR
    subgraph archived["Archive Storage"]
        AM[archived metadata.json\nsnap-8922...]
        AML[manifest-list.avro]
        AMAN[manifest-A.avro\nyear=2025/month=11]
        AMAN2[manifest-B.avro\nyear=2025/month=10]
    end

    subgraph live["Primary Storage (live table)"]
        LM[live metadata.json\nv12 snap-502]
        LML[manifest-list.avro]
        LMAN[manifest-X.avro\nyear=2025/month=12]
        LMAN2[manifest-Y.avro\nyear=2024/...]
    end

    subgraph result["After Restore (replace mode)"]
        NM[new metadata.json\nv13 snap-503\noperation=overwrite]
        NML[new manifest-list.avro]
        LMAN3[manifest-X.avro\nyear=2025/month=12\nunchanged — reused]
        LMAN4[manifest-Y.avro\nyear=2024/...\nunchanged — reused]
        RMAN[manifest-A.avro\nyear=2025/month=11\nrestored]
        RMAN2[manifest-B.avro\nyear=2025/month=10\nrestored]
    end

    AML --> NML
    AMAN --> RMAN
    AMAN2 --> RMAN2
    LML --> NML
    LMAN --> LMAN3
    LMAN2 --> LMAN4
    NML --> NM

    style archived fill:#f3f4f6,stroke:#6b7280
    style live     fill:#dbeafe,stroke:#2563eb
    style result   fill:#dcfce7,stroke:#16a34a
```

Key points:
- Existing manifests for **unaffected partitions** are reused verbatim — no rewrite.
- Restored manifests are added to the new manifest-list alongside current ones.
- A new snapshot (`snap-503`) with `operation=overwrite` is appended.
- The old snapshot (`snap-502`) remains in history for time-travel.
- Internal Avro manifest paths are rewritten from source storage URIs to archive URIs
  via `_make_path_rewriter` before reading — the archive files themselves are not modified.
- The new manifest-list entry includes all Iceberg v2 required fields: `content`,
  `sequence_number`, `min_sequence_number`, and row-count statistics.

---

## Snapshot Retention Decision

```mermaid
flowchart TD
    A([decide_snapshots]) --> B[Sort all snapshots\nnewer-first]
    B --> C[For each snapshot]
    C --> D{Is current\nsnapshot?}
    D -- Yes --> KEEP1[keep — reason: current]
    D -- No --> E{Pinned by\nnamed ref / branch?}
    E -- Yes --> KEEP2[keep — reason: named_ref]
    E -- No --> F{kept_count <\nmin_snapshots_to_keep?}
    F -- Yes --> KEEP3[keep — reason: min_keep\nkept_count++]
    F -- No --> G{timestamp_ms\n>= cutoff_ms?}
    G -- Yes --> KEEP4[keep — reason: within_retention]
    G -- No --> H{Already in\narchive index?}
    H -- Yes --> SKIP[skip — reason: already_archived]
    H -- No --> ARC[archive — reason: archive]

    style KEEP1 fill:#dcfce7
    style KEEP2 fill:#dcfce7
    style KEEP3 fill:#dcfce7
    style KEEP4 fill:#dcfce7
    style SKIP  fill:#f3f4f6
    style ARC   fill:#fef3c7
```

---

## Config Loading

```mermaid
flowchart LR
    YAML[archive-job.yaml\nor restore-job.yaml] --> EXP[_expand_env\nENV VAR substitution]
    EXP --> CFG[ArchiveJobConfig\nor RestoreJobConfig\ndataclass]
    ENV[Environment variables] --> EXP
    CFG --> ARC[IcebergArchiver\nor IcebergRestorer]
    CLI[CLI flags\n--source-root etc.] --> CFG
```

Environment variable substitution uses `${VAR}` syntax.  If the variable is
not set, the literal `${VAR}` string is preserved (visible in logs — useful
for debugging misconfigured secrets).

---

## Key Interfaces

### StorageBackend (reused from core)

All archive/restore I/O goes through `StorageBackend`.  No new cloud SDK calls
are made in this module — `create_storage(uri, **kwargs)` from
`iceberg_sync.storage.factory` selects the correct backend.

| Method | Used by |
|--------|---------|
| `read_bytes(uri)` | partition_scanner, metadata_editor, restorer |
| `write_bytes(uri, data)` | metadata_editor, archive_index |
| `copy_from(src_backend, src_uri, dst_uri)` | archiver, restorer |
| `exists(uri)` | restore_planner (conflict detection) |
| `list_objects(prefix)` | restorer (finding latest archived metadata) |

### NessieCatalog (reused from core)

After writing a new metadata file, `archiver._commit_metadata()` and
`restorer._commit_metadata()` both try `nessie.update(table, new_meta_uri)`
first, falling back to writing `version-hint.text` if Nessie is not configured.

---

## Adding a New Storage Backend

No changes needed in the archive module — add the backend to
`iceberg_sync.storage` following the `StorageBackend` ABC, register it in
`create_storage()`, and it will automatically be available for archive/restore.

---

## Testing Approach

Use `MemoryStorageBackend` (already in the project) to write unit tests
without cloud credentials:

```python
from iceberg_sync.storage.memory import MemoryStorageBackend
from iceberg_sync.archive.archiver import IcebergArchiver

source = MemoryStorageBackend()
archive = MemoryStorageBackend()

# Seed source with a minimal metadata.json + manifest Avro files
# ...

archiver = IcebergArchiver(
    source_storage=source,
    archive_storage=archive,
    source_root="mem://source/iceberg/",
    archive_root="mem://archive/iceberg/",
    older_than="1ms",   # expire everything immediately in tests
    min_snapshots_to_keep=1,
    delete_after_archive=True,
)

result = archiver.archive_table("gold/orders", dry_run=False)
assert result.success
assert result.snapshots_archived == 1
```

---

## Error Handling Contract

| Failure point | Behaviour |
|---------------|-----------|
| Cannot load source metadata | `ArchiveResult.errors` populated; no writes to archive |
| File copy failure (archive) | Log warning; snapshot skipped; other snapshots continue |
| File copy failure (restore) | **Abort immediately**; no metadata written; primary unchanged |
| Nessie update failure | Warning logged; fall back to `version-hint.text` |
| Conflict with `strategy=fail` | `RuntimeError` raised before any copy begins |
| Archive index missing | `ArchiveIndex.load_or_new()` starts a fresh index; not an error |
| Archived metadata missing | `FileNotFoundError` — restore cannot proceed; user must verify archive |

---

## Dependency Map

```mermaid
flowchart BT
    CLI[archive_cli.py] --> ARC[archiver.py]
    CLI --> RST[restorer.py]

    ARC --> SM[snapshot_manager.py]
    ARC --> PS[partition_scanner.py]
    ARC --> ME[metadata_editor.py]
    ARC --> AI[archive_index.py]

    RST --> AI
    RST --> PS
    RST --> RP[restore_planner.py]
    RST --> ME

    ARC --> CFG[config.py]
    RST --> CFG

    ARC --> SF[storage/factory.py]
    RST --> SF
    ARC --> NC[catalog/nessie.py]
    RST --> NC
    ARC --> OA[auth/oauth_client.py]
    RST --> OA

    style CLI fill:#fef3c7,stroke:#d97706
    style ARC fill:#dbeafe,stroke:#2563eb
    style RST fill:#dbeafe,stroke:#2563eb
```
