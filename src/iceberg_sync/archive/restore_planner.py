"""
archive/restore_planner.py — Build a RestorePlan (dry-run) before any writes.

The plan is the single source of truth for what a restore will do.
It is built from the archive index + partition scanner and printed for
human review.  The restorer.execute() method takes the same plan object,
so what you see in the dry-run is exactly what gets executed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from iceberg_sync.archive._utils import fmt_bytes as _fmt_bytes
from iceberg_sync.archive.partition_scanner import ScannedFile, unique_partitions, total_bytes
from iceberg_sync.storage.base import StorageBackend

log = logging.getLogger(__name__)

console = Console()


# ──────────────────────────────────────────────────────────────────────────────
# Plan data model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class PartitionRestoreInfo:
    partition: Dict[str, Any]
    files_count: int
    size_bytes: int
    has_conflict: bool
    conflict_files: int = 0
    conflict_action: str = ""     # "overwrite" | "skip" | "fail"


@dataclass
class MetadataAction:
    description: str
    detail: str = ""


@dataclass
class RestorePlan:
    """
    Immutable description of a planned restore operation.

    Created by RestorePlanner.build_plan() and passed to IcebergRestorer.execute().
    """
    archive_root: str
    target_root: str
    table: str
    restore_as: Optional[str]
    mode: str
    snapshot_id: int
    snapshot_timestamp_iso: str
    requested_partitions: List[Dict[str, Any]]    # empty = full table

    files_to_copy: List[ScannedFile] = field(default_factory=list)
    partition_summary: List[PartitionRestoreInfo] = field(default_factory=list)
    metadata_actions: List[MetadataAction] = field(default_factory=list)
    conflicts: List[PartitionRestoreInfo] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def total_files(self) -> int:
        return len(self.files_to_copy)

    @property
    def total_bytes(self) -> int:
        return sum(f.file_size_bytes for f in self.files_to_copy)

    @property
    def effective_table(self) -> str:
        return self.restore_as or self.table

    @property
    def has_conflicts(self) -> bool:
        return bool(self.conflicts)

    @property
    def is_full_table(self) -> bool:
        return not self.requested_partitions

    # ── Pretty print ──────────────────────────────────────────────────────────

    def print(self) -> None:
        c = Console()
        c.print()
        c.print(Panel.fit(
            "[bold cyan]Restore Plan[/bold cyan]",
            border_style="cyan",
        ))

        # Summary table
        summary = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
        summary.add_column("Key", style="dim")
        summary.add_column("Value", style="bold")
        summary.add_row("Archive",    self.archive_root)
        summary.add_row("Target",     self.target_root)
        summary.add_row("Table",      self.table)
        if self.restore_as:
            summary.add_row("Restore as", self.restore_as)
        summary.add_row("Mode",       self.mode)
        summary.add_row("Snapshot",   f"{self.snapshot_id}  ({self.snapshot_timestamp_iso})")
        scope = "full table" if self.is_full_table else f"{len(self.requested_partitions)} partition(s)"
        summary.add_row("Scope",      scope)
        c.print(summary)

        # Partition detail
        if self.partition_summary:
            pt = Table(
                "Partition", "Files", "Size", "Conflict",
                box=box.SIMPLE_HEAVY, header_style="bold magenta",
            )
            for pi in self.partition_summary:
                part_label = "/".join(f"{k}={v}" for k, v in pi.partition.items())
                conflict_label = (
                    f"[yellow]{pi.conflict_files} files → {pi.conflict_action}[/yellow]"
                    if pi.has_conflict else "[green]none[/green]"
                )
                pt.add_row(part_label, str(pi.files_count), _fmt_bytes(pi.size_bytes), conflict_label)
            c.print(pt)

        # Totals
        c.print(
            f"  [bold]Total:[/bold] {self.total_files} files   "
            f"[bold]{_fmt_bytes(self.total_bytes)}[/bold]"
        )
        c.print()

        # Metadata actions
        if self.metadata_actions:
            c.print("[bold]Metadata actions:[/bold]")
            for ma in self.metadata_actions:
                c.print(f"  [cyan]•[/cyan] {ma.description}", end="")
                if ma.detail:
                    c.print(f"  [dim]{ma.detail}[/dim]", end="")
                c.print()
            c.print()

        # Warnings
        for w in self.warnings:
            c.print(f"  [yellow]⚠[/yellow]  {w}")

        if self.has_conflicts and not self.warnings:
            c.print()

        c.print(
            "[dim]This is a dry-run plan.  "
            "Pass --confirm (or call restorer.execute(plan)) to apply.[/dim]"
        )
        c.print()


# ──────────────────────────────────────────────────────────────────────────────
# Planner
# ──────────────────────────────────────────────────────────────────────────────

class RestorePlanner:
    """
    Builds a RestorePlan by scanning the archive and the live target table.

    This class never writes anything — it is purely analytical.
    """

    def __init__(
        self,
        archive_storage: StorageBackend,
        target_storage: StorageBackend,
        archive_root: str,
        target_root: str,
    ) -> None:
        self._archive = archive_storage
        self._target = target_storage
        self._archive_root = archive_root
        self._target_root = target_root

    def build_plan(
        self,
        *,
        table: str,
        restore_as: Optional[str],
        snapshot_id: int,
        snapshot_timestamp_iso: str,
        requested_partitions: List[Dict[str, Any]],
        files_to_copy: List[ScannedFile],
        mode: str,
        conflict_strategy: str,
    ) -> RestorePlan:

        effective_table = restore_as or table
        target_table_root = f"{self._target_root.rstrip('/')}/{effective_table}"

        # Group files by partition
        partition_map: Dict[str, List[ScannedFile]] = {}
        for f in files_to_copy:
            key = str(sorted(f.partition.items()))
            partition_map.setdefault(key, []).append(f)

        partition_summary: List[PartitionRestoreInfo] = []
        conflicts: List[PartitionRestoreInfo] = []
        warnings: List[str] = []

        for part_key, part_files in partition_map.items():
            part_dict = part_files[0].partition
            part_label = "/".join(f"{k}={v}" for k, v in part_dict.items())
            part_size = sum(pf.file_size_bytes for pf in part_files)

            # Check for conflicts: files that exist in target with same relative path
            conflict_count = 0
            if mode == "replace" and effective_table == table:
                for pf in part_files:
                    rel = pf.file_path.split(table + "/", 1)[-1]
                    target_uri = f"{target_table_root}/{rel}"
                    try:
                        if self._target.exists(target_uri):
                            conflict_count += 1
                    except Exception:
                        pass

            pi = PartitionRestoreInfo(
                partition=part_dict,
                files_count=len(part_files),
                size_bytes=part_size,
                has_conflict=conflict_count > 0,
                conflict_files=conflict_count,
                conflict_action=conflict_strategy if conflict_count > 0 else "",
            )
            partition_summary.append(pi)

            if pi.has_conflict:
                conflicts.append(pi)
                if conflict_strategy == "fail":
                    warnings.append(
                        f"Partition {part_label}: {conflict_count} conflicting file(s). "
                        "Use --conflict-strategy=overwrite to proceed."
                    )

        # Metadata actions (human-readable)
        metadata_actions: List[MetadataAction] = []
        new_meta_version = "(current + 1)"

        if mode == "new_table":
            metadata_actions.append(MetadataAction(
                f"Write fresh metadata.json for {effective_table}",
                "New table — no merge required",
            ))
        elif mode == "replace" and not requested_partitions:
            metadata_actions.append(MetadataAction(
                f"Replace full snapshot in {effective_table}",
                f"Rewrite metadata.json → v{new_meta_version}",
            ))
        elif mode == "replace":
            metadata_actions.append(MetadataAction(
                f"Merge {len(partition_map)} partition(s) into live snapshot of {effective_table}",
                f"Filter current manifests + inject restored manifests → v{new_meta_version}",
            ))
        elif mode == "append":
            metadata_actions.append(MetadataAction(
                f"Append restored files as new snapshot in {effective_table}",
                "operation=append",
            ))

        metadata_actions.append(MetadataAction(
            "Update catalog pointer",
            "version-hint.text or Nessie register",
        ))

        return RestorePlan(
            archive_root=self._archive_root,
            target_root=self._target_root,
            table=table,
            restore_as=restore_as,
            mode=mode,
            snapshot_id=snapshot_id,
            snapshot_timestamp_iso=snapshot_timestamp_iso,
            requested_partitions=requested_partitions,
            files_to_copy=files_to_copy,
            partition_summary=partition_summary,
            metadata_actions=metadata_actions,
            conflicts=conflicts,
            warnings=warnings,
        )
