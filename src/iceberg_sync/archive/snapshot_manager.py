"""
archive/snapshot_manager.py — Decide which snapshots to archive / expire.

Rules applied in order:
  1.  The current snapshot is always kept.
  2.  At least `min_snapshots_to_keep` snapshots are kept (newest first).
  3.  Snapshots older than `older_than` are eligible for archival.
  4.  Any snapshot referenced by a named branch or tag in `refs` is kept.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Duration parser
# ──────────────────────────────────────────────────────────────────────────────

_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)$", re.IGNORECASE)

_UNIT_TO_MS: Dict[str, float] = {
    "ms": 1,
    "s":  1_000,
    "m":  60_000,
    "h":  3_600_000,
    "d":  86_400_000,
    "w":  604_800_000,
}


def parse_duration_ms(duration: str) -> int:
    """
    Parse a human-readable duration string to milliseconds.

    Supported units: ms, s, m, h, d, w
    Examples: "7d", "24h", "30m", "2w", "500ms"
    """
    m = _DURATION_RE.match(duration.strip())
    if not m:
        raise ValueError(
            f"Cannot parse duration '{duration}'. "
            "Expected format: <number><unit> where unit is ms|s|m|h|d|w  (e.g. 7d, 24h)"
        )
    value, unit = float(m.group(1)), m.group(2).lower()
    return int(value * _UNIT_TO_MS[unit])


# ──────────────────────────────────────────────────────────────────────────────
# Decision model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class SnapshotDecision:
    snapshot_id: int
    timestamp_ms: int
    operation: str
    keep: bool
    reason: str   # "current" | "named_ref" | "min_keep" | "within_retention" | "archive"

    @property
    def timestamp_iso(self) -> str:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(
            self.timestamp_ms / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")


# ──────────────────────────────────────────────────────────────────────────────
# Main decision function
# ──────────────────────────────────────────────────────────────────────────────

def decide_snapshots(
    metadata: dict,
    older_than: str,
    min_snapshots_to_keep: int = 1,
    *,
    now_ms: Optional[int] = None,
    already_archived_ids: Optional[Set[int]] = None,
    effective_timestamps: Optional[Dict[int, int]] = None,
) -> List[SnapshotDecision]:
    """
    Evaluate every snapshot in *metadata* and return a decision for each.

    Args:
        metadata:               Parsed metadata.json dict.
        older_than:             Duration string — snapshots older than this are
                                eligible for archival (e.g. "30d").
        min_snapshots_to_keep:  Minimum number of snapshots to keep, regardless
                                of age. Applied after the "current" and
                                "named_ref" rules.
        now_ms:                 Current time in epoch-ms (injectable for tests).
        already_archived_ids:   Snapshot IDs already present in the archive
                                index — skip re-archiving them.
        effective_timestamps:   Optional mapping of snapshot_id → epoch-ms to
                                use as the logical age of that snapshot instead
                                of its write timestamp. Intended for COB-date
                                awareness: pass the partition lower-bound date
                                so that retention is evaluated against the data's
                                business date rather than when it was written.

    Returns:
        List of SnapshotDecision, one per snapshot, ordered newest-first.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    cutoff_ms = now_ms - parse_duration_ms(older_than)
    already_archived = already_archived_ids or set()
    cob_dates = effective_timestamps or {}

    snapshots: List[dict] = metadata.get("snapshots", [])
    if not snapshots:
        return []

    current_id: int = metadata.get("current-snapshot-id", -1)

    # Collect snapshot-ids pinned by named refs (branches / tags)
    pinned_by_ref: Set[int] = set()
    for ref_name, ref_info in metadata.get("refs", {}).items():
        if ref_name != "main":  # main always tracked via current-snapshot-id
            snap_id = ref_info.get("snapshot-id")
            if snap_id is not None:
                pinned_by_ref.add(snap_id)

    # Sort newest-first for min_keep counting
    sorted_snaps = sorted(snapshots, key=lambda s: s.get("timestamp-ms", 0), reverse=True)

    kept_count = 0
    decisions: List[SnapshotDecision] = []

    for snap in sorted_snaps:
        snap_id: int = snap.get("snapshot-id", 0)
        ts_ms: int = snap.get("timestamp-ms", 0)
        operation: str = snap.get("summary", {}).get("operation", "unknown")
        # Use COB date if provided; fall back to write timestamp.
        effective_ts_ms: int = cob_dates.get(snap_id, ts_ms)

        # Rule 1: always keep current
        if snap_id == current_id:
            decisions.append(SnapshotDecision(snap_id, ts_ms, operation, True, "current"))
            kept_count += 1
            continue

        # Rule 2: keep snapshots pinned by named refs
        if snap_id in pinned_by_ref:
            decisions.append(SnapshotDecision(snap_id, ts_ms, operation, True, "named_ref"))
            kept_count += 1
            continue

        # Rule 3: enforce min_snapshots_to_keep
        if kept_count < min_snapshots_to_keep:
            decisions.append(SnapshotDecision(snap_id, ts_ms, operation, True, "min_keep"))
            kept_count += 1
            continue

        # Rule 4: within retention window → keep (uses COB date when provided)
        if effective_ts_ms >= cutoff_ms:
            decisions.append(SnapshotDecision(snap_id, ts_ms, operation, True, "within_retention"))
            kept_count += 1
            continue

        # Skip already archived
        if snap_id in already_archived:
            decisions.append(SnapshotDecision(snap_id, ts_ms, operation, False, "already_archived"))
            continue

        # Eligible for archival
        decisions.append(SnapshotDecision(snap_id, ts_ms, operation, False, "archive"))

    return decisions


def snapshots_to_archive(decisions: List[SnapshotDecision]) -> List[SnapshotDecision]:
    return [d for d in decisions if not d.keep and d.reason == "archive"]


def snapshots_to_keep(decisions: List[SnapshotDecision]) -> List[SnapshotDecision]:
    return [d for d in decisions if d.keep]
