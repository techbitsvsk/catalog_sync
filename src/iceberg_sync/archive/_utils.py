"""archive/_utils.py — Small shared helpers for the archive package."""


def fmt_bytes(n: int) -> str:
    """Format a byte count as a human-readable string (e.g. 1.5 MB)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024   # float division — preserves decimal
    return f"{n:.1f} PB"
