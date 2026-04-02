"""
e2e/scripts/06_access_control.py
==================================
Demonstrate catalog-gateway role-based access control using the
four default OAuth clients.

Expected behaviour:
  admin-client     — unrestricted access, all rows/columns visible
  sync-service     — read + write all namespaces
  analytics-client — gold namespace read-only; EMEA rows only; PII masked
  data-scientist   — silver/bronze read-only; PII columns excluded entirely

Run:
  python e2e/scripts/06_access_control.py

The script shows:
  1. Row counts per client (analytics-client should return fewer rows)
  2. Column visibility (data-scientist should not see clerk/customer_key)
  3. 403 Forbidden when an unauthorised client tries to access a table
"""

from __future__ import annotations

import os
import sys

GATEWAY_URL    = os.getenv("GATEWAY_URL",    "http://localhost:8083")
OAUTH_URL      = os.getenv("OAUTH_URL",      "http://localhost:8081/oauth2/token")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY","minioadmin")

CLIENTS = {
    "admin-client":     ("admin-client",     "admin-secret",   "Unrestricted admin"),
    "sync-service":     ("sync-service",     "sync-secret",    "Read + write all namespaces"),
    "analytics-client": ("analytics-client", "analytics-secret","Gold read, EMEA rows, PII masked"),
    "data-scientist":   ("data-scientist",   "ds-secret",      "Silver/bronze read, PII excluded"),
}


def get_catalog(client_id: str, client_secret: str):
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": GATEWAY_URL,
            "oauth2-server-uri": OAUTH_URL,
            "credential": f"{client_id}:{client_secret}",
            "scope": "catalog:read",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS,
            "s3.secret-access-key": MINIO_SECRET,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def print_banner(title: str) -> None:
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def scan_table(catalog, table_name: str) -> dict:
    """Scan a table and return row count + visible columns."""
    try:
        table = catalog.load_table(table_name)
        arrow_tbl = table.scan(limit=100_000).to_arrow()
        return {
            "status": "ok",
            "rows": len(arrow_tbl),
            "columns": arrow_tbl.column_names,
        }
    except Exception as exc:
        code = "403" if "403" in str(exc) or "Forbidden" in str(exc) else "ERR"
        return {"status": code, "error": str(exc)[:120]}


def main() -> None:
    print_banner("Catalog Gateway — Role-Based Access Control Demo")

    results: dict[str, dict] = {}

    for label, (client_id, client_secret, description) in CLIENTS.items():
        print(f"\n[{label}]  {description}")
        print("-" * 50)
        catalog = get_catalog(client_id, client_secret)

        # List accessible namespaces
        try:
            namespaces = [ns[0] for ns in catalog.list_namespaces()]
            print(f"  Namespaces visible : {namespaces}")
        except Exception as exc:
            print(f"  list_namespaces()  : ERROR — {exc}")
            namespaces = []

        # Try to read tpch.orders
        result = scan_table(catalog, "tpch.orders")
        results[label] = result

        if result["status"] == "ok":
            print(f"  tpch.orders rows   : {result['rows']:,}")
            print(f"  Visible columns    : {result['columns']}")
        elif result["status"] == "403":
            print(f"  tpch.orders        : *** 403 FORBIDDEN ***")
            print(f"  Gateway enforced access control for '{label}' ✓")
        else:
            print(f"  tpch.orders        : {result['status']} — {result.get('error')}")

    # ── Summary ──────────────────────────────────────────────────────────────
    print_banner("Summary")
    print(f"\n{'Client':<20} {'Status':<8} {'Rows':>10}  Visible columns")
    print("-" * 80)
    for label, res in results.items():
        if res["status"] == "ok":
            cols = ", ".join(res["columns"][:5])
            if len(res["columns"]) > 5:
                cols += f" … (+{len(res['columns'])-5} more)"
            print(f"{label:<20} {'OK':<8} {res['rows']:>10}  {cols}")
        else:
            print(f"{label:<20} {res['status']:<8} {'—':>10}  {res.get('error','')}")

    print()
    print("Key observations:")
    print("  • admin-client and sync-service see all rows and all columns.")
    print("  • analytics-client sees only EMEA rows (row filter applied by gateway).")
    print("  • analytics-client's PII columns (clerk) are masked/hashed.")
    print("  • data-scientist gets a 403 on tpch.orders (gold namespace blocked).")
    print("    They can only read silver/bronze tables as defined in OPA policy.")
    print()


if __name__ == "__main__":
    main()
