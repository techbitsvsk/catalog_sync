"""
e2e/scripts/01_generate_tpch.py
================================
Generate a TPC-H-like orders dataset using DuckDB and export it as
Parquet files pre-partitioned by order_month (YYYY-MM).

Output layout (ready for Iceberg ingestion):
  e2e/data/orders/order_month=2024-01/part-0.parquet
  e2e/data/orders/order_month=2024-02/part-0.parquet
  ...
  e2e/data/orders/order_month=2024-10/part-0.parquet

Usage:
  pip install duckdb pyarrow
  python e2e/scripts/01_generate_tpch.py [--rows 1500000] [--out e2e/data/orders]

Sizing guide:
  --rows 500000   ≈  50 MB Parquet total  (quick smoke test)
  --rows 1500000  ≈ 150 MB Parquet total  (default, ~sf=1 TPC-H)
  --rows 5000000  ≈ 500 MB Parquet total  (heavier load test)
  --rows 30000000 ≈   3 GB Parquet total  (full 3 GB target)
"""

import argparse
import os
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA = pa.schema([
    pa.field("order_key",       pa.int64(),       nullable=False),
    pa.field("customer_key",    pa.int64()),
    pa.field("order_status",    pa.string()),
    pa.field("total_price",     pa.decimal128(15, 2)),
    pa.field("order_date",      pa.date32()),
    pa.field("order_priority",  pa.string()),
    pa.field("clerk",           pa.string()),
    pa.field("ship_priority",   pa.int32()),
    pa.field("comment",         pa.string()),
    pa.field("order_month",     pa.string()),   # partition column: YYYY-MM
])


def generate(rows: int, out_dir: Path) -> None:
    print(f"Generating {rows:,} rows spanning 2024-01 through 2024-10 …")
    t0 = time.time()

    conn = duckdb.connect()

    # Build synthetic orders data covering Jan–Oct 2024 (305 days)
    conn.execute(f"""
        CREATE TABLE orders AS
        SELECT
            row_number() OVER ()                                                      AS order_key,
            (random() * 99999 + 1)::BIGINT                                            AS customer_key,
            CASE (random() * 3)::INT
                WHEN 0 THEN 'O'
                WHEN 1 THEN 'F'
                ELSE       'P'
            END                                                                       AS order_status,
            ROUND((random() * 499999 + 1)::DOUBLE, 2)::DECIMAL(15,2)                 AS total_price,
            (DATE '2024-01-01' + ((random() * 304)::INT * INTERVAL '1' DAY))::DATE   AS order_date,
            CASE (random() * 4)::INT
                WHEN 0 THEN '1-URGENT'
                WHEN 1 THEN '2-HIGH'
                WHEN 2 THEN '3-MEDIUM'
                WHEN 3 THEN '4-NOT SPECIFIED'
                ELSE        '5-LOW'
            END                                                                       AS order_priority,
            'Clerk#' || LPAD(((random() * 999)::INT)::VARCHAR, 9, '0')               AS clerk,
            0::INT                                                                    AS ship_priority,
            'Generated order ' || row_number() OVER ()                               AS comment,
            strftime(
                (DATE '2024-01-01' + ((random() * 304)::INT * INTERVAL '1' DAY))::DATE,
                '%Y-%m'
            )                                                                         AS order_month
        FROM range(0, {rows})
    """)

    months = conn.execute(
        "SELECT DISTINCT order_month FROM orders ORDER BY order_month"
    ).fetchall()
    print(f"  Months in dataset: {[m[0] for m in months]}")

    # Export one Parquet file per month partition
    out_dir.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    for (month,) in months:
        part_dir = out_dir / f"order_month={month}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_path = part_dir / "part-0.parquet"

        arrow_tbl = conn.execute(
            "SELECT * FROM orders WHERE order_month = ? ORDER BY order_date",
            [month],
        ).fetch_arrow_table()

        pq.write_table(
            arrow_tbl,
            out_path,
            compression="snappy",
            row_group_size=100_000,
        )
        size_mb = out_path.stat().st_size / 1_048_576
        total_bytes += out_path.stat().st_size
        print(f"  {out_path}  ({size_mb:.1f} MB,  {len(arrow_tbl):,} rows)")

    elapsed = time.time() - t0
    print(
        f"\nDone: {len(months)} partitions, "
        f"{total_bytes / 1_048_576:.1f} MB total "
        f"in {elapsed:.1f}s"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate TPC-H-like orders dataset.")
    parser.add_argument("--rows",  type=int,  default=1_500_000,
                        help="Total number of order rows to generate (default: 1_500_000 ≈ 150 MB)")
    parser.add_argument("--out",   type=Path, default=Path("e2e/data/orders"),
                        help="Output directory for partitioned Parquet files")
    args = parser.parse_args()
    generate(args.rows, args.out)


if __name__ == "__main__":
    main()
