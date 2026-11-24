"""End-to-end showcase queries: snapshots, schema evolution, CDC, time travel, gold marts."""

from __future__ import annotations

import textwrap
from collections.abc import Callable

from lakehouse import duck
from lakehouse import trino_client as tc
from lakehouse.config import Settings

Runner = Callable[[str], tuple[list[str], list[tuple]]]


def _section(title: str) -> None:
    print(f"\n{'=' * 100}\n{title}\n{'=' * 100}")


def _show(engine: str, run: Runner, sql: str) -> list[tuple]:
    sql = textwrap.dedent(sql).strip()
    print(f"\n-- engine: {engine}\n{sql};\n")
    columns, rows = run(sql)
    print(tc.render_table(columns, rows))
    return rows


def run_demo(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    conn = tc.connect(settings)
    dconn = duck.connect(settings)

    def trino(sql: str):
        return tc.query(conn, sql)

    def duckdb(sql: str):
        return duck.query(dconn, sql)

    c = settings.trino_catalog

    _section("1. What is in the lakehouse (Trino information_schema over the Lakekeeper REST catalog)")
    _show(
        "trino",
        trino,
        f"""
        SELECT table_schema, table_name
        FROM {c}.information_schema.tables
        WHERE table_schema IN ('bronze', 'silver', 'gold', 'reference', 'ops')
        ORDER BY 1, 2""",
    )

    _section("2. Iceberg snapshots on bronze.yellow_trips (ingest overwrites + CDC MERGE)")
    _show(
        "trino",
        trino,
        f"""
        SELECT committed_at, snapshot_id, operation,
               element_at(summary, 'added-records') AS added_records,
               element_at(summary, 'deleted-records') AS deleted_records,
               element_at(summary, 'added-position-deletes') AS added_pos_deletes,
               element_at(summary, 'total-records') AS total_records
        FROM {c}.bronze."yellow_trips$snapshots"
        ORDER BY committed_at""",
    )

    _section("3. Schema evolution: cbd_congestion_fee appeared in the January 2025 source file")
    _show(
        "trino",
        trino,
        f"""
        SELECT source_month,
               count(*) AS trips,
               count(cbd_congestion_fee) AS rows_with_cbd_fee,
               round(sum(cbd_congestion_fee), 2) AS cbd_fee_total
        FROM {c}.bronze.yellow_trips
        GROUP BY source_month
        ORDER BY source_month""",
    )

    _section("4. CDC: change feed landed append-only, applied with one MERGE INTO")
    _show(
        "trino",
        trino,
        f"""
        SELECT op, count(*) AS changes, count(DISTINCT trip_id) AS distinct_trips
        FROM {c}.bronze.yellow_trips_changes
        GROUP BY op
        ORDER BY op""",
    )
    log = _show(
        "trino",
        trino,
        f"""
        SELECT batch_id, changes, inserted, updated, deleted, snapshot_before, snapshot_after
        FROM {c}.ops.cdc_merge_log
        ORDER BY applied_at DESC
        LIMIT 1""",
    )
    if not log:
        print("no CDC batch applied yet; run `make cdc`")
        return
    batch_id, before = log[0][0], log[0][5]

    _section("5. Time travel in Trino: the table before vs after the MERGE")
    _show(
        "trino",
        trino,
        f"""
        SELECT 'before_merge' AS version, count(*) AS trips, round(sum(total_amount), 2) AS revenue,
               count_if(trip_id LIKE 'late-%') AS late_arrivals
        FROM {c}.bronze.yellow_trips FOR VERSION AS OF {before}
        UNION ALL
        SELECT 'current', count(*), round(sum(total_amount), 2), count_if(trip_id LIKE 'late-%')
        FROM {c}.bronze.yellow_trips""",
    )

    _section("6. Time travel in DuckDB against the same snapshot (no Trino involved)")
    _show(
        "duckdb",
        duckdb,
        f"""
        SELECT 'before_merge' AS version, count(*) AS trips, round(sum(total_amount), 2) AS revenue
        FROM lake.bronze.yellow_trips AT (VERSION => {before})
        UNION ALL
        SELECT 'current', count(*), round(sum(total_amount), 2)
        FROM lake.bronze.yellow_trips""",
    )

    _section("7. One trip corrected twice in the feed: only the latest image survived the MERGE")
    _show(
        "trino",
        trino,
        f"""
        WITH twice AS (
            SELECT trip_id FROM {c}.bronze.yellow_trips_changes
            WHERE change_batch_id = '{batch_id}' AND op = 'U'
            GROUP BY trip_id HAVING count(*) = 2
            ORDER BY trip_id LIMIT 1
        )
        SELECT 'before_merge' AS version, t.trip_id, t.tip_amount, t.total_amount, t._batch_id
        FROM {c}.bronze.yellow_trips FOR VERSION AS OF {before} t JOIN twice w ON t.trip_id = w.trip_id
        UNION ALL
        SELECT 'current', t.trip_id, t.tip_amount, t.total_amount, t._batch_id
        FROM {c}.bronze.yellow_trips t JOIN twice w ON t.trip_id = w.trip_id
        ORDER BY 1 DESC""",
    )

    _section("8. Gold marts built by dbt on Trino, read by DuckDB")
    _show(
        "duckdb",
        duckdb,
        """
        SELECT pickup_date, trips, revenue, avg_fare, avg_trip_minutes, credit_card_tip_pct
        FROM lake.gold.daily_trip_metrics
        ORDER BY revenue DESC
        LIMIT 5""",
    )
    _show(
        "duckdb",
        duckdb,
        """
        SELECT pickup_month, borough, zone_name, trips, revenue
        FROM lake.gold.zone_revenue_monthly
        WHERE pickup_month = DATE '2025-01-01'
        ORDER BY revenue DESC
        LIMIT 5""",
    )
    _show(
        "trino",
        trino,
        f"""
        SELECT pickup_month, payment_method, trips, share_of_trips_pct
        FROM {c}.gold.payment_mix_monthly
        ORDER BY pickup_month, trips DESC""",
    )
