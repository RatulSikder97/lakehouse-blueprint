"""`lakehouse` command line entry point."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_MONTHS = ["2024-12", "2025-01"]


def _cmd_wait(args: argparse.Namespace) -> int:
    from lakehouse.trino_client import wait_until_ready

    wait_until_ready(timeout_s=args.timeout)
    print("trino is ready")
    return 0


def _cmd_ingest(args: argparse.Namespace) -> int:
    from lakehouse.catalog import get_catalog
    from lakehouse.config import Settings
    from lakehouse.ingest import ingest_month

    settings = Settings()
    catalog = get_catalog(settings)
    for month in args.months:
        r = ingest_month(catalog, settings, month, limit=args.limit)
        added = f", schema evolved: +{','.join(r.added_columns)}" if r.added_columns else ""
        print(
            f"bronze.yellow_trips <- {r.month}: {r.rows:,} rows, snapshot {r.snapshot_id}"
            f" ({r.seconds:.1f}s{added})"
        )
    return 0


def _cmd_cdc(args: argparse.Namespace) -> int:
    from lakehouse.catalog import get_catalog
    from lakehouse.cdc import ChangeSpec, apply_changes, land_changes
    from lakehouse.config import Settings
    from lakehouse.trino_client import connect

    settings = Settings()
    spec = ChangeSpec(
        updates=args.updates,
        deletes=args.deletes,
        inserts=args.inserts,
        double_updates=min(args.double_updates, args.updates),
        seed=args.seed,
    )
    batch_id, n = land_changes(get_catalog(settings), args.month, spec=spec, sample_rows=args.sample_rows)
    print(f"bronze.yellow_trips_changes <- {n:,} change events (batch {batch_id})")
    r = apply_changes(connect(settings), settings.trino_catalog, batch_id)
    print(
        f"MERGE INTO bronze.yellow_trips: {r.changes:,} keys -> inserted {r.inserted:,}, "
        f"updated {r.updated:,}, deleted {r.deleted:,}; snapshot {r.snapshot_before} -> {r.snapshot_after}"
    )
    return 0


def _cmd_maintain(args: argparse.Namespace) -> int:
    from lakehouse.config import Settings
    from lakehouse.maintenance import DEFAULT_TABLES, maintain
    from lakehouse.trino_client import connect, render_table

    settings = Settings()
    tables = tuple(args.tables) if args.tables else DEFAULT_TABLES
    reports = maintain(
        connect(settings),
        settings.trino_catalog,
        tables,
        retention=args.retention,
        file_size_threshold=args.file_size_threshold,
    )
    rows = [
        (
            r.table,
            r.before.data_files,
            r.after.data_files,
            r.before.delete_files,
            r.after.delete_files,
            r.before.snapshots,
            r.after.snapshots,
        )
        for r in reports
    ]
    print(
        f"Iceberg maintenance (optimize <= {args.file_size_threshold}, optimize_manifests, "
        f"expire_snapshots + remove_orphan_files older than {args.retention})\n"
    )
    print(
        render_table(
            [
                "table",
                "data_files_before",
                "data_files_after",
                "delete_files_before",
                "delete_files_after",
                "snapshots_before",
                "snapshots_after",
            ],
            rows,
        )
    )
    return 0


def _cmd_checks(args: argparse.Namespace) -> int:
    from lakehouse.checks import load_checks, run_checks
    from lakehouse.config import Settings
    from lakehouse.trino_client import connect, render_table

    settings = Settings()
    results = run_checks(connect(settings), load_checks(args.file), settings.trino_catalog)
    rows = []
    failed = 0
    for r in results:
        status = "PASS" if r.passed else ("WARN" if r.check.severity == "warn" else "FAIL")
        failed += status == "FAIL"
        value = int(r.value) if r.value is not None and r.value.is_integer() else r.value
        rows.append((r.check.table, r.check.name, value, status))
    print(render_table(["table", "check", "value", "status"], rows))
    print(f"\n{len(results) - failed}/{len(results)} checks passed without failure")
    return 1 if failed else 0


def _cmd_sql(args: argparse.Namespace) -> int:
    from lakehouse.trino_client import render_table

    if args.engine == "duckdb":
        from lakehouse import duck

        cols, rows = duck.query(duck.connect(), args.query)
    else:
        from lakehouse.trino_client import connect, query

        cols, rows = query(connect(), args.query)
    print(render_table(cols, rows))
    return 0


def _cmd_demo(_: argparse.Namespace) -> int:
    from lakehouse.demo import run_demo

    run_demo()
    return 0


def _cmd_fixtures(args: argparse.Namespace) -> int:
    """Build bronze-shaped parquet from raw sample files (used by dbt-duckdb in CI)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from lakehouse.ingest import normalize

    src, out = Path(args.source_dir), Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    batches = []
    for path in sorted(src.glob("yellow_tripdata_*.parquet")):
        month = path.stem.removeprefix("yellow_tripdata_")
        batches.append(
            normalize(
                pq.read_table(path),
                source_month=month,
                source_file=path.name,
                batch_id=f"fixture-{month}",
                ingested_at=now,
            )
        )
    if not batches:
        raise SystemExit(f"no yellow_tripdata_*.parquet files in {src}")
    table = pa.concat_tables(batches, promote_options="default")
    pq.write_table(table, out)
    print(f"wrote {table.num_rows} rows x {table.num_columns} columns to {out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="lakehouse", description=__doc__)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("wait", help="block until Trino answers queries")
    s.add_argument("--timeout", type=int, default=180)
    s.set_defaults(func=_cmd_wait)

    s = sub.add_parser("ingest", help="load TLC months into bronze.yellow_trips")
    s.add_argument("--months", nargs="+", default=DEFAULT_MONTHS)
    s.add_argument("--limit", type=int, default=None, help="only load the first N rows of each month")
    s.set_defaults(func=_cmd_ingest)

    s = sub.add_parser("cdc", help="land a synthetic change feed and MERGE it into bronze")
    s.add_argument("--month", default=DEFAULT_MONTHS[-1])
    s.add_argument("--updates", type=int, default=5_000)
    s.add_argument("--deletes", type=int, default=1_000)
    s.add_argument("--inserts", type=int, default=2_000)
    s.add_argument("--double-updates", type=int, default=250)
    s.add_argument("--sample-rows", type=int, default=200_000)
    s.add_argument("--seed", type=int, default=42)
    s.set_defaults(func=_cmd_cdc)

    s = sub.add_parser("maintain", help="optimize / expire snapshots / remove orphans")
    s.add_argument("--retention", default="7d")
    s.add_argument("--file-size-threshold", default="128MB")
    s.add_argument("--tables", nargs="*")
    s.set_defaults(func=_cmd_maintain)

    s = sub.add_parser("checks", help="run Soda-style data quality checks")
    s.add_argument("--file", default="quality/checks.yml")
    s.set_defaults(func=_cmd_checks)

    s = sub.add_parser("sql", help="run one SQL statement on trino or duckdb")
    s.add_argument("--engine", choices=["trino", "duckdb"], default="trino")
    s.add_argument("query")
    s.set_defaults(func=_cmd_sql)

    s = sub.add_parser("demo", help="print the showcase queries with live output")
    s.set_defaults(func=_cmd_demo)

    s = sub.add_parser("fixtures", help="build the bronze parquet fixture for dbt-duckdb CI")
    s.add_argument("--source-dir", default="tests/fixtures")
    s.add_argument("--out", default="transform/ci_data/yellow_trips.parquet")
    s.set_defaults(func=_cmd_fixtures)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("trino.auth").setLevel(logging.WARNING)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
