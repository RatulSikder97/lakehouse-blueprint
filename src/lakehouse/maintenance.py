"""Iceberg table maintenance through Trino: compaction, snapshot expiry, orphan cleanup."""

from __future__ import annotations

import re
from dataclasses import dataclass

from lakehouse.trino_client import query

_DURATION = re.compile(r"^\d+(\.\d+)?(ms|s|m|h|d)$")
_DATA_SIZE = re.compile(r"^\d+(\.\d+)?(B|kB|MB|GB)$")
_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")

DEFAULT_TABLES = (
    "bronze.yellow_trips",
    "bronze.yellow_trips_changes",
    "silver.trips",
    "gold.daily_trip_metrics",
    "gold.zone_revenue_monthly",
    "gold.payment_mix_monthly",
)


@dataclass
class TableStats:
    data_files: int
    delete_files: int
    snapshots: int


@dataclass
class MaintenanceReport:
    table: str
    before: TableStats
    after: TableStats


def validate_duration(value: str) -> str:
    if not _DURATION.match(value):
        raise ValueError(f"invalid Trino duration {value!r} (examples: 7d, 12h, 0s)")
    return value


def validate_data_size(value: str) -> str:
    if not _DATA_SIZE.match(value):
        raise ValueError(f"invalid Trino data size {value!r} (examples: 128MB, 1GB)")
    return value


def validate_table(name: str) -> str:
    if not _TABLE.match(name):
        raise ValueError(f"table must be schema.table in lower snake case, got {name!r}")
    return name


def maintenance_statements(fq_table: str, retention: str, file_size_threshold: str) -> list[str]:
    """Ordered maintenance procedure calls for one table."""
    validate_duration(retention)
    validate_data_size(file_size_threshold)
    return [
        # Bin-pack small files and fold merge-on-read delete files back into data files.
        f"ALTER TABLE {fq_table} EXECUTE optimize(file_size_threshold => '{file_size_threshold}')",
        f"ALTER TABLE {fq_table} EXECUTE optimize_manifests",
        f"ALTER TABLE {fq_table} EXECUTE expire_snapshots(retention_threshold => '{retention}')",
        f"ALTER TABLE {fq_table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')",
    ]


def table_stats(conn, catalog: str, table: str) -> TableStats:
    schema, name = validate_table(table).split(".")
    _, rows = query(
        conn,
        f'SELECT content, count(*) FROM {catalog}.{schema}."{name}$files" GROUP BY content',
    )
    by_content = {int(content): int(n) for content, n in rows}
    _, snaps = query(conn, f'SELECT count(*) FROM {catalog}.{schema}."{name}$snapshots"')
    return TableStats(
        data_files=by_content.get(0, 0),
        delete_files=by_content.get(1, 0) + by_content.get(2, 0),
        snapshots=int(snaps[0][0]),
    )


def existing_tables(conn, catalog: str, tables: tuple[str, ...]) -> list[str]:
    _, rows = query(
        conn,
        f"SELECT table_schema || '.' || table_name FROM {catalog}.information_schema.tables "
        "WHERE table_type = 'BASE TABLE'",
    )
    present = {r[0] for r in rows}
    return [t for t in tables if t in present]


def maintain(
    conn,
    catalog: str,
    tables: tuple[str, ...] = DEFAULT_TABLES,
    *,
    retention: str = "7d",
    file_size_threshold: str = "128MB",
) -> list[MaintenanceReport]:
    reports = []
    for table in existing_tables(conn, catalog, tuple(validate_table(t) for t in tables)):
        fq = f"{catalog}.{table}"
        before = table_stats(conn, catalog, table)
        for stmt in maintenance_statements(fq, retention, file_size_threshold):
            query(conn, stmt)
        reports.append(MaintenanceReport(table, before, table_stats(conn, catalog, table)))
    return reports
