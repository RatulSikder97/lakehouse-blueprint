# ADR 0003: DuckDB for lightweight reads, Trino for distributed SQL

- Status: accepted
- Date: 2026-11-24

## Context

Lakehouse queries come in two shapes:

1. Interactive, single-user or embedded work: notebooks, CI, data apps, ad-hoc checks on
   gold tables of a few GB.
2. Shared, concurrent, write-heavy SQL: dbt builds, MERGE, maintenance procedures, BI
   connections.

No single engine is the cheapest answer to both.

## Decision

- **Trino** is the shared SQL engine and the only writer of silver/gold. It runs dbt
  (`dbt-trino`), the CDC `MERGE INTO`, the quality checks and the Iceberg maintenance
  procedures.
- **DuckDB** (in-process, `iceberg` extension) is the lightweight reader. It attaches the
  same REST catalog, so gold marts and time-travel queries need no server. dbt also runs on
  DuckDB in CI (`dbt-duckdb` with a parquet fixture), so every PR compiles and tests every
  model and test without containers.
- **Spark** is optional (`--profile spark`) for ETL that outgrows one Trino node and for
  the richer Iceberg procedures (`rewrite_data_files` with sort strategies,
  `rewrite_position_delete_files`).

## Rationale

- One catalog and one copy of the data, with engines picked per workload. This is the
  point of an open table format.
- Trino on Iceberg supports `MERGE`, `CREATE OR REPLACE TABLE` (dbt's `on_table_exists:
  replace` keeps table history), and `ALTER TABLE ... EXECUTE optimize | expire_snapshots |
  remove_orphan_files`.
- DuckDB starts in milliseconds and needs no cluster. Running dbt tests on it in CI costs
  seconds.

## Consequences

- Models must be written in the SQL subset both engines share. We rely on dbt cross-database
  macros (`dbt.datediff`, `dbt.concat`), standard `CASE`/`CAST`/window syntax, and an
  `iceberg_properties()` macro that only emits Trino table properties when `target.type ==
  'trino'`.
- The `ci` target proves logic and contracts, not Iceberg behaviour. The compose-based e2e
  job covers that.
