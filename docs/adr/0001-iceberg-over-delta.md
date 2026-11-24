# ADR 0001: Apache Iceberg as the table format (over Delta Lake and Hudi)

- Status: accepted
- Date: 2026-11-24

## Context

The blueprint needs an open table format that gives ACID commits, MERGE/UPDATE/DELETE,
snapshots and time travel, and schema evolution on object storage, and that is readable
and writable by every engine we use: PyIceberg (ingestion), Trino (MERGE, dbt, maintenance),
DuckDB (lightweight reads), Spark (heavy ETL), plus the AWS managed equivalents (Athena,
EMR, Glue, Redshift, S3 Tables).

## Decision

Use Apache Iceberg format v2, managed through an Iceberg REST catalog.

## Rationale

- **Engine neutrality.** Iceberg's spec and the REST catalog protocol are vendor-neutral.
  All five engines here speak them natively. Delta Lake reaches the same engines, but in
  practice its first-class write path is Spark/Databricks, and the non-Spark writers lag
  on features such as deletion vectors and column mapping.
- **The catalog is a first-class API.** Commits are atomic catalog swaps behind
  `POST /v1/.../tables/{table}`, so the catalog can enforce access control, vend
  short-lived storage credentials and audit every commit (ADR 0002). Delta stores its
  commit protocol in the `_delta_log` directory, which leaves governance to the platform.
- **Hidden partitioning and partition evolution.** Partition transforms (`month(ts)`,
  identity) live in metadata, so queries do not need to know the layout. The bronze table
  is partitioned by `identity(source_month)` for idempotent reloads. Silver/gold are
  partitioned by `pickup_month`.
- **AWS alignment.** Athena, EMR, Glue and Redshift Spectrum all support Iceberg, and S3
  Tables is Iceberg-native, so the local design carries over to AWS unchanged (README,
  "Mapping to AWS").

## Consequences

- Table maintenance is our job: compaction, snapshot expiry, orphan cleanup. That is why
  `make maintain`, the Spark procedures and the weekly Dagster job exist.
- Merge-on-read deletes (from Trino MERGE) make writes cheap and reads more expensive until
  compaction runs. The demo shows this: delete files appear after the MERGE and `optimize`
  removes them.
- PyIceberg still rewrites with copy-on-write for deletes and overwrites. That is fine for
  month-scoped bronze reloads.
