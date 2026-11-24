# ADR 0002: Lakekeeper as the Iceberg REST catalog

- Status: accepted
- Date: 2026-11-24

## Context

Iceberg needs a catalog to hold the pointer to each table's current metadata file. The
options we considered for a local-first stack that maps cleanly to production:

| Option | Notes |
|---|---|
| Apache Polaris | Apache project, REST, RBAC. Heavier JVM service; needs realm/principal bootstrapping and, for MinIO, careful STS setup. |
| Project Nessie | Git-like branches over Iceberg. Development has slowed since its sponsor moved focus to Polaris. |
| `apache/iceberg-rest-fixture` | Reference implementation meant for tests, not a production service. |
| Hive Metastore / JDBC | Not the REST protocol, so clients need engine-specific configuration and no credential vending. |
| **Lakekeeper** | Apache-2.0, a single Rust binary backed by Postgres, implements the REST spec including STS credential vending for S3-compatible stores. |

## Decision

Use Lakekeeper `v0.15.1` (pinned) with Postgres 17 for its state, and enable STS
credential vending against MinIO.

## Rationale

- **Credential vending.** Lakekeeper holds a least-privilege MinIO identity (bucket-scoped
  policy, created by `infra/minio/init.sh`) and returns short-lived STS credentials with
  every `loadTable`. Trino, DuckDB, PyIceberg and Spark run with **no S3 keys
  configured**. That is the same model as AWS Lake Formation or S3 Tables vending scoped
  credentials, and it keeps long-lived secrets out of the query engines.
- **Footprint.** About 45 MB RSS in this stack, against roughly 1 GB for a JVM catalog. That
  matters because the default stack has to run on a laptop.
- **Spec compliance.** It works unmodified with Trino's `rest` catalog, PyIceberg, DuckDB's
  `iceberg` extension (`ATTACH ... (TYPE iceberg)`) and Spark's `RESTCatalog`. All four
  are exercised by `make demo` and the CI smoke test.
- **Operational shape.** It is stateless with Postgres as the source of truth, emits
  Prometheus metrics and audit logs, and can add OpenID/OpenFGA authorization later
  without client changes.

## Consequences

- Authentication is disabled locally (`security=NONE`). In production you would put
  Lakekeeper behind OIDC and enable the OpenFGA authorizer.
- Moving to AWS Glue's Iceberg REST endpoint or S3 Tables only changes catalog
  properties. Table data and metadata stay portable Iceberg files.
