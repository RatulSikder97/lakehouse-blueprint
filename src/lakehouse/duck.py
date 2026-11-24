"""DuckDB as the lightweight engine: attach the Iceberg REST catalog and query in-process."""

from __future__ import annotations

import duckdb

from lakehouse.config import Settings


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(settings: Settings | None = None, alias: str = "lake") -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with the warehouse attached as `alias`.

    Lakekeeper vends S3 credentials per table, so no DuckDB S3 secret is needed.
    """
    settings = settings or Settings()
    if not alias.isidentifier():
        raise ValueError(f"invalid alias {alias!r}")
    conn = duckdb.connect()
    for ext in ("httpfs", "iceberg"):
        conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")
    conn.execute(
        f"ATTACH {_quote(settings.warehouse)} AS {alias} "
        f"(TYPE iceberg, ENDPOINT {_quote(settings.catalog_uri)}, AUTHORIZATION_TYPE 'none')"
    )
    return conn


def query(conn: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[tuple]]:
    rel = conn.execute(sql)
    columns = [d[0] for d in rel.description or []]
    return columns, [tuple(r) for r in rel.fetchall()]
