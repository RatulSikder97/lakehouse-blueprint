"""Thin Trino DB-API helpers plus a plain-text table renderer used by the demo."""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import trino

from lakehouse.config import Settings


def connect(settings: Settings | None = None, schema: str | None = None) -> trino.dbapi.Connection:
    settings = settings or Settings()
    return trino.dbapi.connect(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
        schema=schema,
        http_scheme="http",
    )


def query(conn: trino.dbapi.Connection, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [d[0] for d in (cur.description or [])]
    return columns, [tuple(r) for r in rows]


def scalar(conn: trino.dbapi.Connection, sql: str) -> Any:
    _, rows = query(conn, sql)
    return rows[0][0] if rows else None


def wait_until_ready(settings: Settings | None = None, timeout_s: int = 180) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            scalar(connect(settings), "SELECT 1")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise TimeoutError(f"Trino not ready after {timeout_s}s: {last_error}")


def _fmt(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, Decimal):
        return f"{value:,.2f}"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value:,}" if abs(value) >= 10_000 and abs(value) < 10**15 else str(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def render_table(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    """Render rows as a psql-style text table."""
    cells = [[_fmt(v) for v in row] for row in rows]
    widths = [len(c) for c in columns]
    for row in cells:
        widths = [max(w, len(v)) for w, v in zip(widths, row, strict=True)]
    header = " | ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True))
    sep = "-+-".join("-" * w for w in widths)
    body = [
        " | ".join(v.rjust(w) if _is_num(v) else v.ljust(w) for v, w in zip(row, widths, strict=True))
        for row in cells
    ]
    footer = f"({len(rows)} row{'s' if len(rows) != 1 else ''})"
    return "\n".join([header, sep, *body, footer])


def _is_num(text: str) -> bool:
    return text.replace(",", "").replace(".", "", 1).lstrip("-").isdigit()
