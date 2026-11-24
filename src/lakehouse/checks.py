"""Soda-style declarative data quality checks, executed in Trino (or any DB-API engine).

Syntax (subset of SodaCL):

    checks for silver.trips:
      - row_count > 1000
      - missing_count(trip_id) = 0
      - duplicate_count(trip_id) = 0
      - min(fare_amount) >= 0
      - freshness(_ingested_at) < 1d
      - avg(tip_amount) < 10:
          severity: warn
"""

from __future__ import annotations

import operator
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_CHECK = re.compile(
    r"^(?P<metric>row_count|missing_count|duplicate_count|min|max|avg|sum|freshness)"
    r"(?:\((?P<column>[a-z_][a-z0-9_]*)\))?\s*(?P<op><=|>=|!=|=|<|>)\s*(?P<threshold>\S+)$"
)
_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
_DURATION = re.compile(r"^(?P<n>\d+)(?P<unit>[smhd])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_OPS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "=": operator.eq,
    "!=": operator.ne,
}


@dataclass(frozen=True)
class Check:
    table: str
    metric: str
    column: str | None
    op: str
    threshold: float
    severity: str = "fail"
    raw: str = ""

    @property
    def name(self) -> str:
        return self.raw or f"{self.metric}({self.column}) {self.op} {self.threshold}"


@dataclass(frozen=True)
class CheckResult:
    check: Check
    value: float | None
    passed: bool


def parse_threshold(metric: str, text: str) -> float:
    if metric == "freshness":
        m = _DURATION.match(text)
        if not m:
            raise ValueError(f"freshness threshold must be a duration like 6h or 1d, got {text!r}")
        return float(int(m["n"]) * _UNIT_SECONDS[m["unit"]])
    return float(text)


def parse_check(table: str, expr: str, severity: str = "fail") -> Check:
    m = _CHECK.match(expr.strip())
    if not m:
        raise ValueError(f"unsupported check expression: {expr!r}")
    metric, column = m["metric"], m["column"]
    if metric != "row_count" and column is None:
        raise ValueError(f"{metric} requires a column: {expr!r}")
    if severity not in ("fail", "warn"):
        raise ValueError(f"severity must be fail or warn, got {severity!r}")
    return Check(
        table, metric, column, m["op"], parse_threshold(metric, m["threshold"]), severity, expr.strip()
    )


def load_checks(source: str | Path | dict[str, Any]) -> list[Check]:
    doc = source if isinstance(source, dict) else yaml.safe_load(Path(source).read_text())
    checks: list[Check] = []
    for key, items in doc.items():
        if not key.startswith("checks for "):
            raise ValueError(f"top-level keys must be 'checks for <schema.table>', got {key!r}")
        table = key.removeprefix("checks for ").strip()
        if not _TABLE.match(table):
            raise ValueError(f"invalid table name {table!r}")
        for item in items:
            if isinstance(item, str):
                checks.append(parse_check(table, item))
            elif isinstance(item, dict) and len(item) == 1:
                ((expr, opts),) = item.items()
                checks.append(parse_check(table, expr, (opts or {}).get("severity", "fail")))
            else:
                raise ValueError(f"invalid check entry under {table}: {item!r}")
    return checks


def metric_sql(check: Check, catalog: str) -> str:
    fq = f"{catalog}.{check.table}"
    col = check.column
    match check.metric:
        case "row_count":
            return f"SELECT count(*) FROM {fq}"
        case "missing_count":
            return f"SELECT count_if({col} IS NULL) FROM {fq}"
        case "duplicate_count":
            return (
                f"SELECT coalesce(sum(n - 1), 0) FROM "
                f"(SELECT {col}, count(*) AS n FROM {fq} WHERE {col} IS NOT NULL "
                f"GROUP BY {col} HAVING count(*) > 1)"
            )
        case "freshness":
            return f"SELECT date_diff('second', max({col}), current_timestamp) FROM {fq}"
        case "min" | "max" | "avg" | "sum":
            return f"SELECT CAST({check.metric}({col}) AS double) FROM {fq}"
    raise ValueError(check.metric)


def evaluate(check: Check, value: float | None) -> bool:
    if value is None:
        return False
    return _OPS[check.op](float(value), check.threshold)


def run_checks(conn, checks: list[Check], catalog: str) -> list[CheckResult]:
    from lakehouse.trino_client import scalar

    results = []
    for check in checks:
        raw = scalar(conn, metric_sql(check, catalog))
        value = None if raw is None else float(raw)
        results.append(CheckResult(check, value, evaluate(check, value)))
    return results
