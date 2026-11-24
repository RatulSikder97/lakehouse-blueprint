import pytest

from lakehouse.checks import evaluate, load_checks, metric_sql, parse_check

DOC = {
    "checks for silver.trips": [
        "row_count > 1000",
        "missing_count(trip_id) = 0",
        "duplicate_count(trip_id) = 0",
        "freshness(ingested_at) < 1d",
        {"avg(trip_miles) < 10": {"severity": "warn"}},
    ]
}


def test_load_checks_parses_all_forms():
    checks = load_checks(DOC)
    assert [c.metric for c in checks] == ["row_count", "missing_count", "duplicate_count", "freshness", "avg"]
    assert checks[3].threshold == 86400
    assert checks[4].severity == "warn"
    assert all(c.table == "silver.trips" for c in checks)


def test_repo_checks_file_is_valid():
    assert len(load_checks("quality/checks.yml")) >= 10


@pytest.mark.parametrize(
    "expr",
    ["row_count >> 1", "missing_count = 0", "drop_table(x) = 0", "min(Bad-Col) > 0", "freshness(c) < 1 week"],
)
def test_parse_check_rejects_invalid(expr):
    with pytest.raises(ValueError):
        parse_check("s.t", expr)


def test_load_checks_rejects_bad_table_name():
    with pytest.raises(ValueError):
        load_checks({"checks for trips; drop": ["row_count > 0"]})


@pytest.mark.parametrize(
    ("expr", "value", "passed"),
    [
        ("row_count > 10", 11, True),
        ("row_count > 10", 10, False),
        ("missing_count(a) = 0", 0, True),
        ("min(a) >= 0", -0.01, False),
        ("freshness(a) < 6h", 21599, True),
        ("max(a) != 3", 3, False),
        ("avg(a) <= 2", None, False),
    ],
)
def test_evaluate(expr, value, passed):
    assert evaluate(parse_check("s.t", expr), value) is passed


def test_metric_sql():
    assert metric_sql(parse_check("s.t", "row_count > 0"), "lh") == "SELECT count(*) FROM lh.s.t"
    assert "count_if(a IS NULL)" in metric_sql(parse_check("s.t", "missing_count(a) = 0"), "lh")
    assert "HAVING count(*) > 1" in metric_sql(parse_check("s.t", "duplicate_count(a) = 0"), "lh")
    assert "date_diff('second', max(a), current_timestamp)" in metric_sql(
        parse_check("s.t", "freshness(a) < 1d"), "lh"
    )
