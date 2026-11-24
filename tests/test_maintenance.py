import pytest

from lakehouse.maintenance import maintenance_statements, validate_table
from lakehouse.trino_client import render_table


def test_statements_in_order():
    stmts = maintenance_statements("lh.bronze.t", "7d", "128MB")
    assert [s.split("EXECUTE ")[1].split("(")[0] for s in stmts] == [
        "optimize",
        "optimize_manifests",
        "expire_snapshots",
        "remove_orphan_files",
    ]
    assert "retention_threshold => '7d'" in stmts[2]


@pytest.mark.parametrize(("retention", "size"), [("7 days", "128MB"), ("7d", "128 MB"), ("1d'; --", "1GB")])
def test_rejects_invalid_parameters(retention, size):
    with pytest.raises(ValueError):
        maintenance_statements("lh.bronze.t", retention, size)


@pytest.mark.parametrize("name", ["bronze", "bronze.t.x", "Bronze.T", "bronze.t;drop"])
def test_validate_table(name):
    with pytest.raises(ValueError):
        validate_table(name)


def test_render_table():
    out = render_table(["name", "n"], [("a", 12345), ("bb", None)])
    lines = out.splitlines()
    assert lines[0].startswith("name")
    assert "12,345" in lines[2]
    assert "NULL" in lines[3]
    assert lines[-1] == "(2 rows)"
