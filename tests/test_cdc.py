from datetime import UTC, datetime

import pyarrow.compute as pc
import pytest

from lakehouse.cdc import ChangeSpec, build_change_batch, build_merge_sql

CHANGED_AT = datetime(2026, 2, 1, tzinfo=UTC)
SPEC = ChangeSpec(updates=40, deletes=10, inserts=20, double_updates=5, seed=7)


@pytest.fixture
def base(bronze_2025_01):
    return bronze_2025_01.select([c for c in bronze_2025_01.column_names if not c.startswith("_")])


def test_change_batch_counts(base):
    feed = build_change_batch(base, spec=SPEC, batch_id="b1", changed_at=CHANGED_AT)
    ops = pc.value_counts(feed.column("op")).to_pylist()
    assert {o["values"]: o["counts"] for o in ops} == {"U": 45, "D": 10, "I": 20}
    assert feed.column_names[:4] == ["change_id", "op", "changed_at", "change_batch_id"]
    assert len(set(feed.column("change_id").to_pylist())) == feed.num_rows


def test_change_batch_is_deterministic(base):
    a = build_change_batch(base, spec=SPEC, batch_id="b1", changed_at=CHANGED_AT)
    b = build_change_batch(base, spec=SPEC, batch_id="b1", changed_at=CHANGED_AT)
    assert a.equals(b)


def test_updates_and_deletes_target_existing_keys_inserts_are_new(base):
    feed = build_change_batch(base, spec=SPEC, batch_id="b1", changed_at=CHANGED_AT).to_pylist()
    existing = set(base.column("trip_id").to_pylist())
    assert all(r["trip_id"] in existing for r in feed if r["op"] in ("U", "D"))
    assert all(
        r["trip_id"].startswith("late-b1-") and r["trip_id"] not in existing for r in feed if r["op"] == "I"
    )
    updated = {r["trip_id"] for r in feed if r["op"] == "U"}
    deleted = {r["trip_id"] for r in feed if r["op"] == "D"}
    assert not updated & deleted


def test_double_updates_have_later_timestamp(base):
    feed = build_change_batch(base, spec=SPEC, batch_id="b1", changed_at=CHANGED_AT).to_pylist()
    by_trip: dict[str, list] = {}
    for r in feed:
        if r["op"] == "U":
            by_trip.setdefault(r["trip_id"], []).append(r)
    twice = [v for v in by_trip.values() if len(v) == 2]
    assert len(twice) == SPEC.double_updates
    for first, second in twice:
        assert second["changed_at"] > first["changed_at"]
        assert second["tip_amount"] == pytest.approx(first["tip_amount"] + 0.25)


def test_change_batch_rejects_small_sample(base):
    with pytest.raises(ValueError, match="too small"):
        build_change_batch(base.slice(0, 10), spec=SPEC, batch_id="b", changed_at=CHANGED_AT)


def test_merge_sql_shape():
    sql = build_merge_sql(
        "lh.bronze.t",
        "lh.bronze.c",
        ["trip_id", "fare_amount", "new_col", "_ingested_at", "_batch_id", "_source_file"],
        ["change_id", "op", "changed_at", "change_batch_id", "trip_id", "fare_amount"],
        "batch-1",
    )
    assert "MERGE INTO lh.bronze.t t" in sql
    assert "WHERE change_batch_id = 'batch-1'" in sql
    assert "PARTITION BY trip_id ORDER BY changed_at DESC, change_id DESC" in sql
    assert "WHEN MATCHED AND s.op = 'D' THEN DELETE" in sql
    assert "new_col = NULL" in sql
    assert "_ingested_at = s.changed_at" in sql
    assert "trip_id = s.trip_id" in sql.split("UPDATE SET")[0]
    assert "trip_id = " not in sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]


@pytest.mark.parametrize("bad", [("trip_id; drop table x",), ("Trip_Id",)])
def test_merge_sql_rejects_unsafe_identifiers(bad):
    with pytest.raises(ValueError):
        build_merge_sql("t", "c", list(bad), [], "b")


def test_merge_sql_rejects_quoted_batch_id():
    with pytest.raises(ValueError):
        build_merge_sql("t", "c", ["trip_id"], ["trip_id"], "x' OR '1'='1")
