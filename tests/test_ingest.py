from datetime import UTC, datetime

import pyarrow as pa
import pytest

from lakehouse.config import Settings
from lakehouse.ingest import align_to_schema, canonical_name, normalize, source_path, validate_month

from .conftest import FIXTURES, NOW


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("VendorID", "vendor_id"),
        ("tpep_pickup_datetime", "pickup_at"),
        ("PULocationID", "pu_location_id"),
        ("Airport_fee", "airport_fee"),
        ("airport_fee", "airport_fee"),
        ("cbd_congestion_fee", "cbd_congestion_fee"),
        ("Some New-Column", "some_new_column"),
    ],
)
def test_canonical_name(raw, expected):
    assert canonical_name(raw) == expected


@pytest.mark.parametrize("month", ["2025-1", "2025-13", "25-01", "2025-01-01"])
def test_validate_month_rejects_bad_input(month):
    with pytest.raises(ValueError):
        validate_month(month)


def test_normalize_shapes_bronze(raw_2024_12):
    out = normalize(
        raw_2024_12, source_month="2024-12", source_file="f.parquet", batch_id="b", ingested_at=NOW
    )
    assert out.num_rows == raw_2024_12.num_rows
    assert out.column_names[0] == "trip_id"
    assert out.schema.field("trip_id").nullable is False
    assert out.schema.field("passenger_count").type == pa.int32()
    assert out.schema.field("payment_type").type == pa.int32()
    assert out.schema.field("store_and_fwd_flag").type == pa.string()
    assert out.schema.field("pickup_at").type == pa.timestamp("us")
    assert out.schema.field("_ingested_at").type == pa.timestamp("us", tz="UTC")
    assert "airport_fee" in out.column_names and "Airport_fee" not in out.column_names
    assert out.column("source_month").unique().to_pylist() == ["2024-12"]


def test_trip_id_is_stable_and_unique(raw_2024_12):
    kwargs = dict(source_month="2024-12", source_file="f", batch_id="b", ingested_at=NOW)
    first = normalize(raw_2024_12, **kwargs).column("trip_id")
    second = normalize(raw_2024_12, **{**kwargs, "batch_id": "other", "ingested_at": datetime.now(UTC)})
    assert first.to_pylist() == second.column("trip_id").to_pylist()
    assert len(set(first.to_pylist())) == len(first)
    assert first[0].as_py() == "2024-12-00000000"


def test_new_source_column_passes_through(bronze_2025_01):
    assert bronze_2025_01.schema.field("cbd_congestion_fee").type == pa.float64()


def test_normalize_rejects_colliding_columns():
    raw = pa.table({"Airport_fee": [1.0], "airport_fee": [2.0]})
    with pytest.raises(ValueError, match="collision"):
        normalize(raw, source_month="2024-01", source_file="f", batch_id="b", ingested_at=NOW)


def test_align_adds_missing_optional_columns(raw_2024_12, bronze_2025_01):
    old = normalize(raw_2024_12, source_month="2024-12", source_file="f", batch_id="b", ingested_at=NOW)
    target = bronze_2025_01.schema
    aligned = align_to_schema(old, target)
    assert aligned.schema == target
    assert aligned.column("cbd_congestion_fee").null_count == old.num_rows


def test_align_rejects_unknown_columns(bronze_2025_01):
    narrower = pa.schema([f for f in bronze_2025_01.schema if f.name != "cbd_congestion_fee"])
    with pytest.raises(ValueError, match="not in target"):
        align_to_schema(bronze_2025_01, narrower)


def test_align_rejects_missing_required_column(bronze_2025_01):
    target = pa.schema([pa.field("must_exist", pa.string(), nullable=False), *bronze_2025_01.schema])
    with pytest.raises(ValueError, match="required"):
        align_to_schema(bronze_2025_01, target)


def test_source_path_uses_local_source_dir(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_SOURCE_DIR", str(FIXTURES))
    assert source_path(Settings(), "2025-01").name == "yellow_tripdata_2025-01.parquet"
    with pytest.raises(FileNotFoundError):
        source_path(Settings(), "1999-01")


def test_row_offset_keeps_trip_id_stable_across_row_groups(raw_2024_12):
    kwargs = dict(source_month="2024-12", source_file="f", batch_id="b", ingested_at=NOW)
    whole = normalize(raw_2024_12, **kwargs).column("trip_id").to_pylist()
    head = normalize(raw_2024_12.slice(0, 100), **kwargs).column("trip_id").to_pylist()
    tail = normalize(raw_2024_12.slice(100), row_offset=100, **kwargs).column("trip_id").to_pylist()
    assert head + tail == whole


def test_empty_table_yields_bronze_schema(raw_2025_01):
    schema = normalize(
        raw_2025_01.schema.empty_table(),
        source_month="2025-01",
        source_file="f",
        batch_id="b",
        ingested_at=NOW,
    ).schema
    assert schema.names[0] == "trip_id" and "cbd_congestion_fee" in schema.names
