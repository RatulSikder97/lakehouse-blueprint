"""Bronze ingestion: NYC TLC yellow-taxi monthly parquet -> Iceberg `bronze.yellow_trips`.

Design points:
* Idempotent per source month: a re-run replaces the month atomically (`overwrite` with a
  row filter on the identity-partitioned `source_month` column) instead of duplicating it.
* Schema drift is absorbed, not rejected: column names are canonicalised (TLC flips between
  `Airport_fee` and `airport_fee`), types are pinned, and genuinely new columns
  (e.g. `cbd_congestion_fee`, introduced January 2025) are added via `union_by_name`.
* Every row carries a stable business key (`trip_id`) and lineage metadata.
"""

from __future__ import annotations

import logging
import re
import time
import urllib.request
import warnings
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

from lakehouse.catalog import ensure_namespace
from lakehouse.config import Settings

log = logging.getLogger(__name__)

BRONZE_NAMESPACE = "bronze"
TRIPS_TABLE = f"{BRONZE_NAMESPACE}.yellow_trips"
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

COLUMN_RENAMES = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_at",
    "tpep_dropoff_datetime": "dropoff_at",
    "ratecodeid": "ratecode_id",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
}

# Canonical types for known columns. Unknown (new) columns keep their source type,
# with large_string / timestamp[ns] normalised so every engine can read them.
CANONICAL_TYPES: dict[str, pa.DataType] = {
    "vendor_id": pa.int32(),
    "pickup_at": pa.timestamp("us"),
    "dropoff_at": pa.timestamp("us"),
    "passenger_count": pa.int32(),
    "trip_distance": pa.float64(),
    "ratecode_id": pa.int32(),
    "store_and_fwd_flag": pa.string(),
    "pu_location_id": pa.int32(),
    "do_location_id": pa.int32(),
    "payment_type": pa.int32(),
    "fare_amount": pa.float64(),
    "extra": pa.float64(),
    "mta_tax": pa.float64(),
    "tip_amount": pa.float64(),
    "tolls_amount": pa.float64(),
    "improvement_surcharge": pa.float64(),
    "total_amount": pa.float64(),
    "congestion_surcharge": pa.float64(),
    "airport_fee": pa.float64(),
}

METADATA_COLUMNS = ("source_month", "_source_file", "_batch_id", "_ingested_at")

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}


@dataclass
class IngestResult:
    month: str
    rows: int
    snapshot_id: int | None
    added_columns: list[str] = field(default_factory=list)
    seconds: float = 0.0


def validate_month(month: str) -> str:
    if not MONTH_RE.match(month):
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    return month


def canonical_name(name: str) -> str:
    """Canonicalise a TLC column name: lower snake_case plus explicit renames."""
    snake = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    return COLUMN_RENAMES.get(snake, snake)


def _normalise_type(dtype: pa.DataType) -> pa.DataType:
    if pa.types.is_large_string(dtype):
        return pa.string()
    if pa.types.is_timestamp(dtype):
        return pa.timestamp("us", tz=dtype.tz)
    if pa.types.is_dictionary(dtype):
        return _normalise_type(dtype.value_type)
    return dtype


def normalize(
    raw: pa.Table,
    *,
    source_month: str,
    source_file: str,
    batch_id: str,
    ingested_at: datetime,
    row_offset: int = 0,
) -> pa.Table:
    """Return the bronze-shaped table for (a slice of) one raw TLC file.

    `row_offset` is the position of the slice's first row in the source file, so that
    `trip_id` stays stable when a file is processed row group by row group.
    """
    validate_month(source_month)
    names = [canonical_name(n) for n in raw.column_names]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        raise ValueError(f"column name collision after canonicalisation: {sorted(duplicates)}")
    table = raw.rename_columns(names)

    columns: list[pa.Array | pa.ChunkedArray] = []
    fields: list[pa.Field] = []
    for name in table.column_names:
        col = table.column(name)
        target = CANONICAL_TYPES.get(name, _normalise_type(col.type))
        if col.type != target:
            # safe=False: TLC stores small integer codes as int64/double; values fit in int32.
            col = pc.cast(col, target, safe=False)
        columns.append(col)
        fields.append(pa.field(name, target, nullable=True))

    n = table.num_rows
    # Stable business key: source month + row position in the immutable monthly file.
    positions = pc.cast(pa.array(range(row_offset, row_offset + n), type=pa.int64()), pa.string())
    trip_id = pc.binary_join_element_wise(
        pa.array([source_month] * n, pa.string()), pc.utf8_lpad(positions, 8, "0"), "-"
    )

    ingested = pa.scalar(ingested_at.astimezone(UTC), type=pa.timestamp("us", tz="UTC"))
    meta_arrays = [
        pa.array([source_month] * n, pa.string()),
        pa.array([source_file] * n, pa.string()),
        pa.array([batch_id] * n, pa.string()),
        pa.array([ingested.as_py()] * n, pa.timestamp("us", tz="UTC")),
    ]
    schema = pa.schema(
        [pa.field("trip_id", pa.string(), nullable=False), *fields]
        + [pa.field(m, a.type, nullable=True) for m, a in zip(METADATA_COLUMNS, meta_arrays, strict=True)]
    )
    return pa.Table.from_arrays([trip_id, *columns, *meta_arrays], schema=schema)


def align_to_schema(table: pa.Table, target: pa.Schema) -> pa.Table:
    """Reorder/extend `table` so it matches `target` (missing optional columns become nulls)."""
    arrays = []
    for f in target:
        if f.name in table.column_names:
            col = table.column(f.name)
            arrays.append(col if col.type == f.type else pc.cast(col, f.type))
        else:
            if not f.nullable:
                raise ValueError(f"required column {f.name!r} missing from batch")
            arrays.append(pa.nulls(table.num_rows, type=f.type))
    extra = set(table.column_names) - set(target.names)
    if extra:
        raise ValueError(f"batch has columns not in target schema: {sorted(extra)}")
    return pa.Table.from_arrays(arrays, schema=target)


def source_path(settings: Settings, month: str) -> Path:
    """Locate (downloading if needed) the raw parquet file for `month`."""
    filename = f"yellow_tripdata_{month}.parquet"
    if settings.source_dir is not None:
        path = settings.source_dir / filename
        if not path.exists():
            raise FileNotFoundError(path)
        return path
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    path = settings.cache_dir / filename
    if path.exists() and _is_parquet(path):
        return path
    url = f"{settings.source_base_url}/{filename}"
    tmp = path.with_suffix(".part")
    for attempt in range(1, 5):
        try:
            log.info("downloading %s (attempt %d)", url, attempt)
            req = urllib.request.Request(url, headers={"User-Agent": "lakehouse-blueprint/0.1"})
            with urllib.request.urlopen(req, timeout=120) as resp, tmp.open("wb") as out:
                while chunk := resp.read(1 << 20):
                    out.write(chunk)
            if not _is_parquet(tmp):
                raise OSError(f"{url} did not return a parquet file")
            tmp.replace(path)
            return path
        except OSError as exc:
            log.warning("download failed: %s", exc)
            time.sleep(2**attempt)
    raise RuntimeError(f"could not download {url}")


def _is_parquet(path: Path) -> bool:
    if path.stat().st_size < 12:
        return False
    with path.open("rb") as fh:
        head = fh.read(4)
        fh.seek(-4, 2)
        tail = fh.read(4)
    return head == b"PAR1" and tail == b"PAR1"


def ensure_trips_table(catalog: Catalog, schema: pa.Schema) -> Table:
    ensure_namespace(catalog, BRONZE_NAMESPACE)
    if catalog.table_exists(TRIPS_TABLE):
        return catalog.load_table(TRIPS_TABLE)
    table = catalog.create_table(TRIPS_TABLE, schema=schema, properties=TABLE_PROPERTIES)
    source_id = table.schema().find_field("source_month").field_id
    with table.update_spec() as spec:
        spec.add_field("source_month", IdentityTransform(), "source_month")
    log.info("created %s partitioned by identity(source_month) (field %d)", TRIPS_TABLE, source_id)
    return catalog.load_table(TRIPS_TABLE)


def evolve_schema(table: Table, batch_schema: pa.Schema) -> list[str]:
    """Add any new batch columns to the table schema. Returns the added column names."""
    existing = {f.name for f in table.schema().fields}
    added = [n for n in batch_schema.names if n not in existing]
    if added:
        with table.update_schema() as update:
            update.union_by_name(batch_schema)
        log.info("schema evolution on %s: added %s", TRIPS_TABLE, added)
    return added


def ingest_month(
    catalog: Catalog,
    settings: Settings,
    month: str,
    *,
    limit: int | None = None,
    now: datetime | None = None,
) -> IngestResult:
    """Replace one source month in bronze, streaming the file row group by row group.

    The delete of the previous load and every append commit in a single Iceberg
    transaction, so readers see either the old month or the new month, never a mix.
    Peak memory is one row group (~1M rows) rather than the whole file.
    """
    started = time.monotonic()
    validate_month(month)
    path = source_path(settings, month)
    source = pq.ParquetFile(path)
    now = now or datetime.now(UTC)
    batch_id = f"ingest-{month}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    meta = dict(source_month=month, source_file=path.name, batch_id=batch_id, ingested_at=now)

    # Evolve the table schema up front from the file schema (zero rows).
    file_schema = normalize(source.schema_arrow.empty_table(), **meta).schema
    table = ensure_trips_table(catalog, file_schema)
    added = evolve_schema(table, file_schema)
    table = catalog.load_table(TRIPS_TABLE)
    target = table.schema().as_arrow()

    rows = 0
    remaining = limit
    with warnings.catch_warnings():
        # Expected: PyIceberg deletes with copy-on-write, and a first load matches nothing.
        warnings.filterwarnings("ignore", message="Merge on read is not yet supported")
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        with table.transaction() as tx:
            tx.delete(EqualTo("source_month", month))
            for i in range(source.num_row_groups):
                if remaining is not None and remaining <= 0:
                    break
                raw = source.read_row_group(i)
                if remaining is not None:
                    raw = raw.slice(0, remaining)
                    remaining -= raw.num_rows
                batch = normalize(raw, row_offset=rows, **meta)
                tx.append(align_to_schema(batch, target))
                rows += raw.num_rows
                del raw, batch

    snapshot = catalog.load_table(TRIPS_TABLE).current_snapshot()
    return IngestResult(
        month=month,
        rows=rows,
        snapshot_id=snapshot.snapshot_id if snapshot else None,
        added_columns=added,
        seconds=time.monotonic() - started,
    )
