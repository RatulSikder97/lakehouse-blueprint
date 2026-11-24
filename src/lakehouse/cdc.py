"""CDC-style incremental load: a change feed applied to bronze with an Iceberg MERGE.

The TLC dataset is a static monthly dump, so we simulate what an upstream OLTP system's
Debezium-style feed would deliver: late-arriving trips (I), fare/tip corrections (U, some
of them corrected twice) and voided trips (D). The feed is landed append-only in
`bronze.yellow_trips_changes` and applied to `bronze.yellow_trips` with a single
`MERGE INTO` executed by Trino. Each apply is recorded in `ops.cdc_merge_log` with the
before/after snapshot ids, which is what makes the time-travel demo reproducible.
"""

from __future__ import annotations

import logging
import random
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo

from lakehouse.catalog import ensure_namespace
from lakehouse.ingest import BRONZE_NAMESPACE, TABLE_PROPERTIES, TRIPS_TABLE, align_to_schema, validate_month

log = logging.getLogger(__name__)

CHANGES_TABLE = f"{BRONZE_NAMESPACE}.yellow_trips_changes"
MERGE_LOG_TABLE = "ops.cdc_merge_log"
CHANGE_COLUMNS = ("change_id", "op", "changed_at", "change_batch_id")
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class ChangeSpec:
    updates: int = 5_000
    deletes: int = 1_000
    inserts: int = 2_000
    double_updates: int = 250
    seed: int = 42


@dataclass
class MergeResult:
    batch_id: str
    changes: int
    inserted: int
    updated: int
    deleted: int
    snapshot_before: int
    snapshot_after: int


def build_change_batch(base: pa.Table, *, spec: ChangeSpec, batch_id: str, changed_at: datetime) -> pa.Table:
    """Derive a deterministic change feed from a sample of current bronze rows.

    `base` must contain `trip_id`, `pickup_at`, `dropoff_at`, `tip_amount`, `total_amount`.
    """
    base = _plain_strings(base)
    n = base.num_rows
    if spec.updates + spec.deletes > n:
        raise ValueError(f"sample of {n} rows too small for {spec.updates} updates + {spec.deletes} deletes")
    if spec.double_updates > spec.updates:
        raise ValueError("double_updates cannot exceed updates")
    rng = random.Random(spec.seed)
    picked = rng.sample(range(n), spec.updates + spec.deletes)
    upd_idx, del_idx = picked[: spec.updates], picked[spec.updates :]
    ins_idx = [rng.randrange(n) for _ in range(spec.inserts)]
    changed_at = changed_at.astimezone(UTC)

    parts: list[pa.Table] = []

    # Updates: tip correction, total recomputed. Amounts are rounded to cents.
    upd = base.take(pa.array(upd_idx, pa.int64()))
    deltas = pa.array([round(rng.uniform(0.5, 5.0), 2) for _ in upd_idx], pa.float64())
    upd = _with_amounts(upd, deltas)
    parts.append(_tag(upd, "U", changed_at, batch_id))

    # A subset is corrected twice; only the latest image must win in the MERGE.
    if spec.double_updates:
        again = upd.slice(0, spec.double_updates)
        bump = pa.array([0.25] * spec.double_updates, pa.float64())
        parts.append(_tag(_with_amounts(again, bump), "U", changed_at + timedelta(seconds=1), batch_id))

    # Deletes: voided trips. Row image carried for auditability.
    parts.append(_tag(base.take(pa.array(del_idx, pa.int64())), "D", changed_at, batch_id))

    # Inserts: late-arriving trips with fresh keys, shifted 3 minutes later.
    ins = base.take(pa.array(ins_idx, pa.int64()))
    late_ids = pa.array([f"late-{batch_id}-{i:06d}" for i in range(spec.inserts)], pa.string())
    ins = ins.set_column(ins.schema.get_field_index("trip_id"), ins.schema.field("trip_id"), late_ids)
    shift = pa.scalar(timedelta(minutes=3), pa.duration("us"))
    for col in ("pickup_at", "dropoff_at"):
        i = ins.schema.get_field_index(col)
        ins = ins.set_column(i, ins.schema.field(col), pc.add(ins.column(col), shift))
    parts.append(_tag(ins, "I", changed_at, batch_id))

    feed = pa.concat_tables(parts, promote_options="default")
    change_ids = pa.array([f"{batch_id}-{i:07d}" for i in range(feed.num_rows)], pa.string())
    return feed.set_column(0, pa.field("change_id", pa.string(), nullable=False), change_ids)


def _plain_strings(table: pa.Table) -> pa.Table:
    """PyIceberg scans return large_string; use plain string so generated columns line up."""
    schema = pa.schema(
        [f.with_type(pa.string()) if pa.types.is_large_string(f.type) else f for f in table.schema]
    )
    return table.cast(schema)


def _with_amounts(table: pa.Table, tip_delta: pa.Array) -> pa.Table:
    tip = pc.round(pc.add(pc.fill_null(table.column("tip_amount"), 0.0), tip_delta), 2)
    total = pc.round(pc.add(pc.fill_null(table.column("total_amount"), 0.0), tip_delta), 2)
    table = table.set_column(table.schema.get_field_index("tip_amount"), "tip_amount", tip)
    return table.set_column(table.schema.get_field_index("total_amount"), "total_amount", total)


def _tag(table: pa.Table, op: str, changed_at: datetime, batch_id: str) -> pa.Table:
    n = table.num_rows
    prefix = pa.table(
        {
            "change_id": pa.array([""] * n, pa.string()),
            "op": pa.array([op] * n, pa.string()),
            "changed_at": pa.array([changed_at] * n, pa.timestamp("us", tz="UTC")),
            "change_batch_id": pa.array([batch_id] * n, pa.string()),
        }
    )
    for name in table.column_names:
        prefix = prefix.append_column(table.schema.field(name), table.column(name))
    return prefix


def _check_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


def build_merge_sql(
    target: str, changes: str, target_columns: Sequence[str], change_columns: Sequence[str], batch_id: str
) -> str:
    """Build the MERGE statement applying one change batch (latest image per key wins)."""
    if "'" in batch_id:
        raise ValueError("batch_id must not contain quotes")
    cols = [_check_ident(c) for c in target_columns]
    available = set(change_columns)
    lineage = {"_ingested_at": "s.changed_at", "_batch_id": "s.change_batch_id", "_source_file": "'cdc'"}
    values = {c: lineage.get(c, f"s.{c}" if c in available else "NULL") for c in cols}
    set_clause = ",\n        ".join(f"{c} = {values[c]}" for c in cols if c != "trip_id")
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(values[c] for c in cols)
    return f"""MERGE INTO {target} t
USING (
    SELECT * FROM (
        SELECT c.*, row_number() OVER (PARTITION BY trip_id ORDER BY changed_at DESC, change_id DESC) AS rn
        FROM {changes} c
        WHERE change_batch_id = '{batch_id}'
    ) WHERE rn = 1
) s
ON t.trip_id = s.trip_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET
        {set_clause}
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT ({insert_cols})
    VALUES ({insert_vals})"""


def land_changes(
    catalog: Catalog, month: str, *, spec: ChangeSpec, sample_rows: int = 200_000, now: datetime | None = None
) -> tuple[str, int]:
    """Generate a change batch from bronze rows of `month` and append it to the changes table."""
    validate_month(month)
    now = now or datetime.now(UTC)
    batch_id = f"cdc-{month}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    trips = catalog.load_table(TRIPS_TABLE)
    base = trips.scan(row_filter=EqualTo("source_month", month), limit=sample_rows).to_arrow()
    if base.num_rows == 0:
        raise RuntimeError(f"no bronze rows for {month}; run ingestion first")
    # Corrections only target regular paid trips (refund rows carry negative totals).
    base = base.filter(pc.greater(base.column("total_amount"), 0))
    data_cols = [c for c in base.column_names if not c.startswith("_")]
    feed = build_change_batch(base.select(data_cols), spec=spec, batch_id=batch_id, changed_at=now)

    ensure_namespace(catalog, BRONZE_NAMESPACE)
    if not catalog.table_exists(CHANGES_TABLE):
        catalog.create_table(CHANGES_TABLE, schema=feed.schema, properties=TABLE_PROPERTIES)
    changes = catalog.load_table(CHANGES_TABLE)
    if missing := [n for n in feed.schema.names if n not in {f.name for f in changes.schema().fields}]:
        with changes.update_schema() as update:
            update.union_by_name(feed.schema)
        log.info("schema evolution on %s: added %s", CHANGES_TABLE, missing)
        changes = catalog.load_table(CHANGES_TABLE)
    changes.append(align_to_schema(feed, changes.schema().as_arrow()))
    return batch_id, feed.num_rows


def apply_changes(conn, catalog_name: str, batch_id: str) -> MergeResult:
    """Run the MERGE in Trino and record the run in ops.cdc_merge_log."""
    from lakehouse.trino_client import query, scalar

    target = f"{catalog_name}.{TRIPS_TABLE}"
    changes = f"{catalog_name}.{CHANGES_TABLE}"
    target_cols = [r[0] for r in query(conn, f"DESCRIBE {target}")[1]]
    change_cols = [r[0] for r in query(conn, f"DESCRIBE {changes}")[1]]

    _, counts = query(
        conn,
        f"""
        WITH s AS (
            SELECT * FROM (
                SELECT trip_id, op,
                       row_number() OVER (PARTITION BY trip_id ORDER BY changed_at DESC, change_id DESC) AS rn
                FROM {changes} WHERE change_batch_id = '{batch_id}'
            ) WHERE rn = 1
        )
        SELECT
            count(*) AS changes,
            count_if(t.trip_id IS NULL AND s.op <> 'D') AS inserted,
            count_if(t.trip_id IS NOT NULL AND s.op <> 'D') AS updated,
            count_if(t.trip_id IS NOT NULL AND s.op = 'D') AS deleted
        FROM s LEFT JOIN {target} t ON t.trip_id = s.trip_id
        """,
    )
    total, inserted, updated, deleted = counts[0]
    before = scalar(
        conn,
        f'SELECT snapshot_id FROM {catalog_name}.bronze."yellow_trips$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1",
    )
    query(conn, build_merge_sql(target, changes, target_cols, change_cols, batch_id))
    after = scalar(
        conn,
        f'SELECT snapshot_id FROM {catalog_name}.bronze."yellow_trips$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1",
    )

    query(conn, f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.ops")
    query(
        conn,
        f"""CREATE TABLE IF NOT EXISTS {catalog_name}.{MERGE_LOG_TABLE} (
            batch_id varchar, applied_at timestamp(6) with time zone,
            changes bigint, inserted bigint, updated bigint, deleted bigint,
            snapshot_before bigint, snapshot_after bigint)""",
    )
    query(
        conn,
        f"INSERT INTO {catalog_name}.{MERGE_LOG_TABLE} VALUES ('{batch_id}', current_timestamp, "
        f"{total}, {inserted}, {updated}, {deleted}, {before}, {after})",
    )
    return MergeResult(batch_id, total, inserted, updated, deleted, before, after)
