"""Dagster definitions: software-defined assets for bronze ingestion, CDC and dbt models,
plus asset checks, a maintenance job and schedules."""

import os
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from lakehouse.catalog import get_catalog
from lakehouse.cdc import ChangeSpec, apply_changes, land_changes
from lakehouse.checks import load_checks, run_checks
from lakehouse.cli import DEFAULT_MONTHS
from lakehouse.config import Settings
from lakehouse.ingest import ingest_month
from lakehouse.maintenance import maintain
from lakehouse.trino_client import connect, query, scalar

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", REPO_ROOT / "transform"))
CHECKS_FILE = Path(os.environ.get("LAKEHOUSE_CHECKS_FILE", REPO_ROOT / "quality" / "checks.yml"))

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)


class IngestConfig(dg.Config):
    months: list[str] = DEFAULT_MONTHS
    limit: int | None = None


class CdcConfig(dg.Config):
    month: str = DEFAULT_MONTHS[-1]
    updates: int = 5_000
    deletes: int = 1_000
    inserts: int = 2_000
    seed: int = 42


@dg.asset(
    key=["bronze", "yellow_trips"],
    group_name="bronze",
    kinds={"python", "iceberg"},
    description="NYC TLC yellow trips landed with PyIceberg (idempotent per source month).",
)
def bronze_yellow_trips(context: dg.AssetExecutionContext, config: IngestConfig) -> dg.MaterializeResult:
    settings = Settings()
    catalog = get_catalog(settings)
    rows, evolved = 0, []
    for month in config.months:
        result = ingest_month(catalog, settings, month, limit=config.limit)
        context.log.info(f"{month}: {result.rows} rows -> snapshot {result.snapshot_id}")
        rows += result.rows
        evolved += result.added_columns
    return dg.MaterializeResult(
        metadata={"rows_written": rows, "months": config.months, "schema_columns_added": evolved}
    )


@dg.asset(
    key=["bronze", "yellow_trips_changes"],
    deps=[bronze_yellow_trips],
    group_name="bronze",
    kinds={"python", "trino", "iceberg"},
    description="Synthetic CDC feed landed append-only and MERGEd into bronze.yellow_trips.",
)
def bronze_yellow_trips_changes(config: CdcConfig) -> dg.MaterializeResult:
    settings = Settings()
    spec = ChangeSpec(
        updates=config.updates, deletes=config.deletes, inserts=config.inserts, seed=config.seed
    )
    batch_id, events = land_changes(get_catalog(settings), config.month, spec=spec)
    r = apply_changes(connect(settings), settings.trino_catalog, batch_id)
    return dg.MaterializeResult(
        metadata={
            "batch_id": batch_id,
            "events": events,
            "inserted": r.inserted,
            "updated": r.updated,
            "deleted": r.deleted,
            "snapshot_before": str(r.snapshot_before),
            "snapshot_after": str(r.snapshot_after),
        }
    )


@dg.asset_check(asset=bronze_yellow_trips, blocking=True, description="trip_id is present and unique")
def bronze_trip_id_unique() -> dg.AssetCheckResult:
    settings = Settings()
    dupes = scalar(
        connect(settings),
        f"SELECT count(*) - count(DISTINCT trip_id) FROM {settings.trino_catalog}.bronze.yellow_trips",
    )
    return dg.AssetCheckResult(passed=dupes == 0, metadata={"duplicate_trip_ids": int(dupes)})


@dg.asset_check(asset=bronze_yellow_trips, description="bronze has rows for every configured month")
def bronze_months_present() -> dg.AssetCheckResult:
    settings = Settings()
    _, rows = query(
        connect(settings),
        f"SELECT DISTINCT source_month FROM {settings.trino_catalog}.bronze.yellow_trips",
    )
    present = sorted(r[0] for r in rows)
    missing = sorted(set(DEFAULT_MONTHS) - set(present))
    return dg.AssetCheckResult(passed=not missing, metadata={"present": present, "missing": missing})


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def lakehouse_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dg.op(description="Run the Soda-style checks in quality/checks.yml against Trino.")
def run_quality_checks(context: dg.OpExecutionContext) -> None:
    settings = Settings()
    results = run_checks(connect(settings), load_checks(CHECKS_FILE), settings.trino_catalog)
    failures = [r for r in results if not r.passed and r.check.severity == "fail"]
    for r in results:
        context.log.info(f"{r.check.table} :: {r.check.name} -> {r.value} ({'ok' if r.passed else 'FAILED'})")
    if failures:
        raise dg.Failure(f"{len(failures)} quality check(s) failed: {[f.check.name for f in failures]}")


@dg.op(description="Compact, expire snapshots (7d) and remove orphan files on all lakehouse tables.")
def iceberg_maintenance(context: dg.OpExecutionContext) -> None:
    settings = Settings()
    for r in maintain(connect(settings), settings.trino_catalog):
        context.log.info(
            f"{r.table}: files {r.before.data_files}->{r.after.data_files}, "
            f"snapshots {r.before.snapshots}->{r.after.snapshots}"
        )


@dg.job(description="Data quality gate over published tables.")
def quality_checks_job():
    run_quality_checks()


@dg.job(description="Weekly Iceberg table maintenance.")
def iceberg_maintenance_job():
    iceberg_maintenance()


daily_refresh_job = dg.define_asset_job(
    "daily_refresh",
    selection=dg.AssetSelection.assets(bronze_yellow_trips) | dg.AssetSelection.assets(lakehouse_dbt_assets),
    description="Re-ingest the configured months (idempotent) and rebuild silver/gold with dbt.",
)

defs = dg.Definitions(
    assets=[bronze_yellow_trips, bronze_yellow_trips_changes, lakehouse_dbt_assets],
    asset_checks=[bronze_trip_id_unique, bronze_months_present],
    jobs=[daily_refresh_job, quality_checks_job, iceberg_maintenance_job],
    schedules=[
        dg.ScheduleDefinition(
            job=daily_refresh_job, cron_schedule="0 6 * * *", execution_timezone="America/New_York"
        ),
        dg.ScheduleDefinition(
            job=quality_checks_job, cron_schedule="30 7 * * *", execution_timezone="America/New_York"
        ),
        dg.ScheduleDefinition(
            job=iceberg_maintenance_job, cron_schedule="0 3 * * 0", execution_timezone="America/New_York"
        ),
    ],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
