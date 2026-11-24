#!/usr/bin/env bash
# End-to-end smoke test on the committed 1,000-row fixture (no internet needed for data):
# compose up -> ingest -> CDC MERGE -> dbt freshness + build -> checks -> demo -> maintenance -> assertions.
# Tears the stack (and its volumes) down on exit unless KEEP_STACK=1.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose"
RUN="$COMPOSE --profile tools run --rm -T -e LAKEHOUSE_SOURCE_DIR=/app/tests/fixtures workbench"

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "::group::service logs"; $COMPOSE logs --tail 80 || true; echo "::endgroup::"
  fi
  if [ "${KEEP_STACK:-0}" != "1" ]; then
    $COMPOSE --profile '*' down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT

step() { printf '\n==> %s\n' "$*"; }
assert_contains() {
  if ! grep -qF -- "$2" <<<"$1"; then
    echo "ASSERTION FAILED: expected '$2' in output:"; echo "$1"; exit 1
  fi
}

# Asserts that a single-value query result (render_table output) equals $2.
assert_value() {
  local got
  got=$(sed -n 3p <<<"$1" | tr -d ' ,')
  if [ "$got" != "$2" ]; then
    echo "ASSERTION FAILED: expected value $2, got '$got' in:"; echo "$1"; exit 1
  fi
}

step "build + start core stack"
$COMPOSE --profile tools build workbench
$COMPOSE up -d --wait

step "ingest fixture months into bronze"
out=$($RUN lakehouse ingest)
echo "$out"
assert_contains "$out" "2025-01: 500 rows"
assert_contains "$out" "schema evolved: +cbd_congestion_fee"

step "re-ingest is idempotent"
$RUN lakehouse ingest >/dev/null
out=$($RUN lakehouse sql "SELECT count(*) AS n FROM bronze.yellow_trips")
assert_value "$out" 1000

step "CDC change feed + MERGE"
out=$($RUN lakehouse cdc --updates 50 --deletes 10 --inserts 20 --double-updates 5 --sample-rows 500)
echo "$out"
assert_contains "$out" "inserted 20, updated 50, deleted 10"

step "dbt source freshness + build (seed, models, tests) on Trino"
$RUN sh -c 'cd transform && dbt source freshness && dbt build'

step "Soda-style quality checks"
$RUN lakehouse checks --file quality/checks_smoke.yml

step "demo queries (snapshots, schema evolution, time travel in Trino and DuckDB)"
out=$($RUN lakehouse demo)
echo "$out" | tail -40
assert_contains "$out" "before_merge"

step "time travel returns the pre-MERGE row count"
before=$($RUN lakehouse sql "SELECT snapshot_before FROM ops.cdc_merge_log ORDER BY applied_at DESC LIMIT 1" | sed -n 3p | tr -d ' ')
out=$($RUN lakehouse sql --engine duckdb "SELECT count(*) AS n FROM lake.bronze.yellow_trips AT (VERSION => $before)")
assert_value "$out" 1000
out=$($RUN lakehouse sql --engine duckdb "SELECT count(*) AS n FROM lake.bronze.yellow_trips")
assert_value "$out" 1010

step "Iceberg maintenance"
$RUN lakehouse maintain --retention 0s

echo
echo "E2E SMOKE TEST PASSED"
