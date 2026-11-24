# lakehouse-blueprint - one-command local lakehouse.
SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE   ?= docker compose
RUN       := $(COMPOSE) --profile tools run --rm workbench
MONTHS    ?= 2024-12 2025-01
LIMIT     ?=
RETENTION ?= 7d

.PHONY: help up build down clean ps logs ingest cdc transform docs checks maintain showcase demo \
        orchestration observability spark-sql spark-maintain trino-cli sql duckdb test lint fmt ci-dbt e2e

help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Build the workbench image (ingestion, dbt, DuckDB, Dagster)
	$(COMPOSE) --profile tools build workbench

up: build ## Start the core stack (MinIO, Postgres, Lakekeeper, Trino) and wait until healthy
	$(COMPOSE) up -d --wait
	@echo "Trino      http://localhost:$${TRINO_PORT:-8080}"
	@echo "Lakekeeper http://localhost:$${LAKEKEEPER_PORT:-8181}/ui"
	@echo "MinIO      http://localhost:$${MINIO_CONSOLE_PORT:-9001}"

down: ## Stop every service (all profiles), keep volumes
	$(COMPOSE) --profile '*' down --remove-orphans

clean: ## Stop everything and delete this project's volumes (the `down -v` equivalent)
	$(COMPOSE) --profile '*' down --volumes --remove-orphans

ps: ## Show service status
	$(COMPOSE) --profile '*' ps -a

logs: ## Tail logs
	$(COMPOSE) --profile '*' logs -f --tail 100

ingest: ## Load TLC months into bronze (MONTHS="2024-12 2025-01", LIMIT=n for a slice)
	$(RUN) lakehouse ingest --months $(MONTHS) $(if $(LIMIT),--limit $(LIMIT))

cdc: ## Land a change feed and MERGE it into bronze.yellow_trips
	$(RUN) lakehouse cdc $(if $(LIMIT),--updates 500 --deletes 100 --inserts 200 --double-updates 25 --sample-rows $(LIMIT))

transform: ## dbt: source freshness, then seed + run + test (bronze -> silver -> gold) on Trino
	$(RUN) sh -c 'cd transform && dbt source freshness && dbt build'

docs: ## Generate and serve dbt docs on http://localhost:8580
	$(COMPOSE) --profile tools run --rm -p 8580:8580 workbench sh -c \
	  'cd transform && dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8580 --no-browser'

checks: ## Run Soda-style data quality checks
	$(RUN) lakehouse checks

maintain: ## Iceberg maintenance: optimize, optimize_manifests, expire_snapshots, remove_orphan_files
	$(RUN) lakehouse maintain --retention $(RETENTION)

showcase: ## Print the showcase queries (snapshots, schema evolution, CDC, time travel, gold)
	$(RUN) lakehouse demo

demo: up ingest cdc transform checks showcase ## End to end: up, ingest, CDC, dbt, checks, queries, maintenance
	$(MAKE) --no-print-directory maintain RETENTION=0s

orchestration: build ## Start Dagster (webserver on :3030 + daemon)
	$(COMPOSE) --profile orchestration up -d --wait
	@echo "Dagster    http://localhost:$${DAGSTER_PORT:-3030}"

observability: ## Start Prometheus (:9090) + Grafana (:3031)
	$(COMPOSE) --profile observability up -d --wait
	@echo "Grafana    http://localhost:$${GRAFANA_PORT:-3031}  (anonymous viewer)"

spark-sql: ## Interactive Spark SQL shell on the Iceberg catalog
	$(COMPOSE) --profile spark run --rm spark

spark-maintain: ## Run Iceberg maintenance procedures with Spark (rewrite_data_files etc.)
	$(COMPOSE) --profile spark run --rm spark --properties-file /opt/lakehouse/spark-defaults.conf \
	  -f /opt/lakehouse/maintenance.sql

trino-cli: ## Interactive Trino CLI on the lakehouse catalog
	$(COMPOSE) exec trino trino --catalog lakehouse

sql: ## Run SQL on Trino: make sql Q="select ..."
	$(RUN) lakehouse sql "$(Q)"

duckdb: ## Run SQL on DuckDB over the same catalog: make duckdb Q="select ... from lake.gold..."
	$(RUN) lakehouse sql --engine duckdb "$(Q)"

test: ## Unit tests (no containers)
	uv run pytest

lint: ## ruff + sqlfluff
	uv run ruff check .
	uv run ruff format --check .
	uv run sqlfluff lint transform/models transform/tests

fmt: ## Auto-format Python and SQL
	uv run ruff format .
	uv run ruff check --fix .
	uv run sqlfluff fix transform/models transform/tests

ci-dbt: ## dbt build against DuckDB + a parquet fixture (what CI runs)
	uv run lakehouse fixtures
	cd transform && DBT_TARGET=ci DBT_PROFILES_DIR=. uv run dbt build

e2e: ## Compose-based smoke test on a small slice (uses committed fixtures, then tears down)
	./scripts/e2e_smoke.sh
