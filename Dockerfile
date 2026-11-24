# Workbench image: ingestion, CDC, dbt, DuckDB, quality checks and Dagster in one place.
FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir uv==0.9.28

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/transform \
    DBT_PROJECT_DIR=/app/transform \
    LAKEHOUSE_DATA_DIR=/app/data

RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-group dev

COPY src ./src
COPY transform ./transform
COPY quality ./quality
COPY infra/dagster ./infra/dagster
COPY tests/fixtures ./tests/fixtures
RUN uv sync --frozen --no-group dev \
    && mkdir -p /app/data \
    && chown -R app:app /app/data /app/transform /app/infra/dagster

USER app
# Bake DuckDB extensions and the dbt manifest (needed by Dagster) into the image.
RUN python -c "import duckdb; c = duckdb.connect(); [c.execute(f'INSTALL {e}') for e in ('httpfs', 'iceberg', 'avro')]" \
    && cd transform && dbt parse --quiet

CMD ["lakehouse", "--help"]
