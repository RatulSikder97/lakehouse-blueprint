from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lakehouse.ingest import normalize

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)


@pytest.fixture
def raw_2024_12():
    return pq.read_table(FIXTURES / "yellow_tripdata_2024-12.parquet")


@pytest.fixture
def raw_2025_01():
    return pq.read_table(FIXTURES / "yellow_tripdata_2025-01.parquet")


@pytest.fixture
def bronze_2025_01(raw_2025_01):
    return normalize(
        raw_2025_01,
        source_month="2025-01",
        source_file="yellow_tripdata_2025-01.parquet",
        batch_id="test",
        ingested_at=NOW,
    )
