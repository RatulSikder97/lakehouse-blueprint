"""Regenerate tests/fixtures from real TLC monthly files (deterministic sample).

Usage: uv run python scripts/make_fixtures.py data/cache
"""

import sys
from pathlib import Path

import pyarrow.parquet as pq

ROWS = 500
SEED_STRIDE = 7349  # deterministic, roughly uniform spread across the file


def main(cache_dir: str) -> None:
    out = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
    out.mkdir(parents=True, exist_ok=True)
    for month in ("2024-12", "2025-01"):
        table = pq.read_table(Path(cache_dir) / f"yellow_tripdata_{month}.parquet")
        idx = sorted({(i * SEED_STRIDE) % table.num_rows for i in range(ROWS)})
        sample = table.take(idx)
        pq.write_table(sample, out / f"yellow_tripdata_{month}.parquet", compression="zstd")
        print(f"{month}: {sample.num_rows} rows")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/cache")
