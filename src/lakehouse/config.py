"""Runtime configuration, resolved from environment variables with docker-compose defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    catalog_uri: str = field(
        default_factory=lambda: _env("LAKEHOUSE_CATALOG_URI", "http://lakekeeper:8181/catalog")
    )
    warehouse: str = field(default_factory=lambda: _env("LAKEHOUSE_WAREHOUSE", "lakehouse"))
    trino_host: str = field(default_factory=lambda: _env("TRINO_HOST", "trino"))
    trino_port: int = field(default_factory=lambda: int(_env("TRINO_PORT", "8080")))
    trino_user: str = field(default_factory=lambda: _env("TRINO_USER", "lakehouse"))
    trino_catalog: str = field(default_factory=lambda: _env("TRINO_CATALOG", "lakehouse"))
    data_dir: Path = field(default_factory=lambda: Path(_env("LAKEHOUSE_DATA_DIR", "data")))
    source_base_url: str = field(default_factory=lambda: _env("LAKEHOUSE_SOURCE_BASE_URL", TLC_BASE_URL))
    # When set, raw monthly files are read from this directory instead of being downloaded.
    source_dir: Path | None = field(
        default_factory=lambda: Path(p) if (p := os.environ.get("LAKEHOUSE_SOURCE_DIR")) else None
    )

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"
