"""PyIceberg access to the Lakekeeper REST catalog."""

from __future__ import annotations

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog

from lakehouse.config import Settings


def get_catalog(settings: Settings | None = None) -> Catalog:
    """Return a REST catalog client.

    Storage credentials are not configured here on purpose: Lakekeeper vends short-lived,
    table-scoped S3 credentials (STS) with every `loadTable` response.
    """
    settings = settings or Settings()
    return RestCatalog(
        name="lakehouse",
        uri=settings.catalog_uri,
        warehouse=settings.warehouse,
        **{"header.X-Iceberg-Access-Delegation": "vended-credentials"},
    )


def ensure_namespace(catalog: Catalog, namespace: str) -> None:
    catalog.create_namespace_if_not_exists(namespace)
