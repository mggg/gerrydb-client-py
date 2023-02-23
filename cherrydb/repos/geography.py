"""Repository for geographies."""
from dataclasses import dataclass
from typing import Optional

import httpx
import msgpack
import shapely.wkb
from shapely.geometry.base import BaseGeometry

from cherrydb.exceptions import RequestError
from cherrydb.repos.base import (
    NAMESPACE_ERR,
    ETagObjectRepo,
    err,
    namespaced,
    online,
    parse_etag,
    write_context,
)
from cherrydb.schemas import Geography, GeographyCreateRaw, GeoImport


@dataclass
class GeoImporter:
    """Context for importing geographies in bulk."""

    repo: "GeographyRepo"
    namespace: str
    client: httpx.Client | None = None

    def __enter__(self) -> "GeoImporter":
        """Creates a context for importing geographies in bulk."""
        # Transparently create a GeoImport with the same duration as the write context.
        response = self.repo.ctx.client.post(f"/geo-imports/{self.namespace}")
        response.raise_for_status()  # TODO: refine?
        geo_import = GeoImport(**response.json())

        parent_client = self.repo.ctx.client
        self.client = httpx.Client(
            base_url=parent_client.base_url,
            timeout=parent_client.timeout,
            transport=parent_client._transport,
            headers={
                **dict(parent_client.headers),
                "X-Cherry-Geo-Import-ID": geo_import.uuid,
            },
        )

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()

    @err("Failed to create geographies")
    def create(self, geographies: dict[str, BaseGeometry]) -> list[Geography]:
        """Creates one or more geographies.

        Args:
            path: A short identifier for the column set (e.g. `vap`).
            columns: The columns in the column set.
            description: Longform description of the column set.

        Raises:
            RequestError: If the geogrpahies cannot be created on the server side,
                or if the geographies cannot be serialized.

        Returns:
            A list of new geographies.
        """

        raw_geos = [
            GeographyCreateRaw(path=path, geography=shapely.wkb.dumps(geo)).dict()
            for path, geo in geographies.items()
        ]
        response = self.client.post(
            f"{self.repo.base_url}/{self.namespace}",
            content=msgpack.dumps(raw_geos),
            headers={"content-type": "application/msgpack"},
        )
        print(response.json())
        response.raise_for_status()


class GeographyRepo(ETagObjectRepo[Geography]):
    """Repository for geographies."""

    @write_context
    @online
    def bulk_create(self, namespace: Optional[str] = None) -> GeoImporter:
        """Creates a context for creating geographies in bulk."""
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)

        return GeoImporter(repo=self, namespace=namespace)
