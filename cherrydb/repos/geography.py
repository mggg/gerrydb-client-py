"""Repository for geographies."""
from dataclasses import dataclass
from typing import Optional, Union

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
from cherrydb.schemas import Geography, GeographyCreate, GeoImport


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

    # @err("Failed to create geographies")
    def create(self, geographies: dict[str, BaseGeometry]) -> list[Geography]:
        """Creates one or more geographies.

        Args:
            geographies: Mapping from geography paths to shapes.

        Raises:
            RequestError: If the geographies cannot be created on the server side,
                or if the geographies cannot be serialized.

        Returns:
            A list of new geographies.
        """
        return self._send(geographies, method="POST")

    # @err("Failed to update geographies")
    def update(
        self, geographies: dict[Union[str, Geography], BaseGeometry]
    ) -> list[Geography]:
        """Updates the shapes of one or more geographies.

        Args:
            geographies: Mapping from geography paths or `Geography` objects to shapes.

        Raises:
            RequestError: If the geographies cannot be updated on the server side,
                or if the geographies cannot be serialized.

        Returns:
            A list of updated geographies.
        """
        return self._send(geographies, method="PATCH")

    def _send(
        self, geographies: dict[Union[str, Geography], BaseGeometry], method: str
    ) -> list[Geography]:
        """Creates or updates one or more geographies."""

        raw_geos = [
            GeographyCreate(
                path=key.full_path if isinstance(key, Geography) else key,
                geography=shapely.wkb.dumps(geo),
            ).dict()
            for key, geo in geographies.items()
        ]
        response = self.client.request(
            method,
            f"{self.repo.base_url}/{self.namespace}",
            content=msgpack.dumps(raw_geos),
            headers={"content-type": "application/msgpack"},
        )
        geos_etag = parse_etag(response)
        response.raise_for_status()

        # TODO: Make this more efficient--don't send back or parse
        # geometries that are unmodified by the server.
        response_geos = []
        for response_geo in msgpack.loads(response.content):
            response_geo["geography"] = shapely.wkb.loads(response_geo["geography"])
            response_geos.append(Geography(**response_geo))

        for geo in response_geos:
            self.repo.session.cache.insert(
                obj=geo,
                path=geo.path,
                namespace=geo.namespace,
                etag=geos_etag,
                valid_from=geo.valid_from,
            )


class GeographyRepo(ETagObjectRepo[Geography]):
    """Repository for geographies."""

    @write_context
    @online
    def bulk(self, namespace: Optional[str] = None) -> GeoImporter:
        """Creates a context for creating and updating geographies in bulk."""
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)

        return GeoImporter(repo=self, namespace=namespace)
