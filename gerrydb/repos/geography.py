"""Repository for geographies."""
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Tuple, Union

import httpx
import msgpack
import shapely.wkb
from shapely import Point
from shapely.geometry.base import BaseGeometry

from gerrydb.exceptions import RequestError
from gerrydb.repos.base import (
    NAMESPACE_ERR,
    NamespacedObjectRepo,
    err,
    online,
    write_context,
)
from gerrydb.schemas import Geography, GeographyCreate, GeoImport

if TYPE_CHECKING:
    from gerrydb.client import WriteContext

GeoValType = Union[None, BaseGeometry, Tuple[Optional[BaseGeometry], Optional[Point]]]
GeosType = dict[Union[str, Geography], GeoValType]


def _importer_params(ctx: "WriteContext", namespace: str) -> dict[str, Any]:
    """Generates client parameters with a `GeoImport` context."""
    response = ctx.client.post(f"/geo-imports/{namespace}")
    response.raise_for_status()  # TODO: refine?
    geo_import = GeoImport(**response.json())

    params = ctx.client_params.copy()
    params["headers"]["X-GerryDB-Geo-Import-ID"] = geo_import.uuid
    return params


def _serialize_geos(geographies: GeosType) -> list[GeographyCreate]:
    """Serializes geographies into raw bytes."""
    serialized = []
    for key, geo_pair in geographies.items():
        if isinstance(geo_pair, tuple):
            geo, point = geo_pair
        elif isinstance(geo_pair, BaseGeometry):
            geo = geo_pair
            point = None
        else:
            geo = point = None

        serialized.append(
            GeographyCreate(
                path=key.full_path if isinstance(key, Geography) else key,
                geography=None if geo is None else shapely.wkb.dumps(geo),
                internal_point=None if point is None else shapely.wkb.dumps(point),
            ).dict()
        )

    return serialized


def _parse_geo_response(response: httpx.Response) -> list[Geography]:
    """Parses `Geography` objects from a MessagePack-encoded API response."""
    response_geos = []
    for response_geo in msgpack.loads(response.content):
        response_geo["geography"] = shapely.wkb.loads(response_geo["geography"])
        response_geos.append(Geography(**response_geo))
    return response_geos


@dataclass
class GeoImporter:
    """Context for importing geographies in bulk."""

    repo: "GeographyRepo"
    namespace: str
    client: Optional[httpx.Client] = None

    def __enter__(self) -> "GeoImporter":
        """Creates a context for importing geographies in bulk."""
        self.client = httpx.Client(**_importer_params(self.repo.ctx, self.namespace))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()

    @err("Failed to create geographies")
    def create(self, geographies: dict[str, GeoValType]) -> list[Geography]:
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

    @err("Failed to update geographies")
    def update(self, geographies: GeosType) -> list[Geography]:
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

    def _send(self, geographies: GeosType, method: str) -> list[Geography]:
        """Creates or updates one or more geographies."""
        response = self.client.request(
            method,
            f"{self.repo.base_url}/{self.namespace}",
            content=msgpack.dumps(_serialize_geos(geographies)),
            headers={"content-type": "application/msgpack"},
        )
        response.raise_for_status()
        return _parse_geo_response(response)


@dataclass
class AsyncGeoImporter:
    """Asynchronous context for importing geographies in bulk."""

    repo: "GeographyRepo"
    namespace: str
    client: Optional[httpx.AsyncClient] = None
    max_conns: Optional[int] = None

    async def __aenter__(self) -> "AsyncGeoImporter":
        """Creates a context for asynchronously importing geographies in bulk."""
        params = _importer_params(self.repo.ctx, self.namespace)
        params["transport"] = httpx.AsyncHTTPTransport(retries=1)
        self.client = httpx.AsyncClient(**params)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.client.aclose()

    @err("Failed to create geographies")
    async def create(self, geographies: dict[str, GeoValType]) -> list[Geography]:
        """Creates one or more geographies.

        Args:
            geographies: Mapping from geography paths to shapes.

        Raises:
            RequestError: If the geographies cannot be created on the server side,
                or if the geographies cannot be serialized.

        Returns:
            A list of new geographies.
        """
        return await self._send(geographies, method="POST")

    @err("Failed to update geographies")
    async def update(self, geographies: GeosType) -> list[Geography]:
        """Updates the shapes of one or more geographies.

        Args:
            geographies: Mapping from geography paths or `Geography` objects to shapes.

        Raises:
            RequestError: If the geographies cannot be updated on the server side,
                or if the geographies cannot be serialized.

        Returns:
            A list of updated geographies.
        """
        return await self._send(geographies, method="PATCH")

    async def _send(self, geographies: GeosType, method: str) -> list[Geography]:
        """Creates or updates one or more geographies."""
        response = await self.client.request(
            method,
            f"{self.repo.base_url}/{self.namespace}",
            content=msgpack.dumps(_serialize_geos(geographies)),
            headers={
                "accept": "application/msgpack",
                "content-type": "application/msgpack",
            },
        )
        response.raise_for_status()
        return _parse_geo_response(response)


class GeographyRepo(NamespacedObjectRepo[Geography]):
    """Repository for geographies."""

    @write_context
    @online
    def bulk(self, namespace: Optional[str] = None) -> GeoImporter:
        """Creates a context for creating and updating geographies."""
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)

        return GeoImporter(repo=self, namespace=namespace)

    @write_context
    @online
    def async_bulk(
        self, namespace: Optional[str] = None, max_conns: Optional[int] = None
    ) -> AsyncGeoImporter:
        """Creates an asynchronous context for creating and updating geographies."""
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)

        return AsyncGeoImporter(repo=self, namespace=namespace, max_conns=max_conns)

    # TODO: get()
