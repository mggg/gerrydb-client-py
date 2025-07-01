"""Repository for geographies."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Tuple, Union

import httpx
from http import HTTPStatus
import msgpack
import shapely.wkb
from shapely import Point
from shapely.geometry.base import BaseGeometry
import json

from gerrydb.exceptions import RequestError, ForkingError
from gerrydb.repos.base import (
    NAMESPACE_ERR,
    NamespacedObjectRepo,
    err,
    online,
    write_context,
    namespaced,
)
from gerrydb.schemas import Geography, GeographyCreate, GeoImport

if TYPE_CHECKING:
    from gerrydb.client import WriteContext  # pragma: no cover

GeoValType = Union[None, BaseGeometry, Tuple[Optional[BaseGeometry], Optional[Point]]]
GeosType = dict[Union[str, Geography], GeoValType]

from gerrydb.logging import log


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
            ).model_dump()
        )

    return serialized


def _parse_geo_response(response: httpx.Response) -> list[Geography]:
    """Parses `Geography` objects from a MessagePack-encoded API response."""
    response_geos = []
    for response_geo in msgpack.loads(response.content):
        response_geo["geography"] = shapely.wkb.loads(response_geo["geography"])
        response_geo["internal_point"] = shapely.wkb.loads(
            response_geo["internal_point"]
        )
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
        return self._send(geographies, method="PATCH")  # pragma: no cover

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
    async def update(
        self,
        geographies: GeosType,
        *,
        allow_empty_polys: bool,
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
        return await self._send(
            geographies,
            method="PATCH",
            queries={"allow_empty_polys": allow_empty_polys},
        )

    async def _send(
        self,
        geographies: GeosType,
        method: str,
        queries: Optional[dict[str, str]] = None,
    ) -> list[Geography]:
        """Creates or updates one or more geographies."""

        geos = _serialize_geos(geographies)
        serialized_data = msgpack.dumps(geos)

        response = await self.client.request(
            method,
            f"{self.repo.base_url}/{self.namespace}",
            content=serialized_data,
            headers={
                "accept": "application/msgpack",
                "content-type": "application/msgpack",
            },
            params=queries or {},
        )
        if response.status_code == 422:
            json_content = json.loads(response.content)
            log.debug(
                f"422 for Request: {method},\n\tAt: {self.repo.base_url}/{self.namespace}\n\tBody: {json_content}"
            )

            if (
                json_content.get("detail", None)
                == "Object creation failed. Reason: Cannot create geographies that already exist."
            ):
                raise RequestError("Cannot create geographies that already exist.")

            elif (
                json_content.get("detail", None)
                == "Object creation failed. Reason: Cannot create geographies with duplicate paths."
            ):

                raise RequestError(
                    json_content["detail"] + " " + str(json_content["paths"])
                )

        response.raise_for_status()
        return _parse_geo_response(response)


class GeographyRepo(NamespacedObjectRepo[Geography]):
    """Repository for geographies."""

    @write_context
    @online
    def bulk(self, namespace: Optional[str] = None) -> GeoImporter:
        """Creates a context for creating and updating geographies."""
        log.debug("IN CREATE BULK GEOGRAPHY REPO")
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)  # pragma: no cover

        return GeoImporter(repo=self, namespace=namespace)

    @write_context
    @online
    def async_bulk(
        self, namespace: Optional[str] = None, max_conns: Optional[int] = None
    ) -> AsyncGeoImporter:
        """Creates an asynchronous context for creating and updating geographies."""
        log.debug("IN ASYNC CREATE BULK GEOGRAPHY REPO")
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)  # pragma: no cover

        return AsyncGeoImporter(repo=self, namespace=namespace, max_conns=max_conns)

    @namespaced
    @online
    @err("Failed to load geographies")
    def all_paths(
        self, path: str, namespace: Optional[str] = None, *, layer_name: str
    ) -> list[str]:
        if namespace is None:
            namespace = self.session.namespace  # pragma: no cover

        response = self.session.client.get(
            f"/__geography_list/{namespace}/{path}/{layer_name}"
        )
        response.raise_for_status()
        response_json = response.json()

        return response_json

    @namespaced
    @online
    def check_forkability(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        layer_name: str,
        source_namespace: Optional[str] = None,
        source_layer_name: str,
        allow_extra_source_geos: bool = False,
        allow_empty_polys: bool = False,
    ) -> list[Tuple[str, str]]:
        """Checks whether or not data can be forkd from one namesspace to another."""
        try:
            log.debug("Getting forkability")
            response = self.session.client.get(
                f"/__geography_fork/{namespace}/{path}/{layer_name}?mode=compare&source_namespace={source_namespace}&source_layer={source_layer_name}&allow_extra_source_geos={allow_extra_source_geos}&allow_empty_polys={allow_empty_polys}"
            )
            response.raise_for_status()

        except Exception as e:
            if (
                response.status_code == HTTPStatus.CONFLICT
                or response.status_code == HTTPStatus.FORBIDDEN
            ):
                raise ForkingError(
                    f"Forking failed for the following reason: "
                    f"{e.response.json().get('detail', 'No details provided.')}",
                )
            raise e  # pragma: no cover
        return [(item[0], item[1]) for item in response.json()]

    @namespaced
    @online
    def get_layer_hashes(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        layer_name: str,
    ) -> dict[str, str]:
        """Gets the hash of the source layer for each geography in the target layer."""
        try:
            log.debug("Getting layer hashes")
            response = self.session.client.get(
                f"/__geography_list/{namespace}/{path}/{layer_name}?mode=path_hash_pair"
            )
            response.raise_for_status()
        except Exception as e:
            raise RuntimeError("Failed to get layer hashes.") from e

        return [(item[0], item[1]) for item in response.json()]

    @namespaced
    @online
    def fork_geos(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        layer_name: str,
        source_namespace: Optional[str] = None,
        source_layer_name: str,
        allow_extra_source_geos: bool = False,
        allow_empty_polys: bool = False,
    ) -> bool:
        """Fork the geographies from one namespace into another"""
        self.check_forkability(
            path=path,
            namespace=namespace,
            layer_name=layer_name,
            source_namespace=source_namespace,
            source_layer_name=source_layer_name,
            allow_extra_source_geos=allow_extra_source_geos,
            allow_empty_polys=allow_empty_polys,
        )

        try:
            response = self.session.client.post(
                f"/__geography_fork/{namespace}/{path}/{layer_name}?mode=compare&source_namespace={source_namespace}&source_layer={source_layer_name}&allow_extra_source_geos={allow_extra_source_geos}&allow_empty_polys={allow_empty_polys}"
            )
            response.raise_for_status()

        except Exception as e:
            if (
                response.status_code == HTTPStatus.CONFLICT
                or response.status_code == HTTPStatus.FORBIDDEN
            ):
                raise ForkingError(
                    f"Forking failed for the following reason: "
                    f"{e.response.json().get('detail', 'No details provided.')}",
                )
            raise e
