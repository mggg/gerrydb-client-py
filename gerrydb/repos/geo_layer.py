"""Repository for geographic layers."""
from typing import Optional, Union

from gerrydb.repos.base import (
    NamespacedObjectRepo,
    err,
    namespaced,
    online,
    write_context,
)
from gerrydb.schemas import Geography, GeoLayer, GeoLayerCreate, GeoSetCreate, Locality


class GeoLayerRepo(NamespacedObjectRepo[GeoLayer]):
    """Repository for geographic layers."""

    @err("Failed to create geographic layer")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        description: str | None = None,
        source_url: str | None = None,
    ) -> GeoLayer:
        """Creates a geographic layer.

        Args:
            path: A short identifier for the layer (e.g. `block_groups`).
            description: Longform description of the layer.
            source_url: Original source of the layer
                (e.g. a link to a shapefile on the U.S. Census Bureau website).

        Raises:
            RequestError: If the layer cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new geographic layer.
        """
        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=GeoLayerCreate(
                path=path, description=description, source_url=source_url
            ).dict(),
        )
        response.raise_for_status()

        return self.schema(**response.json())

    @err("Failed to map locality to geographic layer")
    @write_context
    @online
    def map_locality(
        self,
        layer: GeoLayer,
        locality: Union[str, Locality],
        geographies: list[Union[str, Geography]],
    ) -> None:
        """Maps a set of `geographies` to `layer` in `locality`.

        Raises:
            RequestError: If the mapping cannot be created on the server side,
                or if the parameters fail validation.
        """
        response = self.ctx.client.put(
            f"{self.base_url}/{layer.namespace}/{layer.path}",
            params={
                "locality": locality.canonical_path
                if isinstance(locality, Locality)
                else locality
            },
            json=GeoSetCreate(
                paths=[
                    geo if isinstance(geo, str) else geo.full_path
                    for geo in geographies
                ]
            ).dict(),
        )
        response.raise_for_status()
