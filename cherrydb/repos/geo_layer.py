"""Repository for geographic layers."""
from typing import Optional

from cherrydb.exceptions import RequestError
from cherrydb.repos.base import ETagObjectRepo, err, online, parse_etag, write_context
from cherrydb.schemas import GeoLayer, GeoLayerCreate


class GeoLayerRepo(ETagObjectRepo[GeoLayer]):
    """Repository for geographic layers."""

    @err("Failed to create geographic layer")
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
            canonical_path: A short identifier for the layer (e.g. `block_groups`).
            description: Longform description of the layer.
            source_url: Original source of the layer
                (e.g. a link to a shapefile on the U.S. Census Bureau website).

        Raises:
            RequestError: If the layer cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new geographic layer.
        """
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(
                "No namespace specified for create(), and no default available."
            )

        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=GeoLayerCreate(
                path=path, description=description, source_url=source_url
            ).dict(),
        )
        response.raise_for_status()

        obj = self.schema(**response.json())
        obj_etag = parse_etag(response)
        self.session.cache.insert(
            obj=obj, path=obj.path, namespace=namespace, etag=obj_etag
        )
        return obj
