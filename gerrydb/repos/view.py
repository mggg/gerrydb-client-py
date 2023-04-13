"""Repository for views."""
from datetime import datetime
from typing import Optional, Union

from gerrydb.repos.base import NamespacedObjectRepo, namespaced, online, write_context
from gerrydb.schemas import (
    GeoLayer,
    Graph,
    Locality,
    View,
    ViewCreate,
    ViewMeta,
    ViewTemplate,
)


class ViewRepo(NamespacedObjectRepo[View]):
    """Repository for views."""

    # @err("Failed to create view")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        template: Union[str, ViewTemplate],
        locality: Union[str, Locality],
        layer: Union[str, GeoLayer],
        graph: Optional[Union[str, Graph]] = None,
        valid_at: Optional[datetime] = None,
        proj: Optional[str] = None,
    ) -> View:
        """Creates a view.

        Args:
            path: Short identifier for the view.
            namespace: namespace of the view.
            template: View template (path) to create the view from.
            locality: Locality (path) to associate the view with.
            layer: Geographic layer (path) to associate the view with.
            graph: Graph to associate the view with.
            valid_at: Point in time to instantiate the view at.
                If not specified, the latest data sources are used.
            proj: Projection to use for geographies in the view.
                If not specified, the default projection of `locality` is used;
                if that is not specified, NAD 83 lat/long (EPSG:4269) coordinates
                are used.

        Raises:
            RequestError: If the view cannot be created on the server side,
                if the parameters fail validation, or if no namespace is provided.

        Returns:
            The new view.
        """
        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=ViewCreate(
                path=path,
                template=template if isinstance(template, str) else template.full_path,
                locality=(
                    locality if isinstance(locality, str) else locality.canonical_path
                ),
                layer=layer if isinstance(layer, str) else layer.full_path,
                graph=(
                    None
                    if graph is None
                    else (graph if isinstance(graph, str) else graph.full_path)
                ),
                valid_at=valid_at,
                proj=proj,
            ).dict(),
        )
        response.raise_for_status()
        meta = ViewMeta(**response.json())
        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}/{path}",
        )
        print("content length:", len(response.content))
        with open("view.gpkg", "wb") as fp:
            fp.write(response.content)

        return meta

    @namespaced
    @online
    def get(
        self,
        path: str,
        namespace: Optional[str] = None,
    ) -> View:
        """Gets a view.

        Raises:
            RequestError: If the view cannot be retrieved on the server side,
                if the parameters fail validation, or if no namespace is provided.
        """
        response = self.session.client.get(
            f"{self.base_url}/{namespace}/{path}",
        )
        response.raise_for_status()
        return ViewMeta(**response.json())
