"""Repository for dual graphs."""
from typing import Optional, Union

import networkx as nx

from cherrydb.repos.base import (
    ETagObjectRepo,
    err,
    namespaced,
    online,
    parse_etag,
    write_context,
)
from cherrydb.schemas import GeoLayer, Graph, GraphCreate, Locality


class GraphRepo(ETagObjectRepo[Graph]):
    """Repository for dual graphs."""

    @err("Failed to create dual graph")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        locality: str | Locality,
        layer: str | GeoLayer,
        graph: nx.Graph,
        description: str,
    ) -> Graph:
        """Imports a dual graph from a NetworkX graph.

        Args:
            path: A short identifier for the graph (e.g. `iowa_counties_rook`).
            namespace: The graph's namespace.
            locality: `Locality` (or locality path) to associate the graph with.
            layer: `GeoLayer` (or layer path) to associate the graph with.
            description: Longform description of the graph.

        Raises:
            RequestError: If the graph cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new districting plan.
        """
        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=GraphCreate(
                path=path,
                description=description,
                locality=(
                    locality.canonical_path
                    if isinstance(locality, Locality)
                    else locality
                ),
                layer=layer.full_path if isinstance(layer, GeoLayer) else layer,
            ).dict(),
        )
        response.raise_for_status()

        obj = self.schema(**response.json())
        obj_etag = parse_etag(response)
        """
        self.session.cache.insert(
            obj=obj, path=obj.path, namespace=namespace, etag=obj_etag
        )
        """
        return obj