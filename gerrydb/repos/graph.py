"""Repository for dual graphs."""
from typing import Optional

import networkx as nx

from gerrydb.repos.base import (
    NamespacedObjectRepo,
    err,
    namespaced,
    online,
    write_context,
)
from gerrydb.schemas import GeoLayer, Graph, GraphCreate, Locality


class GraphRepo(NamespacedObjectRepo[Graph]):
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
        proj: str | None = None,
    ) -> Graph:
        """Imports a dual graph from a NetworkX graph.

        Args:
            path: A short identifier for the graph (e.g. `iowa_counties_rook`).
            namespace: The graph's namespace.
            locality: `Locality` (or locality path) to associate the graph with.
            layer: `GeoLayer` (or layer path) to associate the graph with.
            graph: Dual graph of the geographies in `locality` and `layer`.
                Node keys must match geography paths.
            description: Longform description of the graph.
            proj: Geographic projection used for projection-dependent edge weights
                such as shared perimeter, specified in WKT (well-known text) format.

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
                locality=(
                    locality.canonical_path
                    if isinstance(locality, Locality)
                    else locality
                ),
                layer=layer.full_path if isinstance(layer, GeoLayer) else layer,
                description=description,
                edges=[
                    (
                        geo_path_1,
                        geo_path_2,
                        {k: v for k, v in weights.items() if k != "id"},
                    )
                    for (geo_path_1, geo_path_2), weights in graph.edges.items()
                ],
                proj=proj,
            ).dict(),
        )
        response.raise_for_status()
        return self.schema(**response.json())
