"""Repository for views."""
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
import networkx as nx
import pandas as pd
import shapely.wkb
from shapely.geometry.base import BaseGeometry

from gerrydb.exceptions import ViewLoadError
from gerrydb.repos.base import (
    NamespacedObjectRepo,
    namespaced,
    normalize_path,
    online,
    write_context,
)
from gerrydb.schemas import (
    GeoLayer,
    Graph,
    GraphMeta,
    Locality,
    ObjectMeta,
    ViewCreate,
    ViewMeta,
    ViewTemplate,
)

_EXPECTED_META_KEYS = {
    "namespace",
    "template",
    "locality",
    "layer",
    "meta",
    "valid_at",
    "proj",
}
_EXPECTED_TABLES = {
    "gerrydb_geo_meta",
    "gerrydb_geo_meta_xref",
    "gerrydb_view_meta",
}


def _load_gpkg_geometry(geom: bytes) -> BaseGeometry:
    pass  # TODO


class View:
    """Rendered view."""

    namespace: str
    path: str
    template: ViewTemplate
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    valid_at: datetime
    proj: Optional[str]
    graph: Optional[GraphMeta]

    _gpkg_path: Path
    _conn: sqlite3.Connection

    def __init__(self, meta: ViewMeta, gpkg_path: Path, conn: sqlite3.Connection):
        self.namespace = meta.namespace
        self.path = meta.path
        self.template = meta.template
        self.locality = meta.locality
        self.layer = meta.layer
        self.meta = meta.meta
        self.valid_at = meta.valid_at
        self.proj = meta.proj
        self.graph = meta.graph

        self._gpkg_path = gpkg_path
        self._conn = conn

    @classmethod
    def from_gpkg(cls, path: Path) -> "View":
        """Loads a view from a GeoPackage."""
        conn = sqlite3.connect(path)
        tables = conn.execute(
            "SELECT name FROM sqlite_schema WHERE "
            "type ='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        missing_tables = _EXPECTED_TABLES - set(table[0] for table in tables)
        if missing_tables:
            raise ViewLoadError(
                "Cannot load view. Does the GeoPackage have GerryDB "
                f"extensions? (missing tables: {', '.join(missing_tables)})"
            )

        meta_rows = conn.execute("SELECT key, value FROM gerrydb_view_meta").fetchall()
        raw_meta = {row[0]: json.loads(row[1]) for row in meta_rows}
        missing_keys = _EXPECTED_META_KEYS - set(raw_meta)
        if missing_keys:
            raise ViewLoadError(
                f"Cannot load view metadata. (missing keys: {', '.join(missing_keys)})"
            )
        return cls(meta=ViewMeta(**raw_meta), gpkg_path=path, conn=conn)

    def to_df(
        self, plans: bool = False, internal_points: bool = False
    ) -> gpd.GeoDataFrame:
        """Loads the view as a GeoDataFrame."""
        gdf = gpd.read_file(self._gpkg_path, layer=self.path).set_index("path")

        if plans:
            # TODO: handle missing plans table.
            plans_df = pd.read_sql_query(
                "SELECT * FROM gerrydb_plan_assignment",
                self._conn,
                index_col="path",
            )
            gdf = gdf.join(plans_df)

        if internal_points:
            internal_points_gdf = (
                gpd.read_file(self._gpkg_path, layer=f"{self.path}__internal_points")
                .set_index("path")
                .rename(columns={"geometry": "internal_point"})
            )
            gdf = gdf.join(internal_points_gdf)

        return gpd.GeoDataFrame(gdf)

    def to_graph(self, plans: bool = True, geometry: bool = False) -> nx.Graph:
        """Loads the view as a NetworkX graph."""
        raw_node_areas = self._conn.execute(
            "SELECT path, area FROM gerrydb_graph_node_area"
        ).fetchall()
        node_areas = {row[0]: row[1] for row in raw_node_areas}

        raw_cols = self._conn.execute(
            "SELECT name from pragma_table_info(?)",
            (self.path,),
        ).fetchall()
        excluded_cols = {"fid"} if geometry else {"fid", "geography"}
        columns = [row[0] for row in raw_cols if row[0] not in excluded_cols]

        # Load nodes with selected attributes.
        graph = nx.Graph()
        for row in self._conn.execute(f"SELECT {', '.join(columns)} FROM {self.path}"):
            node_attrs = dict(zip(columns, row))
            path = node_attrs["path"]
            node_attrs["area"] = node_areas[path]
            del node_attrs["path"]
            if geometry:
                node_attrs["geometry"] = (
                    None
                    if node_attrs["geography"] is None
                    else shapely.wkb.loads(node_attrs["geography"])
                )
                del node_attrs["geography"]
            graph.add_node(path, **node_attrs)

        # Load edges with weights (attributes).
        raw_edges = self._conn.execute(
            "SELECT path_1, path_2, weights from gerrydb_graph_edge"
        ).fetchall()
        graph.add_edges_from(
            (edge[0], edge[1], json.loads(edge[2])) for edge in raw_edges
        )

        return graph


class ViewRepo(NamespacedObjectRepo[ViewMeta]):
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
        graph: Optional[Union[str, Graph, GraphMeta]] = None,
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
        view_meta = ViewMeta(**response.json())

        gpkg_response = self.ctx.client.post(
            f"{self.base_url}/{namespace}/{path}",
        )
        gpkg_response.raise_for_status()
        gpkg_render_id = gpkg_response.headers["X-GerryDB-View-Render-ID"]

        gpkg_path = self.session.cache.upsert_view_gpkg(
            namespace=view_meta.namespace,
            path=view_meta.path,
            render_id=gpkg_render_id,
            content=gpkg_response.content,
        )
        return View.from_gpkg(gpkg_path)

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
        gpkg_path = self.session.cache.get_view_gpkg(
            namespace=normalize_path(namespace), path=normalize_path(path)
        )
        if gpkg_path is None:
            # Generate a new render (assuming the view exists).
            gpkg_response = self.ctx.client.post(
                f"{self.base_url}/{namespace}/{path}",
            )
            gpkg_response.raise_for_status()
            gpkg_render_id = gpkg_response.headers["X-GerryDB-Render-ID"]

            gpkg_path = self.session.cache.upsert_view_gpkg(
                namespace=normalize_path(namespace),
                path=normalize_path(path),
                render_id=gpkg_render_id,
                content=gpkg_response.content,
            )

        return View.from_gpkg(gpkg_path)
