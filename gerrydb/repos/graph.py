"""Repository for dual graphs."""

from typing import Optional, Union

import networkx as nx
import sqlite3
import io
from datetime import datetime

from gerrydb.repos.base import (
    NamespacedObjectRepo,
    err,
    namespaced,
    normalize_path,
    online,
    write_context,
)
from gerrydb.exceptions import GraphLoadError, GraphCreateError
from gerrydb.schemas import (
    GeoLayer,
    Graph,
    GraphMeta,
    Locality,
    ObjectMeta,
    GraphCreate,
    GraphMeta,
    BaseGeometry,
)
import time
import logging
from pathlib import Path
import json
import networkx as nx
import shapely
from gerrydb.logging import log

try:
    import gerrychain
except ImportError:  # pragma: no cover
    gerrychain = None


_EXPECTED_META_KEYS = {
    "created_at",
    "description",
    "layer",
    "locality",
    "meta",
    "namespace",
    "path",
    "proj",
}
_EXPECTED_TABLES = {
    "gerrydb_geo_meta",
    "gerrydb_geo_attrs",
    "gerrydb_graph_meta",
}
# by flag (see https://www.geopackage.org/spec/#gpb_format)
_GPKG_ENVELOPE_BYTES = {
    0: 0,
    1: 32,
    2: 48,
    3: 48,
    4: 64,
}


def _load_gpkg_geometry(geom: bytes) -> BaseGeometry:
    """Loads a geometry from a raw GeoPackage WKB blob."""
    # header format: https://www.geopackage.org/spec/#gpb_format
    if geom == None:
        raise ValueError("Invalid GeoPackage geometry: empty geometry.")

    envelope_flag = (geom[3] & 0b00001110) >> 1
    try:
        envelope_bytes = _GPKG_ENVELOPE_BYTES[envelope_flag]
    except KeyError:
        raise ValueError("Invalid GeoPackage geometry: bad envelope flag.")

    wkb_offset = envelope_bytes + 8
    return shapely.wkb.loads(geom[wkb_offset:])


class DBGraph:
    """Rendered Graph"""

    namespace: str
    path: str
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime
    proj: Optional[str]
    graph: nx.Graph

    _gpkg_path: Path
    _conn: sqlite3.Connection

    def __init__(
        self,
        meta: GraphMeta,
        gpkg_path: Path,
        conn: sqlite3.Connection,
        include_geometries: bool = False,
    ):
        self.namespace = meta.namespace
        self.path = meta.path
        self.full_path = f"/{self.namespace}/{self.path}"
        self.locality = meta.locality
        self.layer = meta.layer
        self.meta = meta.meta
        self.created_at = meta.created_at
        self.proj = meta.proj

        self._gpkg_path = gpkg_path
        self._conn = conn

        # Actually load the graph.
        log.debug("INCLUDE GEOMETRIES %s", include_geometries)
        self.graph = self.to_networkx(include_geometries=include_geometries)

    @classmethod
    def from_gpkg(
        cls,
        path: Path,
    ) -> "DBGraph":
        """Loads a graph from a GeoPackage."""
        log.debug("IN GRAPH FROM GPKG")
        start = time.perf_counter()
        if isinstance(path, io.BytesIO):
            path.seek(0)
            conn = sqlite3.connect(
                "file:cached_view?mode=memory&cache=shared", uri=True
            )
            conn.executescript(path.read().decode("utf-8"))
        else:
            conn = sqlite3.connect(path)

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE "
            "type ='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        missing_tables = _EXPECTED_TABLES - set(table[0] for table in tables)
        if missing_tables:
            raise GraphLoadError(
                "Cannot load graph. Does the GeoPackage have GerryDB "
                f"extensions? (missing table(s): {', '.join(missing_tables)})"
            )

        meta_rows = conn.execute("SELECT key, value FROM gerrydb_graph_meta").fetchall()
        raw_meta = {row[0]: json.loads(row[1]) for row in meta_rows}
        missing_keys = _EXPECTED_META_KEYS - set(raw_meta)
        if missing_keys:
            raise GraphLoadError(
                f"Cannot load graph metadata. (missing keys: {', '.join(missing_keys)})"
            )
        ret = cls(meta=GraphMeta(**raw_meta), gpkg_path=path, conn=conn)
        end = time.perf_counter()
        log.debug(f"Time to convert gpkg: {end - start}")
        return ret

    def to_networkx(
        self,
        include_geometries: bool = False,
    ) -> nx.Graph:
        """Loads a graph from a GeoPackage."""
        log.debug("IN TO NETWORKX")
        raw_cols = self._conn.execute(
            "SELECT name from pragma_table_info(?)",
            (f"{self.path}__geometry",),
        ).fetchall()

        if set(raw_cols) != {("fid",), ("geography",), ("path",)}:
            raise GraphLoadError(
                "Unexpected or missing columns in Graph Geopackage Geometry table."
                " Expected columns: fid, geography, path."
                f" Found columns: {set(raw_cols)}"
            )

        columns = ["path", "geography"] if include_geometries else ["path"]
        prefixed_columns = [f"{self.path}__geometry.{col}" for col in columns]

        join_clauses = []

        if include_geometries:
            # Join geographic layers: add internal points.
            columns.append("internal_point")
            prefixed_columns.append(f"{self.path}__internal_points.internal_point")
            join_clauses.append(
                f"JOIN {self.path}__internal_points "
                f"ON {self.path}__geometry.path = {self.path}__internal_points.path"
            )

        query = f"SELECT {', '.join(prefixed_columns)} FROM {self.path}__geometry "
        query += " ".join(join_clauses)

        # Load nodes with selected attributes.
        graph = nx.Graph()
        for row in self._conn.execute(query):
            node_attrs = dict(zip(columns, row))
            path = node_attrs["path"]
            del node_attrs["path"]
            if include_geometries:
                node_attrs["geometry"] = (
                    None
                    if node_attrs["geography"] is None
                    else _load_gpkg_geometry(node_attrs["geography"])
                )
                del node_attrs["geography"]
                node_attrs["internal_point"] = (
                    None
                    if node_attrs["internal_point"] is None
                    else _load_gpkg_geometry(node_attrs["internal_point"])
                )
            graph.add_node(path, **node_attrs)

        edge_query = self._conn.execute(
            "SELECT path_1, path_2, weights FROM gerrydb_graph_edge"
        )
        for edge in edge_query:
            graph.add_edge(edge[0], edge[1], attr=edge[2])

        return graph

    def __repr__(self):  # pragma: no cover
        return f"DBGraph(path={self.path}, namespace={self.namespace}, locality={self.locality}, layer={self.layer}, meta={self.meta}, created_at={self.created_at}, proj={self.proj})"


class GraphRepo(NamespacedObjectRepo[Graph]):
    """Repository for dual graphs."""

    # @err("Failed to create dual graph")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        locality: Union[str, Locality],
        layer: Union[str, GeoLayer],
        graph: nx.Graph,
        description: str,
        proj: Optional[str] = None,
        timeout: int = 1200,
    ) -> DBGraph:
        """
        Imports a dual graph from a NetworkX graph.

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
            The new districting plan in the form of a gerrydb `Graph` schema
            object.
        """
        log.debug("IN GRAPH REPO CREATE")
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
            ).model_dump(mode="json"),
            timeout=timeout,
        )

        try:
            response.raise_for_status()
        except Exception as e:
            raise GraphCreateError(
                f"Failed to create graph. Got code {response.status_code}. Details: {response.json().get('detail', 'No details provided.')}"
            )

        graph_meta = self.schema(**response.json())

        log.debug("THE GRAPH PATH IS %s", graph_meta.path)
        log.debug("THE GRAPH NAMESPACE IS %s", graph_meta.namespace)
        gpkg_path = self._get(path=graph_meta.path, namespace=graph_meta.namespace)
        log.debug("THE GPKG PATH IS %s", gpkg_path)
        return DBGraph.from_gpkg(gpkg_path)

    def _get(self, path: str, namespace: str, request_timeout: int = 1200) -> Path:
        """Downloads view data as a GeoPackage."""
        # Generate a new render (assuming the view exists).
        # These can take a long time to render depending on the size of the view.
        gpkg_response = self.session.client.post(
            f"{self.base_url}/{namespace}/{path}",
            timeout=request_timeout,
        )
        log.debug("THE GPKG RESPONSE IS %s", gpkg_response)
        log.debug("THE GPKG RESPONSE HEADERS ARE %s", gpkg_response.headers)

        if gpkg_response.status_code >= 400:
            gpkg_response.raise_for_status()  # pragma: no cover
        if gpkg_response.next_request is not None:  # pragma: no cover
            # Redirect to Google Cloud Storage (probably).
            gpkg_response = self.session.client.get(
                gpkg_response.next_request.url
            )  # pragma: no cover
            gpkg_response.raise_for_status()  # pragma: no cover
            gpkg_render_id = gpkg_response.headers[  # pragma: no cover
                "x-goog-meta-gerrydb-graph-render-id"
            ]
        else:
            gpkg_render_id = gpkg_response.headers["x-gerrydb-graph-render-id"]

        return self.session.cache.upsert_graph_gpkg(
            namespace=normalize_path(namespace, path_length=1),
            path=normalize_path(path),
            render_id=gpkg_render_id,
            content=gpkg_response.content,
        )

    @namespaced
    @online
    def get(
        self,
        path: str,
        namespace: Optional[str] = None,
        request_timeout: int = 1200,
    ) -> DBGraph:
        """Gets a graph.

        Raises:
            RequestError: If the graph cannot be retrieved on the server side,
                if the parameters fail validation, or if no namespace is provided.
        """
        gpkg_path = self.session.cache.get_graph_gpkg(
            namespace=normalize_path(namespace, path_length=1),
            path=normalize_path(path),
        )
        if gpkg_path is None:
            gpkg_path = self._get(path, namespace, request_timeout)  # pragma: no cover
        log.debug("THE GPKG PATH IS %s", gpkg_path)
        return DBGraph.from_gpkg(gpkg_path)

    @err("Failed to load objects")
    def all(self, namespace: Optional[str] = None) -> list[GraphMeta]:
        """Gets all objects in a namespace."""
        log.debug(f"Loading all objects from {self.base_url}/{namespace}")
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)

        response = self.session.client.get(f"{self.base_url}/{namespace}")
        response.raise_for_status()
        return [GraphMeta(**obj) for obj in response.json()]
