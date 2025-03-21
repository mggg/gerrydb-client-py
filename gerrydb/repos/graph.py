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
    online,
    write_context,
)
from gerrydb.exceptions import GraphLoadError
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
from pathlib import Path
import json
import networkx as nx
import shapely

try:
    import gerrychain
except ImportError:
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
        self.locality = meta.locality
        self.layer = meta.layer
        self.meta = meta.meta
        self.created_at = meta.created_at
        self.proj = meta.proj

        self._gpkg_path = gpkg_path
        self._conn = conn

        # Actually load the graph.
        self.graph = self.to_networkx(
            self._gpkg_path, include_geometries=include_geometries
        )

    @classmethod
    def from_gpkg(
        cls,
        path: Path,
    ) -> "DBGraph":
        """Loads a graph from a GeoPackage."""
        print("IN GRAPH FROM GPKG")
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
        print(f"Time to convert gpkg: {end - start}")
        return ret

    def to_networkx(
        self,
        include_geometries: bool = False,
    ) -> nx.Graph:
        """Loads a graph from a GeoPackage."""
        print("IN TO NETWORKX")
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

        return graph

    def __repr__(self):
        return f"DBGraph(path={self.path}, namespace={self.namespace}, locality={self.locality}, layer={self.layer}, meta={self.meta}, created_at={self.created_at}, proj={self.proj})"


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
        locality: Union[str, Locality],
        layer: Union[str, GeoLayer],
        graph: nx.Graph,
        description: str,
        proj: Optional[str] = None,
        timeout: int = 1200,
    ) -> Graph:
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
            timeout=timeout,
        )
        response.raise_for_status()
        return self.schema(**response.json())

    # @namespaced
    # @online
    def get(
        self,
        path: str,
        namespace: Optional[str] = None,
        request_timeout: int = 1200,
    ) -> Graph:
        """Gets a graph.

        Raises:
            RequestError: If the graph cannot be retrieved on the server side,
                if the parameters fail validation, or if no namespace is provided.
        """
        # gpkg_path = self.session.cache.get_graph_gpkg(
        #     namespace=normalize_path(namespace, path_length=1),
        #     path=normalize_path(path),
        # )
        # if gpkg_path is None:
        #     gpkg_path = self._get(path, namespace, request_timeout)
        return DBGraph.from_gpkg(path)
