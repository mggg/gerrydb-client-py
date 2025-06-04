"""Integration/VCR tests for dual graphs."""

import networkx as nx
from gerrydb.repos.graph import (
    _load_gpkg_geometry,
    _GPKG_ENVELOPE_BYTES,
    DBGraph,
    GraphRepo,
)
from gerrydb.exceptions import GraphLoadError, GraphCreateError
import pytest
from shapely import Point, LineString, Polygon
import shapely
from pathlib import Path
from io import BytesIO
import geopandas as gpd
import httpx


def graphs_equal(G1: nx.Graph, G2: nx.Graph) -> bool:
    # Quick check: same node‑set and same edge‑set
    if set(G1.nodes) != set(G2.nodes):
        return False
    if set(G1.edges) != set(G2.edges):
        return False

    return True


@pytest.mark.vcr
def test_graph_repo_create_get__valid(client_with_ia_layer_loc, ia_graph):
    client_ns, layer, locality, _ = client_with_ia_layer_loc
    with client_ns.context(notes="Uploading a graph for Iowa counties") as ctx:
        graph = ctx.graphs.create(
            path="ia_counties_rook2",
            locality=locality,
            layer=layer,
            description="Naive rook adjacency for Iowa counties.",
            proj="epsg:26915",
            graph=ia_graph,
        )
        saved_edges = {
            (path_1.split("/")[-1], path_2.split("/")[-1])
            for path_1, path_2 in graph.graph.edges
        }
        sorted_saved_edges = set([tuple(sorted(edge)) for edge in saved_edges])
        sorted_ia_graph_edges = set([tuple(sorted(edge)) for edge in ia_graph.edges])
        assert sorted_saved_edges == sorted_ia_graph_edges

        retrieved_graph = ctx.graphs["ia_counties_rook2"]
        assert graphs_equal(graph.graph, retrieved_graph.graph)
        assert graph.namespace == retrieved_graph.namespace
        assert graph.path == retrieved_graph.path
        assert graph.locality == retrieved_graph.locality
        assert graph.layer == retrieved_graph.layer
        assert graph.meta == retrieved_graph.meta
        assert graph.created_at == retrieved_graph.created_at
        assert graph.proj == retrieved_graph.proj


def make_gpkg_blob(wkb_bytes: bytes, flag: int) -> bytes:
    """
    Construct a minimal "GeoPackage WKB blob" with a given envelope_flag.
    The header is:
      - 3 arbitrary bytes
      - 1 byte whose bits 1-3 encode envelope_flag
      - 4 more header bytes
      - envelope_bytes zero-padding
      - the raw WKB
    """
    # how many envelope bytes the real loader expects:
    envelope_bytes = _GPKG_ENVELOPE_BYTES.get(flag)
    if envelope_bytes is None:
        # so that our “bad-flag” test ends up here instead of KeyError in make_blob
        envelope_bytes = 0

    # envelope_flag is in bits 1–3 of the fourth byte:
    flags_byte = (flag << 1) & 0b00001110

    header = b"\x00\x00\x00" + bytes([flags_byte]) + b"\x00" * 4
    padding = b"\x00" * envelope_bytes
    return header + padding + wkb_bytes


def test_load_point_with_no_envelope():
    pt = Point(1.23, 4.56)
    wkb = shapely.wkb.dumps(pt)
    blob = make_gpkg_blob(wkb, flag=0)  # envelope_flag == 0 → no envelope bytes
    out = _load_gpkg_geometry(blob)
    assert isinstance(out, Point)
    assert out.equals(pt)


@pytest.mark.parametrize(
    "geom",
    [
        Point(0, 0),
        LineString([(0, 0), (1, 1), (2, 3)]),
        Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
    ],
)
def test_load_various_geoms_for_all_supported_flags(geom):
    """
    For every valid flag in the real mapping, build a blob and assert
    we still get the same geometry back.
    """
    wkb = shapely.wkb.dumps(geom)
    for flag, envelope_bytes in _GPKG_ENVELOPE_BYTES.items():
        blob = make_gpkg_blob(wkb, flag=flag)
        out = _load_gpkg_geometry(blob)
        assert type(out) is type(geom)
        assert out.equals(geom)


def test_none_raises_empty_geometry():
    with pytest.raises(ValueError) as exc:
        _load_gpkg_geometry(None)
    assert "empty geometry" in str(exc.value).lower()


def test_bad_envelope_flag_raises():
    # pick a flag outside the real mapping
    bad_flag = max(_GPKG_ENVELOPE_BYTES) + 1
    pt = Point(5, 6)
    wkb = shapely.wkb.dumps(pt)
    # build a blob whose fourth byte encodes our invalid flag
    blob = make_gpkg_blob(wkb, flag=bad_flag)
    with pytest.raises(ValueError) as exc:
        _load_gpkg_geometry(blob)
    assert "bad envelope flag" in str(exc.value).lower()


FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_from_gpkg__gpkg_file_loads_graph():
    gpkg_path = FIXTURES / "test_graph.gpkg"
    graph = DBGraph.from_gpkg(gpkg_path)

    assert isinstance(graph, DBGraph)
    assert isinstance(graph.graph, nx.Graph)

    # namespace/path come from the metadata table
    assert graph.full_path.startswith(f"/{graph.namespace}/{graph.path}")

    # at least one node & edge
    assert graph.graph.number_of_nodes() > 0
    assert graph.graph.number_of_edges() > 0


def test_from_gpkg__missing_tables_raises():
    bad = FIXTURES / "test_graph_missing_meta.gpkg"
    with pytest.raises(
        GraphLoadError,
        match="Cannot load graph. Does the GeoPackage have GerryDB extensions?",
    ):
        DBGraph.from_gpkg(bad)


def test_from_gpkg__missing_meta_keys_raises():
    bad = FIXTURES / "test_graph_missing_keys.gpkg"
    with pytest.raises(
        GraphLoadError,
        match="Cannot load graph metadata.",
    ):
        DBGraph.from_gpkg(bad)


def test_bad_to_networkx():
    bad = FIXTURES / "test_graph_missing_cols.gpkg"

    with pytest.raises(
        GraphLoadError,
        match="Unexpected or missing columns in Graph Geopackage Geometry table.",
    ):
        DBGraph.from_gpkg(bad)


def test_graph_with_geometry():
    # Test that the graph is loaded correctly with geometry
    gpkg_path = FIXTURES / "test_graph.gpkg"

    gdf = gpd.read_file(gpkg_path, layer="me_10_county_dual__geometry").set_index(
        "path"
    )
    gdf_internal = gpd.read_file(
        gpkg_path, layer="me_10_county_dual__internal_points"
    ).set_index("path")

    graph = DBGraph.from_gpkg(gpkg_path)

    nx_graph = graph.to_networkx(include_geometries=True)

    for node, data in nx_graph.nodes(data=True):
        assert data["geometry"] == gdf.loc[node].geometry
        assert data["internal_point"] == gdf_internal.loc[node].geometry


def _unwrap(fn):
    """Unwraps decorators from a function."""
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


class DummyContext:
    client = httpx.Client()


class DummyRepo:
    base_url = "http://localhost:8000/api/v1/graphs"
    ctx = DummyContext()


def test_graph_create_raise_for_status(httpx_mock):
    # Mock the response to raise an error
    httpx_mock.add_response(
        url="http://localhost:8000/api/v1/graphs/test_namespace",
        status_code=400,
        json={"detail": "Bad things happened"},
    )

    raw_create = _unwrap(GraphRepo.create)

    with pytest.raises(GraphCreateError, match="Bad things happened"):
        raw_create(
            self=DummyRepo(),
            path="test_graph",
            namespace="test_namespace",
            locality="test_locality",
            layer="test_layer",
            graph=nx.Graph(),
            description="Test graph",
        )
