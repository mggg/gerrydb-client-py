"""Tests for views."""

import pytest
import networkx as nx
from gerrydb.repos.view import _load_gpkg_geometry, View
from gerrydb.exceptions import ViewLoadError
from io import BytesIO
from pathlib import Path
import geopandas as gpd
from httpx import HTTPError


def graphs_equal(G1: nx.Graph, G2: nx.Graph) -> bool:
    # Quick check: same node‑set and same edge‑set
    if set(G1.nodes) != set(G2.nodes):
        return False
    edge_set_1 = set([tuple(sorted(e)) for e in G1.edges])
    edge_set_2 = set([tuple(sorted(e)) for e in G1.edges])
    if edge_set_1 != edge_set_2:
        return False

    return True


@pytest.mark.vcr
def test_view_repo_get__invalid(client_with_ia_layer_loc, ia_dataframe):
    client_ns, layer, locality, columns = client_with_ia_layer_loc
    with pytest.raises(HTTPError, match="Not Found"):
        client_ns.views.get("invalid")


@pytest.mark.vcr
def test_view_repo_create__valid(client_with_ia_layer_loc, ia_dataframe):
    client_ns, layer, locality, columns = client_with_ia_layer_loc
    with client_ns.context(notes="Creating a view template and view for Iowa") as ctx:
        view_template = ctx.view_templates.create(
            path="valid_test", columns=list(columns.values()), description="Base view."
        )
        view = ctx.views.create(
            path="ia_valid_test",
            template=view_template,
            locality=locality,
            layer=layer,
        )

    assert set(geo.path for geo in view.geographies) == set(ia_dataframe.index)
    assert set(col.full_path for col in columns.values()) == set(view.values)
    assert view.graph is None


# TODO: test various cases where a view can't be instantiated.
@pytest.fixture(scope="module")
def ia_view(client_with_ia_layer_loc):
    """A basic Iowa counties view without a dual graph (assumes creation works)."""
    client_ns, layer, locality, columns = client_with_ia_layer_loc
    with client_ns.context(notes="Creating a view template and view for Iowa") as ctx:
        view_template = ctx.view_templates.create(
            path="base", columns=list(columns.values()), description="Base view."
        )
        return ctx.views.create(
            path="ia_base",
            template=view_template,
            locality=locality,
            layer=layer,
        )


@pytest.fixture(scope="module")
def ia_view_with_graph(client_with_ia_layer_loc, ia_graph):
    """A basic Iowa counties view with a dual graph (assumes creation works)."""
    client_ns, layer, locality, columns = client_with_ia_layer_loc
    with client_ns.context(notes="Creating a view template and view for Iowa") as ctx:
        view_template = ctx.view_templates.create(
            path="graph_base", columns=list(columns.values()), description="Base view."
        )
        graph = ctx.graphs.create(
            path="ia_counties",
            locality=locality,
            layer=layer,
            description="Naive rook adjacency for Iowa counties.",
            proj="epsg:26915",
            graph=ia_graph,
        )
        return ctx.views.create(
            path="ia_graph",
            template=view_template,
            locality=locality,
            layer=layer,
            graph=graph,
        )


@pytest.mark.vcr
def test_view_repo_view_to_dataframe(ia_view, ia_dataframe):
    view_df = ia_view.to_df()
    assert set(view_df.index) == set(ia_dataframe.index)
    assert set(view_df.columns) == set(
        "/".join(col.split("/")[2:]) for col in ia_view.values
    ) | {"geometry"}


@pytest.mark.vcr
def test_view_repo_view_to_graph(ia_view_with_graph, ia_graph):
    view_graph = ia_view_with_graph.to_graph()

    assert graphs_equal(view_graph, ia_graph)

    expected_cols = set(
        "/".join(col.split("/")[2:]) for col in ia_view_with_graph.values
    )
    # Previous tests in the test suite can add some values to the graph nodes.
    # so we just check that the expected columns are present.
    assert all(
        expected_cols - set(data) == set() for _, data in view_graph.nodes(data=True)
    )


@pytest.mark.vcr
def test_view_repo_view_to_graph_geo(ia_view_with_graph, ia_graph):
    view_graph = ia_view_with_graph.to_graph(geometry=True)

    assert graphs_equal(view_graph, ia_graph)

    expected_cols = set(
        "/".join(col.split("/")[2:]) for col in ia_view_with_graph.values
    ) | {"internal_point", "geometry"}

    # Previous tests in the test suite can add some values to the graph nodes.
    # so we just check that the expected columns are present.
    assert all(
        expected_cols - set(data) == set() for _, data in view_graph.nodes(data=True)
    )


def test_bad_gpkg_geometry__None():
    with pytest.raises(
        ValueError, match="Invalid GeoPackage geometry: empty geometry."
    ):
        _load_gpkg_geometry(None)


def test_bad_gpkg_geometry__badbytes():
    bad_flags = (7 << 1) & 0xFF  # == 14 == 0x0e

    bad_blob = b"GP" + bytes([0x01, bad_flags]) + b"\x00" * (4 + 4 + 10)
    with pytest.raises(
        ValueError, match="Invalid GeoPackage geometry: bad envelope flag."
    ):
        _load_gpkg_geometry(bad_blob)


def test_from_gpkg__gpkg_bytes():
    bytes_path = Path(__file__).parents[1] / "fixtures" / "test_land.gpkg"

    buffer = BytesIO(bytes_path.read_bytes())
    view = View.from_gpkg(buffer)

    view_df = view.to_df(internal_points=True)
    other_df = gpd.read_parquet(
        Path(__file__).parents[1] / "fixtures" / "test_land_df.parquet"
    )
    view_df.sort_index(inplace=True)
    other_df.sort_index(inplace=True)

    assert view_df["geometry"].equals(other_df["geometry"])
    assert view_df["internal_point"].equals(other_df["internal_point"])


def test_from_gpkg__gpkg_base():
    gpkg_path = Path(__file__).parents[1] / "fixtures" / "test_land.gpkg"

    view = View.from_gpkg(gpkg_path)

    view_df = view.to_df(internal_points=True)

    assert view_df.equals(
        gpd.read_parquet(
            Path(__file__).parents[1] / "fixtures" / "test_land_df.parquet"
        )
    )


def test_from_gpkg__missing_meta():
    gpkg_path = Path(__file__).parents[1] / "fixtures" / "test_land_missing_keys.gpkg"

    with pytest.raises(
        ViewLoadError, match="Does the GeoPackage have GerryDB extensions?"
    ):
        View.from_gpkg(gpkg_path)


def test_from_gpkg__missing_keys():
    gpkg_path = Path(__file__).parents[1] / "fixtures" / "test_land_missing_meta.gpkg"

    with pytest.raises(ViewLoadError, match="Cannot load view metadata"):
        View.from_gpkg(gpkg_path)
