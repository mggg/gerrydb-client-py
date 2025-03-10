"""Tests for views."""

import pytest


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

    assert set(view_graph) == set(ia_graph)
    assert set(view_graph.edges) == set(ia_graph.edges)

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

    assert set(view_graph) == set(ia_graph)
    assert set(view_graph.edges) == set(ia_graph.edges)

    expected_cols = set(
        "/".join(col.split("/")[2:]) for col in ia_view_with_graph.values
    ) | {"internal_point", "geometry"}

    # Previous tests in the test suite can add some values to the graph nodes.
    # so we just check that the expected columns are present.
    assert all(
        expected_cols - set(data) == set() for _, data in view_graph.nodes(data=True)
    )
