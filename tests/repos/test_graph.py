"""Integration/VCR tests for dual graphs."""


def test_graph_repo_create_get__valid(client_with_ia_layer_loc, ia_graph):
    client_ns, layer, locality, _ = client_with_ia_layer_loc
    with client_ns.context(notes="Uploading a graph for Iowa counties") as ctx:
        graph = ctx.graphs.create(
            path="ia_counties_rook",
            locality=locality,
            layer=layer,
            description="Naive rook adjacency for Iowa counties.",
            proj="epsg:26915",
            graph=ia_graph,
        )
        saved_edges = {
            (path_1.split("/")[-1], path_2.split("/")[-1])
            for path_1, path_2, _ in graph.edges
        }
        assert saved_edges == set(ia_graph.edges)
        assert ctx.graphs["ia_counties_rook"] == graph
