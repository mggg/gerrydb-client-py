"""Integration/VCR tests for dual graphs."""

import networkx as nx


def graphs_equal(G1: nx.Graph, G2: nx.Graph) -> bool:
    # Quick check: same node‑set and same edge‑set
    if set(G1.nodes) != set(G2.nodes):
        return False
    if set(G1.edges) != set(G2.edges):
        return False

    return True


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
            for path_1, path_2 in graph.graph.edges
        }
        sorted_saved_edges = set([tuple(sorted(edge)) for edge in saved_edges])
        sorted_ia_graph_edges = set([tuple(sorted(edge)) for edge in ia_graph.edges])
        assert sorted_saved_edges == sorted_ia_graph_edges

        retrieved_graph = ctx.graphs["ia_counties_rook"]
        assert graphs_equal(graph.graph, retrieved_graph.graph)
        assert graph.namespace == retrieved_graph.namespace
        assert graph.path == retrieved_graph.path
        assert graph.locality == retrieved_graph.locality
        assert graph.layer == retrieved_graph.layer
        assert graph.meta == retrieved_graph.meta
        assert graph.created_at == retrieved_graph.created_at
        assert graph.proj == retrieved_graph.proj
