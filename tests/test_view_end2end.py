import pytest
import networkx as nx
import httpx
from shapely.geometry import Polygon, Point
from gerrydb.exceptions import ForkingError
import pandas as pd


def graphs_equal(G1: nx.Graph, G2: nx.Graph) -> bool:
    # Quick check: same node‑set and same edge‑set
    if set(G1.nodes) != set(G2.nodes):
        return False
    edge_set_1 = set([tuple(sorted(e)) for e in G1.edges])
    edge_set_2 = set([tuple(sorted(e)) for e in G1.edges])
    if edge_set_1 != edge_set_2:
        return False

    return True


# FIXME: All of the tests that actually get views in the end need to have the columns tested!!!


@pytest.mark.slow
def test_basic_view(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
):
    census10 = "census.2010_test01"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    new_col_names = {k: v.canonical_path for k, v in columns10.items()} | {
        "geometry": "geometry",
        "internal_point": "internal_point",
    }
    me_2010_gdf = me_2010_gdf.rename(columns=new_col_names).filter(
        items=new_col_names.values()
    )

    me_2010_gdf.to_crs(epsg=4267, inplace=True)

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=me_2010_gdf.columns,
            create_geos=True,
            locality="maine",
            layer="county",
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", "aland"],
            namespace=census10,
            description="Some test views.",
        )

    with client.context(notes="Now making the views") as ctx:
        land_view = ctx.views.create(
            path="test_land",
            namespace=census10,
            template=land_template,
            locality="maine",
            layer=layer10,
        )

    me_2010_gdf = me_2010_gdf.to_crs(epsg=4269)

    land_df = land_view.to_df(plans=True, internal_points=True)
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2010_gdf["area_land"])
    assert land_df["area_water"].equals(me_2010_gdf["area_water"])
    assert land_df["geometry"].equals(me_2010_gdf["geometry"])

    for geo in land_view.geographies:
        assert geo.geography.equals(land_df.loc[geo.path, "geometry"])

    for p1, p2 in zip(land_df.internal_point, me_2010_gdf.internal_point):
        assert p1.equals_exact(p2, tolerance=8), f"{p1} != {p2}"


@pytest.mark.slow
def test_basic_view_with_graph_no_plan(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2010_nx_graph,
):
    census10 = "census.2010_test02"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    with client.context(notes="Now making test view graph") as ctx:
        view_graph = ctx.graphs.create(
            graph=me_2010_nx_graph,
            path="test_graph",
            locality=root_loc,
            layer=layer10,
            description="Test graph for Maine counties.",
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", "aland"],
            namespace=census10,
            description="Some test views.",
        )

    with client.context(notes="Now making the views") as ctx:
        _ = ctx.views.create(
            path="test_land",
            namespace=census10,
            template=land_template,
            locality=root_loc,
            layer=layer10,
            graph=view_graph,
        )

    land_view = client.views.get("test_land")

    land_df = land_view.to_df(plans=True, internal_points=True)
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2010_gdf["ALAND10"])
    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])
    assert land_df["geometry"].equals(me_2010_gdf["geometry"])
    graph_out = land_view.to_graph(plans=True, geometry=True)
    assert graphs_equal(graph_out, me_2010_nx_graph)


@pytest.mark.slow
def test_basic_view_with_graph_and_plan(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2010_nx_graph,
    me_2010_plan_dict,
):
    census10 = "census.2010_test03"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    me_2010_gdf["total_pop"] = pd.Series(
        {
            "23029": 32856,
            "23005": 281674,
            "23017": 57833,
            "23003": 71870,
            "23025": 52228,
            "23009": 54418,
            "23023": 35293,
            "23019": 153923,
            "23015": 34457,
            "23031": 197131,
            "23013": 39736,
            "23021": 17535,
            "23001": 107702,
            "23011": 122151,
            "23027": 38786,
            "23007": 30768,
        }
    )

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )
        ctx.columns.create(
            "total_pop",
            aliases=["totpop", "p001001", "p0001001"],
            column_kind="count",
            column_type="int",
            description="2010 Census total population",
            source_url="https://www.census.gov/",
        )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    columns10.update({"total_pop": client.columns["total_pop"]})

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    with client.context(notes="Creating a plan for Maine counties") as ctx:
        ctx.plans.create(
            path="test_plan",
            locality=root_loc,
            layer=layer10,
            assignments=me_2010_plan_dict,
            description="Test plan for Maine counties.",
        )

    with client.context(notes="Now making test view graph") as ctx:
        view_graph = ctx.graphs.create(
            graph=me_2010_nx_graph,
            path="test_graph",
            locality=root_loc,
            layer=layer10,
            description="Test graph for Maine counties.",
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", "aland", "total_pop"],
            namespace=census10,
            description="Some test views.",
        )

    with client.context(notes="Now making the views") as ctx:
        land_view = ctx.views.create(
            path="test_land",
            namespace=census10,
            template=land_template,
            locality=root_loc,
            layer=layer10,
            graph=view_graph,
        )

    land_df = land_view.to_df(plans=True, internal_points=True)
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2010_gdf["ALAND10"])
    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])
    assert land_df["geometry"].equals(me_2010_gdf["geometry"])
    assert (
        land_df["test_plan"]
        .astype(int)
        .equals(pd.Series(me_2010_plan_dict).sort_index())
    )
    partition_dict = land_view.to_partition_dict(autotally=True)
    assert set(partition_dict["test_plan"].updaters.keys()) == set(
        ["cut_edges", "total_pop"]
    )
    assert partition_dict["test_plan"]["total_pop"] == {"0": 684021, "1": 644340}
    assert (
        partition_dict["test_plan"].assignment.to_series().astype(int).to_dict()
        == me_2010_plan_dict
    )

    graph_out = land_view.to_graph(plans=True, geometry=True)
    assert graphs_equal(graph_out, me_2010_nx_graph)


@pytest.mark.slow
def test_view_repo_fork_column_conflict_with_maine(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2020_gdf,
    me_2020_column_tabluation,
):
    census10 = "census.2010_test1"
    census20 = "census.2020_test1"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10, census20]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]
    layer20 = client.geo_layers[(census20, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    # ====================
    # CHANGE THE NAMESPACE
    # ====================
    client.namespace = census20
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2020_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns20 = {
        col.source: client.columns[col.target]
        for col in me_2020_column_tabluation.columns
        if col.source in me_2020_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2020_gdf,
            columns=columns20,
            create_geos=True,
            locality=root_loc,
            layer=layer20,
        )

    client.namespace = census10
    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", f"{census20}/aland"],
            namespace=census10,
            description="Some test views.",
        )

    with pytest.raises(httpx.HTTPStatusError, match="409 Conflict"):
        with client.context(notes="Now making the views") as ctx:
            ctx.views.create(
                path="test_land",
                namespace=census10,
                template=land_template,
                locality=root_loc,
                layer=layer10,
            )


@pytest.mark.slow
def test_basic_view_no_geos_errors_on_empty_polys(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
):
    census10 = "census.2010_test2"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    me_2010_gdf = me_2010_gdf.drop(columns=["geometry", "internal_point"])
    with pytest.raises(ValueError, match="No 'geometry' column found in dataframe"):
        with client.context(
            notes="Creating a view template and view for Maine counties"
        ) as ctx:
            ctx.load_dataframe(
                df=me_2010_gdf,
                columns=columns10,
                create_geos=True,
                locality=root_loc,
                layer=layer10,
                include_geos=False,
            )


@pytest.mark.slow
def test_basic_view_no_geos_and_allow_empty_polys(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
):
    census10 = "census.2010_test3"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
            allow_empty_polys=True,
            include_geos=False,
        )

        column_set = ctx.column_sets.create(
            path="test_column_set",
            columns=["full_name", "lsad"],
            description="Some test column set.",
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater"],
            column_sets=[column_set],
            namespace=census10,
            description="Some test views.",
        )

    with client.context(notes="Now making the views") as ctx:
        land_view = ctx.views.create(
            path="test_land",
            namespace=census10,
            template=land_template,
            locality=root_loc,
            layer=layer10,
        )

    land_df = land_view.to_df(internal_points=True)
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)

    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])
    assert land_df["full_name"].equals(me_2010_gdf["NAMELSAD10"])
    assert land_df["lsad"].equals(me_2010_gdf["LSAD10"])
    assert all([geo == Polygon() for geo in land_df["geometry"]])
    assert all([pt == Point() for pt in land_df["internal_point"]])


@pytest.mark.slow
def test_patch_view_with_new_geos(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2020_gdf,
    me_2020_column_tabluation,
):
    census10 = "census.2010_test4"
    census20 = "census.2020_test4"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10, census20]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    me_2010_gdf = me_2010_gdf.drop(columns=["geometry", "internal_point"])
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
            allow_empty_polys=True,
            include_geos=False,
        )

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2020_column_tabluation.columns:
            ctx.columns.update(
                path=col.target,
                aliases=col.aliases,
            )

    columns20 = {
        col.source: client.columns[col.target]
        for col in me_2020_column_tabluation.columns
        if col.source in me_2020_gdf.columns
    }
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2020_gdf,
            columns=columns20,
            locality=root_loc,
            layer=layer10,
            patch_geos=True,
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", "aland"],
            namespace=census10,
            description="Some test views.",
        )

    with client.context(notes="Now making the views") as ctx:
        land_view = ctx.views.create(
            path="test_land",
            namespace=census10,
            template=land_template,
            locality=root_loc,
            layer=layer10,
        )

    land_df = land_view.to_df(internal_points=True)
    land_df.sort_index(inplace=True)
    me_2020_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2020_gdf["ALAND20"])
    assert land_df["area_water"].equals(me_2020_gdf["AWATER20"])
    assert land_df["geometry"].equals(me_2020_gdf["geometry"])


@pytest.mark.slow
def test_patch_with_empty_polys(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2020_gdf,
    me_2020_column_tabluation,
):
    census10 = "census.2010_test5"
    census20 = "census.2020_test5"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10, census20]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    me_2010_gdf = me_2010_gdf.drop(columns=["geometry", "internal_point"])
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
            allow_empty_polys=True,
            include_geos=False,
        )

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2020_column_tabluation.columns:
            ctx.columns.update(
                path=col.target,
                aliases=col.aliases,
            )

    columns20 = {
        col.source: client.columns[col.target]
        for col in me_2020_column_tabluation.columns
        if col.source in me_2020_gdf.columns
    }
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2020_gdf,
            columns=columns20,
            locality=root_loc,
            layer=layer10,
            patch_geos=True,
        )

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            patch_geos=True,
            locality=root_loc,
            layer=layer10,
            allow_empty_polys=True,
            include_geos=False,
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", "aland"],
            namespace=census10,
            description="Some test views.",
        )

    with client.context(notes="Now making the views") as ctx:
        land_view = ctx.views.create(
            path="test_land",
            namespace=census10,
            template=land_template,
            locality=root_loc,
            layer=layer10,
        )

    land_df = land_view.to_df()
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2010_gdf["ALAND10"])
    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])
    assert all([geo == Polygon() for geo in land_df["geometry"]])


@pytest.mark.slow
def test_view_empty_polys_both_namespaces(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2020_gdf,
    me_2020_column_tabluation,
):
    census10 = "census.2010_test6"
    census20 = "census.2020_test6"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10, census20]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    me_2010_gdf = me_2010_gdf.drop(columns=["geometry", "internal_point"])
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            patch_geos=True,
            locality=root_loc,
            layer=layer10,
            allow_empty_polys=True,
            include_geos=False,
        )

    # ====================
    # CHANGE THE NAMESPACE
    # ====================
    client.namespace = census20
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2020_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    me_2020_gdf = me_2020_gdf.drop(columns=["geometry", "internal_point"])
    columns20 = {
        col.source: client.columns[col.target]
        for col in me_2020_column_tabluation.columns
        if col.source in me_2020_gdf.columns
    }
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2020_gdf,
            columns=columns20,
            locality=root_loc,
            layer=layer10,
            create_geos=True,
            allow_empty_polys=True,
            include_geos=False,
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", (census20, "aland")],
            namespace=census10,
            description="Some test views.",
        )

    with client.context(notes="Now making the views") as ctx:
        land_view = ctx.views.create(
            path="test_land",
            namespace=census10,
            template=land_template,
            locality=root_loc,
            layer=layer10,
        )

    land_df = land_view.to_df()
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)
    me_2020_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2020_gdf["ALAND20"])
    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])
    assert all([geo == Polygon() for geo in land_df["geometry"]])


@pytest.mark.slow
def test_patching_with_incompatible_geos_causes_fork_error(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2020_gdf,
    me_2020_column_tabluation,
):
    census10 = "census.2010_test7"
    census20 = "census.2020_test7"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10, census20]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]
    layer20 = client.geo_layers[(census20, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    # ====================
    # CHANGE THE NAMESPACE
    # ===================
    client.namespace = census20
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2020_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )
        for col in me_2010_column_tabluation.columns:
            ctx.columns.update(
                path=col.target,
                aliases=col.aliases,
            )

    # Trick to make it possible to upload 2010 df into 2020 namespace
    columns20 = {
        col.source.replace("20", "10"): client.columns[col.target]
        for col in me_2020_column_tabluation.columns
        if col.source in me_2020_gdf.columns
    }
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns20,
            locality=root_loc,
            layer=layer10,
            create_geos=True,
        )

    # ====================
    # CHANGE THE NAMESPACE
    # ===================
    client.namespace = census10
    me_2010_gdf2 = me_2010_gdf.drop(columns=["geometry", "internal_point"])

    with client.context(notes="Patching the old 2010 geos") as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf2,
            columns=columns10,
            patch_geos=True,
            locality=root_loc,
            layer=layer10,
            allow_empty_polys=True,
            include_geos=False,
        )

    # ====================
    # CHANGE THE NAMESPACE
    # ===================
    client.namespace = census20
    with pytest.raises(
        ForkingError,
        match=(
            "some geometries in the target namespace/layer are different from the "
            "geometries in the source namespace/layer."
        ),
    ):
        with client.context(notes="Trying to crash the system.") as ctx:
            ctx.load_dataframe(
                df=me_2010_gdf,
                columns=columns20,
                patch_geos=True,
                locality=root_loc,
                layer=layer10,
                allow_empty_polys=True,
            )


@pytest.mark.slow
def test_several_cross_namespace_views(
    client_with_census_namespaces_and_columns,
    me_2010_gdf,
    me_2010_column_tabluation,
    me_2020_gdf,
    me_2020_column_tabluation,
):
    census10 = "census.2010_test8"
    census20 = "census.2020_test8"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10, census20]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )
    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]
    layer20 = client.geo_layers[(census20, "county")]

    client.namespace = census10
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    # ====================
    # CHANGE THE NAMESPACE
    # ===================
    client.namespace = census20
    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2020_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns20 = {
        col.source: client.columns[col.target]
        for col in me_2020_column_tabluation.columns
        if col.source in me_2020_gdf.columns
    }

    # Update the geometry and internal point columns to match the 2010 ones
    me_2020_gdf["geometry"] = me_2010_gdf["geometry"]
    me_2020_gdf["internal_point"] = me_2010_gdf["internal_point"]

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2020_gdf,
            columns=columns20,
            locality=root_loc,
            layer=layer20,
            create_geos=True,
        )

    # ====================
    # CHANGE THE NAMESPACE
    # ===================
    client.namespace = census10
    with client.context(notes="Making some test views") as ctx:
        column_set = ctx.column_sets.create(
            path="test_column_set",
            columns=["full_name", "lsad"],
            description="Some test column set.",
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", (census20, "aland")],
            description="Some test views.",
        )

        land_view = ctx.views.create(
            path="test_land",
            template=land_template,
            locality=root_loc,
            layer=layer10,
        )

        set_template = ctx.view_templates.create(
            path="test_set",
            column_sets=[column_set],
            description="Some test views.",
        )

        set_view = ctx.views.create(
            path="test_set",
            template=set_template,
            locality=root_loc,
            layer=layer10,
        )

        full_template = ctx.view_templates.create(
            path="test_full",
            columns=["awater", (census20, "aland")],
            column_sets=[column_set],
            description="Some test views.",
        )

        full_view = ctx.views.create(
            path="test_full",
            template=full_template,
            locality=root_loc,
            layer=layer10,
        )

    land_df = land_view.to_df(internal_points=True)
    land_df.sort_index(inplace=True)
    set_df = set_view.to_df(internal_points=True)
    set_df.sort_index(inplace=True)
    full_df = full_view.to_df(internal_points=True)
    full_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)
    me_2020_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2020_gdf["ALAND20"])
    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])
    assert land_df["geometry"].equals(me_2020_gdf["geometry"])
    assert set_df["full_name"].equals(me_2010_gdf["NAMELSAD10"])
    assert set_df["lsad"].equals(me_2010_gdf["LSAD10"])
    assert set_df["geometry"].equals(me_2020_gdf["geometry"])
    assert set_df["internal_point"].equals(land_df["internal_point"])
    assert full_df["area_land"].equals(land_df["area_land"])
    assert full_df["area_water"].equals(land_df["area_water"])
    assert full_df["geometry"].equals(land_df["geometry"])
    assert full_df["internal_point"].equals(land_df["internal_point"])
    assert full_df["full_name"].equals(set_df["full_name"])
    assert full_df["lsad"].equals(set_df["lsad"])


@pytest.mark.slow
def test_basic_view_update_column(
    client_with_census_namespaces_and_columns, me_2010_gdf, me_2010_column_tabluation
):
    census10 = "census.2010_test9"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    me_2010_gdf.at[me_2010_gdf.index[0], "ALAND10"] = 10
    me_2010_gdf.at[me_2010_gdf.index[1], "AWATER10"] = 11

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            locality=root_loc,
            layer=layer10,
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", "aland"],
            description="Some test views.",
        )

        land_view = ctx.views.create(
            path="test_land",
            template=land_template,
            locality=root_loc,
            layer=layer10,
        )

    land_df = land_view.to_df()
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2010_gdf["ALAND10"])
    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])
    assert land_df["geometry"].equals(me_2010_gdf["geometry"])


@pytest.mark.slow
def test_basic_view_update_column_bad_geos(
    client_with_census_namespaces_and_columns, me_2010_gdf, me_2010_column_tabluation
):
    census10 = "census.2010_test10"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    me_2010_gdf.at[me_2010_gdf.index[0], "ALAND10"] = 10
    me_2010_gdf.at[me_2010_gdf.index[1], "AWATER10"] = 11
    me_2010_gdf["geometry"] = me_2010_gdf.at[me_2010_gdf.index[0], "geometry"]
    me_2010_gdf["internal_point"] = me_2010_gdf.at[
        me_2010_gdf.index[0], "internal_point"
    ]

    with pytest.raises(
        ValueError, match="Conflicting geometries found in dataframe and passed layer."
    ):
        with client.context(
            notes="Creating a view template and view for Maine counties"
        ) as ctx:
            ctx.load_dataframe(
                df=me_2010_gdf,
                columns=columns10,
                locality=root_loc,
                layer=layer10,
            )


@pytest.mark.slow
def test_basic_view_update_column_no_geos(
    client_with_census_namespaces_and_columns, me_2010_gdf, me_2010_column_tabluation
):
    census10 = "census.2010_test11"
    client = client_with_census_namespaces_and_columns

    with client.context(notes="Creating namespaces") as ctx:
        for namespace in [census10]:
            ctx.namespaces.create(
                path=namespace,
                description=f"gerrydb-client-py {namespace} namespace for testing",
                public=True,
            )

            for layer in ["state", "county", "tract", "bg", "block", "vtd"]:
                ctx.geo_layers.create(
                    namespace=namespace,
                    path=layer,
                    description=f"{namespace.split('.')[-1]} U.S. Census {layer} layer.",
                    source_url="https://www.census.gov/",
                )

    root_loc = client.localities["maine"]
    layer10 = client.geo_layers[(census10, "county")]

    client.namespace = census10

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        for col in me_2010_column_tabluation.columns:
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url="https://imadeitup.com",
            )

    columns10 = {
        col.source: client.columns[col.target]
        for col in me_2010_column_tabluation.columns
        if col.source in me_2010_gdf.columns
    }

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf,
            columns=columns10,
            create_geos=True,
            locality=root_loc,
            layer=layer10,
        )

    me_2010_gdf.at[me_2010_gdf.index[0], "ALAND10"] = 10
    me_2010_gdf.at[me_2010_gdf.index[1], "AWATER10"] = 11

    me_2010_gdf2 = me_2010_gdf.drop(columns=["geometry", "internal_point"])

    with pytest.raises(
        ValueError,
        match="`include_geos` is True, but no 'geometry' column found in dataframe.",
    ):
        with client.context(
            notes="Creating a view template and view for Maine counties"
        ) as ctx:
            ctx.load_dataframe(
                df=me_2010_gdf2,
                columns=columns10,
                locality=root_loc,
                layer=layer10,
            )

    with client.context(
        notes="Creating a view template and view for Maine counties"
    ) as ctx:
        ctx.load_dataframe(
            df=me_2010_gdf2,
            columns=columns10,
            locality=root_loc,
            layer=layer10,
            include_geos=False,
        )

    with client.context(notes="Now making the view templates") as ctx:
        land_template = ctx.view_templates.create(
            path="test_land",
            columns=["awater", "aland"],
            description="Some test views.",
        )

        land_view = ctx.views.create(
            path="test_land",
            template=land_template,
            locality=root_loc,
            layer=layer10,
            use_locality_proj=True,
        )

    land_df = land_view.to_df(internal_points=True)
    land_df.sort_index(inplace=True)
    me_2010_gdf.sort_index(inplace=True)

    assert land_df["area_land"].equals(me_2010_gdf["ALAND10"])
    assert land_df["area_water"].equals(me_2010_gdf["AWATER10"])

    import geopandas as gpd

    land_geo = gpd.GeoDataFrame(
        land_df, geometry="geometry", crs=land_view.proj
    ).to_crs(me_2010_gdf.crs)

    # Check for equality up to floating point tolerance from reprojection
    for p1, p2 in zip(land_geo.geometry, me_2010_gdf.geometry):
        assert p1.equals_exact(p2, tolerance=8), f"{p1} != {p2}"
