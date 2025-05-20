"""Fixtures for API tests."""

import json
import os
from pathlib import Path
import pickle

import geopandas as gpd
import pytest
from networkx.readwrite import json_graph
import networkx as nx

from gerrydb import GerryDB


@pytest.fixture
def client():
    # Requires a running GerryDB server.
    return GerryDB(
        host=os.environ.get("GERRYDB_TEST_SERVER", "localhost:8000"),
        key=os.environ.get("GERRYDB_TEST_API_KEY"),
    )


@pytest.fixture
def client_ns(request, client):
    """Creates a test-level namespace and associates it with a client."""
    test_name = request.node.name.replace("[", "__").replace("]", "")
    try:
        with client.context(
            notes=f"Test setup for gerrydb-client-py test {request.node.name}"
        ) as ctx:
            ctx.namespaces.create(
                path=test_name,
                description=f"gerrydb-client-py test {request.node.name}",
                public=True,
            )

    except Exception:
        pass

    client.namespace = test_name
    return client


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": [("X-API-Key", "DUMMY")],
    }


@pytest.fixture(scope="session")
def ia_dataframe():
    """`GeoDataFrame` of Iowa counties."""
    shp_path = Path(__file__).resolve().parent / "fixtures" / "tl_2020_19_county20.zip"
    return gpd.read_file(shp_path).set_index("GEOID20")


@pytest.fixture(scope="session")
def ia_graph():
    """NetworkX `Graph` of Iowa counties."""
    graph_path = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "tl_2020_19_county20__rook_epsg26915.json"
    )
    with open(graph_path) as graph_fp:
        return json_graph.adjacency_graph(json.loads(graph_fp.read()))


@pytest.fixture(scope="session")
def ia_column_meta():
    """Metadata for selected columns in the Iowa counties fixture."""
    return {
        "NAME20": {
            "path": "name",
            "description": "2020 Census name",
            "source_url": "https://www.census.gov/",
            "column_kind": "identifier",
            "column_type": "str",
        },
        "FUNCSTAT20": {
            "path": "funcstat",
            "description": "2020 Census 2020 Census functional status.",
            "source_url": "https://www.census.gov/",
            "column_kind": "categorical",
            "column_type": "str",
        },
    }


@pytest.fixture(scope="session")
def client_with_ia_layer_loc(ia_dataframe, ia_column_meta):
    """A namespaced client with a `GeoLayer` and `Locality` for Iowa counties."""
    client = GerryDB(
        host=os.environ.get("GERRYDB_TEST_SERVER", "localhost:8000"),
        key=os.environ.get("GERRYDB_TEST_API_KEY"),
    )

    with client.context(
        notes=f"Test setup for gerrydb-client-py plan repository tests",
    ) as ctx:
        try:
            ctx.namespaces.create(
                path="plan_ns",
                description="gerrydb-client-py plan repository tests",
                public=True,
            )
        except Exception:
            pass

    client.namespace = "plan_ns"

    with client.context(notes="Importing Iowa counties shapefile") as ctx:
        columns = {
            name: ctx.columns.create(**meta) for name, meta in ia_column_meta.items()
        }
        try:
            layer = ctx.geo_layers.create(
                path="counties",
                description="2020 U.S. Census counties.",
                source_url="https://www.census.gov/",
            )
            locality = ctx.localities.create(
                canonical_path="iowa",
                name="State of Iowa",
                aliases=["ia", "19"],
                default_proj="epsg:26915",  # UTM zone 15N
            )
        except Exception:
            locality = ctx.localities["iowa"]
            layer = ctx.geo_layers["counties"]

        ctx.load_dataframe(
            df=ia_dataframe,
            columns=columns,
            create_geos=True,
            namespace=client.namespace,
            layer=layer,
            locality=locality,
        )

    return client, layer, locality, columns


@pytest.fixture(scope="session")
def client_with_census_namespaces_and_columns():
    """A namespaced client with a `GeoLayer` and `Locality` for Iowa counties."""
    client = GerryDB(
        host=os.environ.get("GERRYDB_TEST_SERVER", "localhost:8000"),
        key=os.environ.get("GERRYDB_TEST_API_KEY"),
    )

    with client.context(
        notes=f"Test setup for gerrydb-client-py plan repository tests",
    ) as ctx:
        try:
            ctx.localities.create(
                canonical_path="maine",
                name="State of Maine",
                aliases=["me", "23"],
                default_proj="epsg:26919",
            )

        except Exception:
            pass

    return client


@pytest.fixture(scope="session")
def me_2010_gdf():
    """`GeoDataFrame` of Maine 2010 Census blocks."""
    pkl_path = (
        Path(__file__).resolve().parent / "fixtures" / "23_county_all_geos_2010.pkl"
    )

    with open(pkl_path, "rb") as pkl_fp:
        gdf = pickle.load(pkl_fp)

    return gdf


@pytest.fixture(scope="session")
def me_2020_gdf():
    """`GeoDataFrame` of Maine 2020 Census blocks."""
    pkl_path = (
        Path(__file__).resolve().parent / "fixtures" / "23_county_all_geos_2020.pkl"
    )

    with open(pkl_path, "rb") as pkl_fp:
        gdf = pickle.load(pkl_fp)

    return gdf


@pytest.fixture(scope="session")
def me_2010_column_tabluation():
    pkl_path = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "tabular_config_geo_columns_2010.pkl"
    )

    with open(pkl_path, "rb") as pkl_fp:
        gdf = pickle.load(pkl_fp)

    return gdf


@pytest.fixture(scope="session")
def me_2020_column_tabluation():
    pkl_path = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "tabular_config_geo_columns_2020.pkl"
    )

    with open(pkl_path, "rb") as pkl_fp:
        gdf = pickle.load(pkl_fp)

    return gdf


@pytest.fixture
def me_2010_nx_graph():
    return nx.from_edgelist(
        [
            ("23029", "23009"),
            ("23029", "23019"),
            ("23029", "23003"),
            ("23005", "23031"),
            ("23005", "23023"),
            ("23005", "23001"),
            ("23005", "23017"),
            ("23017", "23031"),
            ("23017", "23001"),
            ("23017", "23007"),
            ("23003", "23025"),
            ("23003", "23019"),
            ("23003", "23021"),
            ("23025", "23011"),
            ("23025", "23007"),
            ("23025", "23027"),
            ("23025", "23019"),
            ("23025", "23021"),
            ("23009", "23013"),
            ("23009", "23027"),
            ("23009", "23019"),
            ("23023", "23001"),
            ("23023", "23011"),
            ("23023", "23015"),
            ("23019", "23027"),
            ("23019", "23021"),
            ("23015", "23011"),
            ("23015", "23013"),
            ("23015", "23027"),
            ("23013", "23027"),
            ("23001", "23011"),
            ("23001", "23007"),
            ("23011", "23007"),
            ("23011", "23027"),
        ]
    )


@pytest.fixture
def me_2010_plan_dict():
    return {
        "23009": 0,
        "23013": 0,
        "23021": 0,
        "23023": 0,
        "23019": 0,
        "23011": 0,
        "23007": 0,
        "23003": 0,
        "23029": 0,
        "23015": 0,
        "23025": 0,
        "23027": 0,
        "23031": 1,
        "23001": 1,
        "23017": 1,
        "23005": 1,
    }
