"""Fixtures for API tests."""
import json
import os
from pathlib import Path

import geopandas as gpd
import pytest
from networkx.readwrite import json_graph

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
    with client.context(
        notes=f"Test setup for gerrydb-client-py test {request.node.name}"
    ) as ctx:
        ctx.namespaces.create(
            path=test_name,
            description=f"gerrydb-client-py test {request.node.name}",
            public=True,
        )

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
        ctx.namespaces.create(
            path="plan",
            description="gerrydb-client-py plan repository tests",
            public=True,
        )

    client.namespace = "plan"

    with client.context(notes="Importing Iowa counties shapefile") as ctx:
        columns = {
            name: ctx.columns.create(**meta) for name, meta in ia_column_meta.items()
        }
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
        ctx.load_dataframe(
            df=ia_dataframe,
            columns=columns,
            create_geo=True,
            namespace=client.namespace,
            layer=layer,
            locality=locality,
        )

    return client, layer, locality, columns
