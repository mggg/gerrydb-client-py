"""Fixtures for API tests."""
import os
from pathlib import Path

import geopandas as gpd
import pytest

from cherrydb import CherryDB


@pytest.fixture
def client():
    # Requires a running CherryDB server.
    return CherryDB(
        host=os.environ.get("CHERRY_TEST_SERVER", "localhost:8000"),
        key=os.environ.get("CHERRY_TEST_API_KEY"),
    )


@pytest.fixture
def client_ns(request, client):
    """Creates a test-level namespace and associates it with a client."""
    test_name = request.node.name.replace("[", "__").replace("]", "")
    with client.context(
        notes=f"Test setup for cherrydb-client-py test {request.node.name}"
    ) as ctx:
        ctx.namespaces.create(
            path=test_name,
            description=f"cherrydb-client-py test {request.node.name}",
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
