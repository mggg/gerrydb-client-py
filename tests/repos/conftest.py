import os

import pytest

from cherrydb import CherryDB


@pytest.fixture
def client():
    # Requires a running CherryDB server.
    return CherryDB(
        host=os.environ.get("CHERRY_TEST_SERVER", "localhost:8000"),
        key=os.environ.get("CHERRY_TEST_API_KEY"),
    )


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": [("X-API-Key", "DUMMY")],
    }
