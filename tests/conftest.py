"""Fixtures for API tests."""
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
