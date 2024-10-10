"""Integration/VCR tests for namespaces."""

import pytest


@pytest.mark.vcr
def test_namespace_repo_create_get(client):
    with client.context(notes="adding a namespace") as ctx:
        namespace = ctx.namespaces.create(
            path="test", public=True, description="Test namespace."
        )
    assert namespace.public is True
    assert namespace.description == "Test namespace."

    assert client.namespaces["test"] == namespace


@pytest.mark.vcr
def test_namespace_repo_create_all(client):
    with client.context(notes="adding a namespace") as ctx:
        ctx.namespaces.create(path="all", public=True, description="Test namespace.")
    assert "all" in [namespace.path for namespace in client.namespaces.all()]
