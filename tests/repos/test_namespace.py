"""Integration/VCR tests for namespaces."""
import pytest
from cherrydb.exceptions import OnlineError


@pytest.mark.vcr
def test_namespace_repo_create_get__online(client):
    with client.context(notes="adding a namespace") as ctx:
        namespace = ctx.namespaces.create(
            path="test", public=True, description="Test namespace."
        )
    assert namespace.public is True
    assert namespace.description == "Test namespace."

    assert client.namespaces["test"] == namespace


@pytest.mark.vcr
def test_namespace_repo_create_get__offline(client):
    with client.context(notes="adding a namespace") as ctx:
        namespace = ctx.namespaces.create(
            path="offline", public=True, description="Test namespace."
        )

    client.offline = True
    assert client.namespaces["offline"] == namespace


@pytest.mark.vcr
def test_namespace_repo_create_all__online(client):
    with client.context(notes="adding a namespace") as ctx:
        namespace = ctx.namespaces.create(
            path="all", public=True, description="Test namespace."
        )
    assert "all" in [namespace.path for namespace in client.namespaces.all()]


@pytest.mark.vcr
def test_namespace_repo_create_all__online_offline(client):
    with client.context(notes="adding a namespace") as ctx:
        ctx.namespaces.create(
            path="all_offline", public=True, description="Test namespace."
        )

    client.offline = True
    with pytest.raises(OnlineError):
        client.namespaces.all()
