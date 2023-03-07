"""Integration/VCR tests for localities."""
import pytest

from cherrydb.schemas import LocalityCreate


@pytest.mark.vcr
def test_locality_repo_create_get__online(client):
    name = "Commonwealth of Massachusetts"
    with client.context(notes="adding a locality") as ctx:
        loc = ctx.localities.create(name=name, canonical_path="ma")
    assert loc.name == name
    assert loc.canonical_path == "ma"

    assert client.localities["ma"] == loc


@pytest.mark.vcr
def test_locality_repo_create_bulk__online(client):
    with client.context(notes="adding localities in bulk") as ctx:
        locs = ctx.localities.create_bulk(
            [
                LocalityCreate(canonical_path="foo", name="Foo"),
                LocalityCreate(canonical_path="bar", name="Bar", aliases=["baz"]),
            ]
        )

    assert len(locs) == 2
    assert set(loc.name for loc in locs) == {"Foo", "Bar"}


@pytest.mark.vcr
def test_locality_repo_create_get__offline(client):
    name = "State of California"
    with client.context(notes="adding a locality") as ctx:
        loc = ctx.localities.create(name=name, canonical_path="ca")

    client.offline = True
    assert client.localities["ca"] == loc


@pytest.mark.vcr
def test_locality_repo_create_all__online(client):
    name = "State of Vermont"
    with client.context(notes="adding a locality") as ctx:
        ctx.localities.create(name=name, canonical_path="vt")

    assert name in [loc.name for loc in client.localities.all()]


@pytest.mark.vcr
def test_locality_repo_create_all__online_offline(client):
    name = "State of New Hampshire"
    with client.context(notes="adding a locality") as ctx:
        ctx.localities.create(name=name, canonical_path="nh")
    client.localities.all()  # Populate cache.

    client.offline = True
    assert name in [loc.name for loc in client.localities.all()]


@pytest.mark.vcr
def test_locality_repo_create_update_get__online_offline(client):
    name = "State of Maryland"
    aliases = ["maryland"]

    with client.context(notes="adding and then updating a locality") as ctx:
        loc = ctx.localities.create(name=name, canonical_path="md")
        assert loc.aliases == []

        updated_loc = ctx.localities.update("md", aliases=aliases)
        assert updated_loc.name == name
        assert updated_loc.aliases == aliases

    client.offline = True
    assert client.localities["md"].aliases == aliases
