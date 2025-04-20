"""Integration/VCR tests for localities."""

import pytest
from gerrydb.schemas import LocalityCreate


@pytest.mark.vcr
def testlocality_repo_create_get(client):
    name = "Commonwealth of Massachusetts"
    with client.context(notes="adding a locality") as ctx:
        loc = ctx.localities.create(name=name, canonical_path="ma2")
    assert loc.name == name
    assert loc.canonical_path == "ma2"

    assert client.localities["ma2"] == loc


@pytest.mark.vcr
def test_locality_repo_create_bulk(client):
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
def test_locality_repo_create_all(client):
    name = "State of Vermont"
    with client.context(notes="adding a locality") as ctx:
        ctx.localities.create(name=name, canonical_path="vt2")

    assert name in [loc.name for loc in client.localities.all()]
