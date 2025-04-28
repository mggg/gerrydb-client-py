"""Integration/VCR tests for localities."""

import pytest
from gerrydb.schemas import LocalityCreate
from gerrydb.repos.locality import LocalityRepo
from gerrydb.exceptions import ResultError
from types import SimpleNamespace
import logging


@pytest.mark.vcr
def testlocality_repo_create_get(client):
    name = "Commonwealth of Massachusetts"
    with client.context(notes="adding a locality") as ctx:
        loc = ctx.localities.create(name=name, canonical_path="ma2")
    assert loc.name == name
    assert loc.canonical_path == "ma2"

    assert client.localities["ma2"] == loc


@pytest.mark.vcr
def testlocality_repo_create_patch(client):
    name = "Commonwealth of Massachusetts V2"
    with client.context(notes="adding a locality") as ctx:
        loc = ctx.localities.create(name=name, canonical_path="mav2")
    assert loc.name == name
    assert loc.canonical_path == "mav2"

    with client.context(notes="updating a locality") as ctx:
        loc = ctx.localities.update("mav2", aliases=["new_ma"])
    assert client.localities["new_ma"] == loc
    assert loc.canonical_path == "mav2"
    assert loc.aliases == ["new_ma"]


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


def _unwrap(fn):
    """Unwraps decorators from a function."""
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


def test_create_bulk_logs_and_swallows_duplicate(caplog, monkeypatch):
    caplog.set_level(logging.ERROR, logger="gerrydb")
    logging.getLogger("gerrydb").addHandler(caplog.handler)

    def fake_create(self, canonical_path, name, parent_path, default_proj, aliases):
        if canonical_path == "alreadyexists":
            raise ResultError("Failed to create canonical path to new location(s).")
        return SimpleNamespace(canonical_path=canonical_path, name=name)

    monkeypatch.setattr(LocalityRepo, "create", fake_create)

    repo = LocalityRepo(session=None)

    locs = [
        LocalityCreate(canonical_path="bar", name="Bar", aliases=["baz"]),
        LocalityCreate(canonical_path="alreadyexists", name="alreadyexists"),
    ]

    raw = _unwrap(LocalityRepo.create_bulk)
    result = raw(repo, locs)

    assert isinstance(result[0], SimpleNamespace)
    assert result[1] == -1
    assert "Failed to create alreadyexists, path already exists" in caplog.text


def test_create_bulk_logs_throws_other_errors(caplog, monkeypatch):
    caplog.set_level(logging.ERROR, logger="gerrydb")
    logging.getLogger("gerrydb").addHandler(caplog.handler)

    def fake_create(self, canonical_path, name, parent_path, default_proj, aliases):
        raise ResultError("Bad thing happened")

    monkeypatch.setattr(LocalityRepo, "create", fake_create)

    repo = LocalityRepo(session=None)

    locs = [
        LocalityCreate(canonical_path="bar", name="Bar", aliases=["baz"]),
        LocalityCreate(canonical_path="alreadyexists", name="alreadyexists"),
    ]

    raw = _unwrap(LocalityRepo.create_bulk)
    with pytest.raises(ResultError, match="Bad thing happened"):
        raw(repo, locs)


@pytest.mark.vcr
def test_locality_repo_create_all(client):
    name = "State of Vermont"
    with client.context(notes="adding a locality") as ctx:
        ctx.localities.create(name=name, canonical_path="vt2")

    assert name in [loc.name for loc in client.localities.all()]
