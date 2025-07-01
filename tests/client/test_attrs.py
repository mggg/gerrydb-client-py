import pytest
import os
from unittest import mock
from pathlib import Path
from types import SimpleNamespace as _BaseNS
import pytest
import httpx

import gerrydb.client as client_mod
from gerrydb.client import GerryDB, WriteContext


class SimpleNamespace(_BaseNS):
    """
    A drop-in replacement for types.SimpleNamespace that also
    implements Pydantic-style .model_dump(mode="json") by returning its own dict.
    """

    def model_dump(self, mode=None) -> dict:
        return self.dict()


class DummyTempDir:
    def __init__(self):
        self.name = "/does/not/exist"
        self.cleaned = False

    def cleanup(self):
        self.cleaned = True


class FakeResponse:
    def __init__(self):
        self.status_code = 201

    def raise_for_status(self):
        pass

    def json(self):
        return {"uuid": "00000000-0000-0000-0000-000000000000"}


class DummyHttpxClient:
    """Stands in for httpx.Client in both roles."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        # also collect posts if someone calls .post on this instance
        self.posts = []

    def post(self, url, json):
        self.posts.append((url, json))
        return FakeResponse()

    def close(self):
        self.closed = True


# Autouse fixture to suppress real HTTP and temp dirs
@pytest.fixture(autouse=True)
def patch_tmp_and_http(monkeypatch, tmp_path):
    # Patch TemporaryDirectory
    monkeypatch.setattr(client_mod, "TemporaryDirectory", lambda: DummyTempDir())
    # Patch httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kw: DummyHttpxClient(**kw))
    yield


@pytest.fixture(autouse=True)
def patch_repos(monkeypatch):
    # For each property, replace the actual repo with DummyRepo
    monkeypatch.setattr(client_mod, "ColumnRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "ColumnSetRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "GeographyRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "GeoLayerRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "GraphRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "LocalityRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "NamespaceRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "PlanRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "ViewRepo", DummyRepo)
    monkeypatch.setattr(client_mod, "ViewTemplateRepo", DummyRepo)
    yield


# Define a generic dummy repo that captures init args
class DummyRepo:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@pytest.fixture(autouse=True)
def patch_everything(monkeypatch, tmp_path):
    monkeypatch.setattr(client_mod, "TemporaryDirectory", lambda: DummyTempDir())

    monkeypatch.setattr(
        "gerrydb.client.ObjectMetaCreate",
        lambda notes: SimpleNamespace(dict=lambda: {"notes": notes}),
    )
    monkeypatch.setattr(
        "gerrydb.client.ObjectMeta",
        lambda **kwargs: SimpleNamespace(uuid=kwargs["uuid"]),
    )

    monkeypatch.setattr(httpx, "Client", lambda **kw: DummyHttpxClient(**kw))

    yield


def test_writecontext_via_gdb_context():
    db = GerryDB(host="localhost:1234", key="APIKEY")
    assert isinstance(db.client, DummyHttpxClient)
    assert hasattr(db._temp_dir, "cleanup")

    with db.context(notes="unit-test-notes") as ctx:
        assert isinstance(ctx, WriteContext)
        assert ctx.db.client.posts == [("/meta/", {"notes": "unit-test-notes"})]
        assert ctx.meta.uuid == "00000000-0000-0000-0000-000000000000"
        expected_headers = {
            **db._base_headers,
            "X-GerryDB-Meta-ID": "00000000-0000-0000-0000-000000000000",
        }
        assert ctx.client_params == {
            "base_url": db._base_url,
            "headers": expected_headers,
            "timeout": db.timeout,
            "transport": db._transport,
        }

        assert isinstance(ctx.client, DummyHttpxClient)
        assert ctx.client.kwargs == ctx.client_params

    assert ctx.client.closed


def test_gdb_context_manager_cleans_up():
    db = GerryDB(host="localhost:1234", key="APIKEY")
    client = db.client
    td = db._temp_dir
    assert isinstance(client, DummyHttpxClient)
    assert hasattr(td, "cleanup")

    with db as same:
        assert same is db

    assert client.closed
    assert td.cleaned
    assert db.client is None
    assert db._temp_dir is None


def test_gerrydb_properties_instantiation():
    # Create instance in in-memory mode
    db = GerryDB(host="localhost:8000", key="KEY123")

    # columns
    col = db.columns
    assert isinstance(col, DummyRepo)
    assert col.kwargs == {
        "schema": client_mod.Column,
        "base_url": "/columns",
        "session": db,
    }

    # column_sets
    colset = db.column_sets
    assert isinstance(colset, DummyRepo)
    assert colset.kwargs == {
        "schema": client_mod.ColumnSet,
        "base_url": "/column-sets",
        "session": db,
    }

    # geo
    geo = db.geo
    assert isinstance(geo, DummyRepo)
    assert geo.kwargs == {
        "schema": client_mod.Geography,
        "base_url": "/geographies",
        "session": db,
    }

    # geo_layers
    gl = db.geo_layers
    assert isinstance(gl, DummyRepo)
    assert gl.kwargs == {
        "schema": client_mod.GeoLayer,
        "base_url": "/layers",
        "session": db,
    }

    # graphs
    graphs = db.graphs
    assert isinstance(graphs, DummyRepo)
    assert graphs.kwargs == {
        "schema": client_mod.Graph,
        "base_url": "/graphs",
        "session": db,
    }

    # localities
    loc = db.localities
    assert isinstance(loc, DummyRepo)
    # LocalityRepo is invoked with session only
    assert loc.kwargs == {"session": db}

    # namespaces
    ns = db.namespaces
    assert isinstance(ns, DummyRepo)
    assert ns.kwargs == {
        "schema": None,
        "base_url": None,
        "session": db,
    }

    # plans
    plans = db.plans
    assert isinstance(plans, DummyRepo)
    assert plans.kwargs == {
        "schema": client_mod.Plan,
        "base_url": "/plans",
        "session": db,
    }

    # views
    views = db.views
    assert isinstance(views, DummyRepo)
    assert views.kwargs == {
        "schema": client_mod.ViewMeta,
        "base_url": "/views",
        "session": db,
    }

    # view_templates
    vtemp = db.view_templates
    assert isinstance(vtemp, DummyRepo)
    assert vtemp.kwargs == {
        "schema": client_mod.ViewTemplate,
        "base_url": "/view-templates",
        "session": db,
    }


@pytest.fixture
def write_ctx():
    # build a WriteContext without ever calling __enter__
    db = GerryDB(host="localhost:8000", key="KEY123", namespace="test")
    return WriteContext(db=db, notes="testing notes")


def test_write_context_properties_instantiation(monkeypatch, write_ctx):
    # patch each repo class in the client module to DummyRepo
    for repo_name in [
        "ColumnRepo",
        "ColumnSetRepo",
        "GeographyRepo",
        "GeoLayerRepo",
        "GraphRepo",
        "LocalityRepo",
        "NamespaceRepo",
        "PlanRepo",
        "ViewRepo",
        "ViewTemplateRepo",
    ]:
        monkeypatch.setattr(client_mod, repo_name, DummyRepo)

    col = write_ctx.columns
    assert isinstance(col, DummyRepo)
    assert col.kwargs == {
        "schema": client_mod.Column,
        "base_url": "/columns",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    colset = write_ctx.column_sets
    assert isinstance(colset, DummyRepo)
    assert colset.kwargs == {
        "schema": client_mod.ColumnSet,
        "base_url": "/column-sets",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    geo = write_ctx.geo
    assert isinstance(geo, DummyRepo)
    assert geo.kwargs == {
        "schema": client_mod.Geography,
        "base_url": "/geographies",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    gl = write_ctx.geo_layers
    assert isinstance(gl, DummyRepo)
    assert gl.kwargs == {
        "schema": client_mod.GeoLayer,
        "base_url": "/layers",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    graphs = write_ctx.graphs
    assert isinstance(graphs, DummyRepo)
    assert graphs.kwargs == {
        "schema": client_mod.Graph,
        "base_url": "/graphs",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    loc = write_ctx.localities
    assert isinstance(loc, DummyRepo)
    # LocalityRepo is invoked with session=self.db, ctx=self
    assert loc.kwargs == {
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    ns = write_ctx.namespaces
    assert isinstance(ns, DummyRepo)
    assert ns.kwargs == {
        "schema": None,
        "base_url": None,
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    plans = write_ctx.plans
    assert isinstance(plans, DummyRepo)
    assert plans.kwargs == {
        "schema": client_mod.Plan,
        "base_url": "/plans",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    views = write_ctx.views
    assert isinstance(views, DummyRepo)
    assert views.kwargs == {
        "schema": client_mod.ViewMeta,
        "base_url": "/views",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }

    vtemp = write_ctx.view_templates
    assert isinstance(vtemp, DummyRepo)
    assert vtemp.kwargs == {
        "schema": client_mod.ViewTemplate,
        "base_url": "/view-templates",
        "session": write_ctx.db,
        "ctx": write_ctx,
    }
