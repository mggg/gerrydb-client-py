import pytest
from click.testing import CliRunner
from gerrydb.create import cli
from gerrydb.exceptions import ResultError


class DummyNamespaceRepo:
    def __init__(self):
        self.called = False
        self.args = None

    def create(self, path, description, public):
        self.called = True
        self.args = (path, description, public)


class DummyGeoLayerRepo:
    def __init__(self):
        self.called = False
        self.args = None

    def create(self, path, description, source_url):
        self.called = True
        self.args = (path, description, source_url)


class DummyContext:
    def __init__(self):
        self.namespaces = DummyNamespaceRepo()
        self.geo_layers = DummyGeoLayerRepo()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Allow exceptions to propagate
        return False


class DummyDB:
    def __init__(self, ctx):
        self._ctx = ctx

    def context(self, notes):
        return self._ctx


@pytest.fixture
def dummy_ctx():
    return DummyContext()


@pytest.fixture(autouse=True)
def patch_gerrydb(monkeypatch, dummy_ctx):
    # Replace GerryDB so it always returns DummyDB(dummy_ctx)
    monkeypatch.setattr(
        "gerrydb.create.GerryDB", lambda *args, **kwargs: DummyDB(dummy_ctx)
    )
    return dummy_ctx


@pytest.fixture
def runner():
    return CliRunner()


def test_namespace_happy(runner, dummy_ctx):
    result = runner.invoke(
        cli, ["namespace", "foo", "--description", "mydesc", "--public"]
    )
    assert result.exit_code == 0
    assert "Failed to create" not in result.output
    assert dummy_ctx.namespaces.called
    assert dummy_ctx.namespaces.args == ("foo", "mydesc", True)


def test_namespace_already_exists(runner, dummy_ctx):
    # Simulate a duplicate-key error
    def fail_create(path, description, public):
        raise ResultError(f"Failed to create namespace {path}: duplicate")

    dummy_ctx.namespaces.create = fail_create

    result = runner.invoke(cli, ["namespace", "foo", "--description", "mydesc"])
    assert result.exit_code == 0
    assert "Failed to create foo namespace, already exists" in result.output


def test_namespace_other_error_bubbles(runner, dummy_ctx):
    # Simulate an unexpected error
    def fail_create(path, description, public):
        raise ResultError("unexpected failure")

    dummy_ctx.namespaces.create = fail_create

    result = runner.invoke(
        cli,
        ["namespace", "foo", "--description", "mydesc"],
        catch_exceptions=True,
    )
    assert result.exit_code != 0
    # error should be raised as exception
    assert isinstance(result.exception, ResultError)
    assert "unexpected failure" in str(result.exception)


def test_geo_layer_happy(runner, dummy_ctx):
    result = runner.invoke(
        cli,
        [
            "geo-layer",
            "roads",
            "--description",
            "desc",
            "--namespace",
            "plan",
            "--source-url",
            "http://example.com",
        ],
    )
    assert result.exit_code == 0
    assert "Failed to create" not in result.output
    assert dummy_ctx.geo_layers.called
    assert dummy_ctx.geo_layers.args == ("roads", "desc", "http://example.com")


def test_geo_layer_already_exists(runner, dummy_ctx):
    # Simulate duplicate-layer error
    def fail_create(path, description, source_url):
        raise ResultError(f"Failed to create geographic layer {path}: duplicate")

    dummy_ctx.geo_layers.create = fail_create

    result = runner.invoke(
        cli, ["geo-layer", "roads", "--description", "desc", "--namespace", "plan"]
    )
    assert result.exit_code == 0
    assert "Failed to create roads layer, already exists" in result.output


def test_geo_layer_other_error_bubbles(runner, dummy_ctx):
    # Simulate an unexpected error
    def fail_create(path, description, source_url):
        raise ResultError("other failure")

    dummy_ctx.geo_layers.create = fail_create

    result = runner.invoke(
        cli,
        ["geo-layer", "roads", "--description", "desc", "--namespace", "plan"],
        catch_exceptions=True,
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, ResultError)
    assert "other failure" in str(result.exception)
