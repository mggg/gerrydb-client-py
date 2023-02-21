"""Integration/VCR tests for columns."""
import pytest

from cherrydb.schemas import ColumnKind, ColumnType


@pytest.fixture
def column():
    """Column metadata."""
    return {
        "path": "total_pop",
        "description": "2020 Census total population",
        "source_url": "https://www.census.gov/",
        "column_kind": "count",
        "column_type": "int",
        "aliases": ["totpop", "p001001", "p0001001"],
    }


@pytest.mark.vcr
def test_column_repo_create_get__online(client_ns, column):
    with client_ns.context(notes="adding a column") as ctx:
        col = ctx.columns.create(**column)

    assert col.kind == ColumnKind.COUNT
    assert col.type == ColumnType.INT
    assert client_ns.columns["total_pop"] == col
    assert client_ns.columns["totpop"] == col
    assert client_ns.columns[f"/{client_ns.namespace}/total_pop"] == col


@pytest.mark.vcr
def test_column_repo_create_get__online_offline(client_ns, column):
    with client_ns.context(notes="adding a column") as ctx:
        col = ctx.columns.create(**column)

    client_ns.offline = True
    assert client_ns.columns["total_pop"] == col
    assert client_ns.columns["totpop"] == col


@pytest.mark.vcr
def test_column_repo_create_all__online(client_ns, column):
    with client_ns.context(notes="adding a column") as ctx:
        ctx.columns.create(**column)

    assert "total_pop" in [col.path for col in client_ns.columns.all()]


@pytest.mark.vcr
def test_column_repo_create_all__online_offline(client_ns, column):
    with client_ns.context(notes="adding a column") as ctx:
        ctx.columns.create(**column)
    client_ns.columns.all()  # Populate cache.

    client_ns.offline = True
    assert "total_pop" in [col.path for col in client_ns.columns.all()]


@pytest.mark.vcr
def test_column_repo_create_update_get__online(client_ns, column):
    with client_ns.context(notes="adding and then updating a column") as ctx:
        ctx.columns.create(**column)
        updated_col = ctx.columns.update("total_pop", aliases=["population"])

    assert client_ns.columns["population"] == updated_col


@pytest.mark.vcr
def test_column_repo_create_update_get__online_offline(client_ns, column):
    with client_ns.context(notes="adding and then updating a column") as ctx:
        ctx.columns.create(**column)
        updated_col = ctx.columns.update("total_pop", aliases=["population"])

    client_ns.offline = True
    assert client_ns.columns["population"] == updated_col