"""Integration/VCR tests for columns."""
import pytest
from shapely import box

from cherrydb.schemas import ColumnKind, ColumnType


@pytest.fixture
def column(pop_column_meta):
    """Column metadata."""
    return pop_column_meta


@pytest.mark.vcr
def test_column_repo_create_get(client_ns, column):
    with client_ns.context(notes="adding a column") as ctx:
        col = ctx.columns.create(**column)

    assert col.kind == ColumnKind.COUNT
    assert col.type == ColumnType.INT
    assert client_ns.columns["total_pop"] == col
    assert client_ns.columns["totpop"] == col
    assert client_ns.columns[f"/{client_ns.namespace}/total_pop"] == col


@pytest.mark.vcr
def test_column_repo_create_all(client_ns, column):
    with client_ns.context(notes="adding a column") as ctx:
        ctx.columns.create(**column)

    assert "total_pop" in [col.path for col in client_ns.columns.all()]


@pytest.mark.vcr
def test_column_repo_create_update_get(client_ns, column):
    with client_ns.context(notes="adding and then updating a column") as ctx:
        ctx.columns.create(**column)
        updated_col = ctx.columns.update("total_pop", aliases=["population"])

    assert client_ns.columns["population"] == updated_col


def test_column_repo_set_values(client_ns, column):
    n = 10000
    with client_ns.context(notes="adding a column, geographies, and values") as ctx:
        col = ctx.columns.create(**column)
        with ctx.geo.bulk() as geo_ctx:
            geo_ctx.create({str(idx): box(0, 0, 1, 1) for idx in range(n)})
        ctx.columns.set_values(col, values={str(idx): idx for idx in range(n)})
