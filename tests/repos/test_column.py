"""Integration/VCR tests for columns."""

import pytest
from shapely import box

from gerrydb.schemas import ColumnKind, ColumnType
from gerrydb.client import GerryDB
import asyncio
import httpx
from httpx import HTTPError


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
    assert client_ns.columns[(f"{client_ns.namespace}", "total_pop")] == col


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
            geo_ctx.create({f"{idx:010d}": box(0, 0, 1, 1) for idx in range(n)})
        ctx.columns.set_values(
            path=col.path, values={f"{idx:010d}": idx for idx in range(n)}
        )


def test_column_repo_set_values_invalid_path_or_col(client_ns):
    with pytest.raises(ValueError, match="Either `path` or `col` must be provided."):
        client_ns.context().columns.set_values(
            path=None,
            namespace="test",
            col=None,
            values={},
        )


def test_column_repo_async_set_values_invalid_values(client_ns):
    with pytest.raises(
        ValueError,
        match="Either `path` or `col` must be provided.",
    ):
        asyncio.run(
            client_ns.context().columns.async_set_values(
                path=None,
                namespace="test",
                col=None,
                values={},
            )
        )


# turn off both “unused mock” and “unexpected request” errors
pytestmark = pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False,
    assert_all_requests_were_expected=False,
)


def test_async_set_values_ephemeral_client(httpx_mock):
    # 1) stub the POST /meta/ for WriteContext.__enter__
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "irrelevant",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user@example.com",
        },
    )

    # 2) stub the PUT that async_set_values will fire
    #    URL is: {base_url}/{namespace}/{path}
    httpx_mock.add_response(
        method="PUT",
        url="http://localhost:8000/api/v1/columns/test_ns/foo",
        status_code=204,
    )

    # 3) spin up a real client + context
    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_ns",
        cache_max_size_gb=0.001,
    )

    with db.context(notes="whatever") as ctx:
        asyncio.run(
            ctx.columns.async_set_values(
                path="foo",
                namespace="test_ns",
                values={"aa": 1, "bb": 2},
                client=None,  # explicit, but same as omitting
            )
        )


def test_async_set_values_ephemeral_client_is_closed(httpx_mock, monkeypatch):
    # 1) stub WriteContext metadata call
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "irrelevant",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user@example.com",
        },
    )

    # 2) stub the PUT
    httpx_mock.add_response(
        method="PUT",
        url="http://localhost:8000/api/v1/columns/test_ns/foo",
        status_code=204,
    )

    # 3) intercept AsyncClient.aclose
    closed = False

    async def fake_aclose(self):
        nonlocal closed
        closed = True

    monkeypatch.setattr(httpx.AsyncClient, "aclose", fake_aclose)

    # 4) run the code
    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_ns",
        cache_max_size_gb=0.001,
    )

    with db.context(notes="whatever") as ctx:
        asyncio.run(
            ctx.columns.async_set_values(
                path="foo",
                namespace="test_ns",
                values={"aa": 1, "bb": 2},
            )
        )

    # 5) confirm that our fake aclose ran
    assert closed, "Expected the ephemeral AsyncClient to be closed"


def test_async_set_values_bad_response(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "irrelevant",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user@example.com",
        },
    )

    httpx_mock.add_response(
        method="PUT",
        url="http://localhost:8000/api/v1/columns/test_ns/foo",
        status_code=404,
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_ns",
        cache_max_size_gb=0.001,
    )

    with pytest.raises(HTTPError, match="Client error '404 Not Found'"):
        with db.context(notes="whatever") as ctx:
            asyncio.run(
                ctx.columns.async_set_values(
                    path="foo",
                    namespace="test_ns",
                    values={"aa": 1, "bb": 2},
                )
            )
