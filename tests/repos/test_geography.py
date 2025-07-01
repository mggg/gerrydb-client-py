"""Integration/VCR tests for columns."""

import json
import pytest
from shapely import box
import httpx
from types import SimpleNamespace as _BaseNS
from http import HTTPStatus
from httpx import HTTPStatusError

from gerrydb.repos.geography import AsyncGeoImporter, GeographyRepo
from gerrydb.exceptions import RequestError, ForkingError
from gerrydb import GerryDB


class SimpleNamespace(_BaseNS):
    """
    A drop-in replacement for types.SimpleNamespace that also
    implements Pydantic-style .model_dump(mode="json") by returning its own dict.
    """

    def model_dump(self) -> dict:
        return self.dict()


def test_geography_repo_create(client_ns):
    with client_ns.context(notes="adding a geography") as ctx:
        with ctx.geo.bulk() as bulk_ctx:
            geos = bulk_ctx.create(
                {f"{idx:010d}": box(0, 0, 1, 1) for idx in range(10000)}
            )

    assert all([geo.geography == box(0, 0, 1, 1) for geo in geos])


pytestmark = [
    pytest.mark.httpx_mock(
        assert_all_responses_were_requested=False,
        assert_all_requests_were_expected=False,
    ),
]


class DummyRepo:
    """Just need a .base_url attribute."""

    base_url = "http://localhost:8000/api/v1/geographies"


@pytest.fixture
def importer():
    """
    Build an AsyncGeoImporter whose .client is a real AsyncClient
    (pytest-httpx will intercept its requests).
    """
    client = httpx.AsyncClient()
    return AsyncGeoImporter(repo=DummyRepo(), namespace="test_ns", client=client)


@pytest.mark.asyncio
async def test_send_422_already_exists(httpx_mock, importer):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geographies/test_ns",
        status_code=422,
        content=json.dumps(
            {
                "detail": "Object creation failed. "
                "Reason: Cannot create geographies that already exist."
            }
        ).encode(),
    )

    with pytest.raises(RequestError) as exc:
        await importer._send(
            {"foo": box(0, 0, 1, 1)},
            method="POST",
        )

    assert "Cannot create geographies that already exist." in str(exc.value)


@pytest.mark.asyncio
async def test_send_422_duplicate_paths(httpx_mock, importer):
    paths = ["/test_ns/a", "/test_ns/a"]
    httpx_mock.add_response(
        method="PATCH",
        url="http://localhost:8000/api/v1/geographies/test_ns",
        status_code=422,
        content=json.dumps(
            {
                "detail": "Object creation failed. "
                "Reason: Cannot create geographies with duplicate paths.",
                "paths": paths,
            }
        ).encode(),
    )

    with pytest.raises(RequestError) as exc:
        await importer._send(
            {
                "aa": box(0, 0, 1, 1),
                "aa": box(0, 0, 1, 1),
            },
            method="PATCH",
        )

    msg = str(exc.value)
    assert "Cannot create geographies with duplicate paths." in msg
    assert str(paths) in msg


def _unwrap(fn):
    """Unwraps decorators from a function."""
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


def test_check_get_layer_hashes_raises_error_on_bad_path():
    func = _unwrap(GeographyRepo.get_layer_hashes)

    dummy_self = SimpleNamespace(
        base_url="/foo",
        request=lambda method, url, params=None: (_ for _ in ()).throw(
            RuntimeError("boom")
        ),
    )

    with pytest.raises(RuntimeError, match="Failed to get layer hashes."):
        func(dummy_self, path="bad_path", namespace="bad_ns", layer_name="bad_layer")


def test_fork_geos_errors_500(httpx_mock):
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
        method="GET",
        url="http://localhost:8000/api/v1/__geography_fork/test_namespace/bar/foo?mode=compare&source_namespace=test_namespace2&source_layer=foo&allow_extra_source_geos=False&allow_empty_polys=False",
        json=[
            ("aa", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("bb", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/__geography_fork/test_namespace/bar/foo?mode=compare&source_namespace=test_namespace2&source_layer=foo&allow_extra_source_geos=False&allow_empty_polys=False",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )

    ctx = db.context(notes="whatever")

    with pytest.raises(HTTPStatusError, match="Internal Server Error"):
        with ctx:
            ctx.geo.fork_geos(
                path="bar",
                namespace="test_namespace",
                layer_name="foo",
                source_namespace="test_namespace2",
                source_layer_name="foo",
            )


def test_fork_geos_errors_409(httpx_mock):
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
        method="GET",
        url="http://localhost:8000/api/v1/__geography_fork/test_namespace/bar/foo?mode=compare&source_namespace=test_namespace2&source_layer=foo&allow_extra_source_geos=False&allow_empty_polys=False",
        json=[
            ("aa", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("bb", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/__geography_fork/test_namespace/bar/foo?mode=compare&source_namespace=test_namespace2&source_layer=foo&allow_extra_source_geos=False&allow_empty_polys=False",
        status_code=HTTPStatus.CONFLICT,
        json={"detail": "Bad things happened."},
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )

    ctx = db.context(notes="whatever")

    with pytest.raises(ForkingError, match="Forking failed for the following reason:"):
        with ctx:
            ctx.geo.fork_geos(
                path="bar",
                namespace="test_namespace",
                layer_name="foo",
                source_namespace="test_namespace2",
                source_layer_name="foo",
            )
