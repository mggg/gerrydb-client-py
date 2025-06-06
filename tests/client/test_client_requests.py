import pytest
import pandas as pd
import geopandas as gpd
from types import SimpleNamespace
from gerrydb.client import GerryDB, WriteContext
from shapely.geometry import Polygon

# disable both “unused mocks” and “unexpected requests” errors
pytestmark = pytest.mark.httpx_mock(
    assert_all_responses_were_requested=False,
    assert_all_requests_were_expected=False,
)


def test_create_write_context(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://example.com/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    db = GerryDB(key="key", host="example.com")

    with db.context(notes="Test notes") as ctx:
        assert ctx.notes == "Test notes"
        assert ctx.db == db


def test_create_geos_already_exists_httpx(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geo-imports/test_namespace",
        status_code=400,
        json={"detail": "Cannot create geographies that already exist."},
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = pd.DataFrame(index=["a", "b"])
    locality = SimpleNamespace(canonical_path="foo")
    layer = SimpleNamespace(path="foo", namespace="test_namespace")

    with pytest.raises(Exception) as exc:
        with ctx:
            ctx._WriteContext__create_geos(
                df=df,
                namespace="test_namespace",
                locality=locality,
                layer=layer,
                batch_size=1,
                max_conns=1,
                allow_empty_polys=False,
            )

    resp = exc.value.__cause__ or exc.value
    assert hasattr(resp, "response")
    assert (
        resp.response.json()["detail"]
        == "Cannot create geographies that already exist."
    )


def test_update_geos_already_exists_httpx(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geo-imports/test_namespace",
        status_code=400,
        json={"detail": "Cannot create geographies that already exist."},
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = pd.DataFrame(index=["a", "b"])
    locality = SimpleNamespace(canonical_path="foo")
    layer = SimpleNamespace(path="foo", namespace="test_namespace")

    with pytest.raises(Exception) as exc:
        with ctx:
            ctx._WriteContext__update_geos(
                df=df,
                namespace="test_namespace",
                locality=locality,
                layer=layer,
                batch_size=1,
                max_conns=1,
                allow_empty_polys=False,
            )

    resp = exc.value.__cause__ or exc.value
    assert hasattr(resp, "response")
    assert (
        resp.response.json()["detail"]
        == "Cannot create geographies that already exist."
    )


def test_validate_geo_compatabilty_empty_polys_but_not_explicitly_allowed(
    httpx_mock,
):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geo-imports/test_namespace",
        status_code=400,
        json={"detail": "Cannot create geographies that already exist."},
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo",
        json=["a", "b"],
    )
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo?mode=path_hash_pair",
        json=[
            ("a", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("b", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = pd.DataFrame(
        {"geometry": [Polygon(), Polygon()]},
        index=["a", "b"],
    )
    locality = SimpleNamespace(canonical_path="foo")
    layer = SimpleNamespace(path="foo", namespace="test_namespace")

    with pytest.raises(
        ValueError,
        match="The 'geometry' column in the dataframe contains empty polygons",
    ) as exc:
        with ctx:
            ctx._WriteContext__validate_geos_compatabilty(
                df=df,
                locality=locality,
                layer=layer,
                namespace="test_namespace",
            )


def test_validate_geo_compatabilty_forking_different_namespaces_errors_emtpy_polys(
    httpx_mock,
):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geo-imports/test_namespace",
        status_code=400,
        json={"detail": "Cannot create geographies that already exist."},
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo",
        json={"detail": '["a", "b"]'},
    )
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace2/foo/foo",
        json=["a", "b"],
    )
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo?mode=path_hash_pair",
        json=[
            ("a", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("b", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_fork/test_namespace/foo/foo?mode=compare&source_namespace=test_namespace2&source_layer=foo&allow_extra_source_geos=False&allow_empty_polys=False",
        json=[
            ("a", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("b", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )
    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = pd.DataFrame(
        {"geometry": [Polygon(), Polygon()]},
        index=["a", "b"],
    )
    locality = SimpleNamespace(canonical_path="foo")
    layer = SimpleNamespace(path="foo", namespace="test_namespace2")

    with pytest.raises(
        ValueError,
        match=(
            "Attempted to fork geometries from layer 'foo' in namespace 'test_namespace2' to "
            "layer 'foo' in namespace test_namespace. However, some of the source geometries "
            "in 'test_namespace2/foo' are empty polygons and empty polygons have not been "
            "allowed explicitly."
        ),
    ) as exc:
        with ctx:
            ctx._WriteContext__validate_geos_compatabilty(
                df=df,
                locality=locality,
                layer=layer,
                namespace="test_namespace",
            )


def test_validate_geo_compatabilty_no_known_paths(
    httpx_mock,
):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geo-imports/test_namespace",
        status_code=400,
        json={"detail": "Cannot create geographies that already exist."},
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo",
        json=[],
    )
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo?mode=path_hash_pair",
        json=[
            ("a", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("b", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = pd.DataFrame(
        {"geometry": [Polygon(), Polygon()]},
        index=["a", "b"],
    )
    locality = SimpleNamespace(canonical_path="foo")
    layer = SimpleNamespace(path="foo", namespace="test_namespace")

    with pytest.raises(
        IndexError,
        match=(
            "The index of the dataframe does not appear to match any geographies in the "
            "namespace which have the following geoid format: 'NO GEOGRAPHIES FOUND IN "
            "NAMESPACE MATCHING GIVEN LOCALITY AND LAYER'. Please ensure that "
            "the index of the dataframe matches the format of the geoid."
        ),
    ) as exc:
        with ctx:
            ctx._WriteContext__validate_geos_compatabilty(
                df=df,
                locality=locality,
                layer=layer,
                namespace="test_namespace",
                allow_empty_polys=True,
            )


def test_validate_geo_compatabilty_extra_known_paths(
    httpx_mock,
):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geo-imports/test_namespace",
        status_code=400,
        json={"detail": "Cannot create geographies that already exist."},
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo",
        json=["a", "b", "c"],
    )
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo?mode=path_hash_pair",
        json=[
            ("a", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("b", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = pd.DataFrame(
        {"geometry": [Polygon(), Polygon()]},
        index=["a", "b"],
    )
    locality = SimpleNamespace(canonical_path="foo")
    layer = SimpleNamespace(path="foo", namespace="test_namespace")

    with pytest.raises(
        ValueError,
        match=(
            "Failure in load_dataframe. Tried to import geographies for layer 'foo' and "
            "locality 'foo', but the passed dataframe does not contain the following "
            "geographies: {'c'}. Please provide values for these geographies in the dataframe "
            "or create a new locality with only the relevant geographies."
        ),
    ) as exc:
        with ctx:
            ctx._WriteContext__validate_geos_compatabilty(
                df=df,
                locality=locality,
                layer=layer,
                namespace="test_namespace",
                allow_empty_polys=True,
            )


def test_validate_geo_compatabilty_extra_df_paths(
    httpx_mock,
):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/geo-imports/test_namespace",
        status_code=400,
        json={"detail": "Cannot create geographies that already exist."},
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo",
        json=["a"],
    )
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/__geography_list/test_namespace/foo/foo?mode=path_hash_pair",
        json=[
            ("a", "75b6f320f5eb33d79cbcd9cf62be5a83"),
            ("b", "75b6f320f5eb33d79cbcd9cf62be5a83"),
        ],
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = pd.DataFrame(
        {"geometry": [Polygon(), Polygon()]},
        index=["a", "b"],
    )
    locality = SimpleNamespace(canonical_path="foo")
    layer = SimpleNamespace(path="foo", namespace="test_namespace")

    with pytest.raises(
        ValueError,
        match=(
            "Failure in load_dataframe. Tried to import geographies for layer 'foo' and "
            "locality 'foo', but the following geographies do not exist in the namespace "
            "'test_namespace': {'b'}"
        ),
    ) as exc:
        with ctx:
            ctx._WriteContext__validate_geos_compatabilty(
                df=df,
                locality=locality,
                layer=layer,
                namespace="test_namespace",
                allow_empty_polys=True,
            )


def test_validate_columsn_bad_column_type(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")
    with pytest.raises(
        TypeError, match="The 'columns' parameter must be a list of paths,"
    ):
        ctx._WriteContext__validate_columns(columns=1)


def test_validate_columns_bad_column_formats(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/columns/test_namespace",
        json=[
            {
                "canonical_path": "name",
                "description": "2010 Census area name",
                "source_url": "https://imadeitup.com",
                "kind": "identifier",
                "type": "str",
                "namespace": "census.2010_test1",
                "aliases": ["name10"],
                "meta": {
                    "notes": "Creating a view template and view for Maine counties",
                    "uuid": "ee79533f-b8c2-41e4-aac9-a2719614f2be",
                    "created_at": "2025-04-26T20:07:43.656305+00:00",
                    "created_by": "peter.r.rock2@gmail.com",
                },
            }
        ],
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")
    with pytest.raises(
        ValueError,
        match="Column paths passed to the `load_dataframe` function cannot contain '/'",
    ):
        ctx._WriteContext__validate_columns(columns=["/bad/columns"])


def test_validate_columns_bad_column_type(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/columns/test_namespace",
        json=[
            {
                "canonical_path": "name",
                "description": "2010 Census area name",
                "source_url": "https://imadeitup.com",
                "kind": "identifier",
                "type": "str",
                "namespace": "census.2010_test1",
                "aliases": ["name10"],
                "meta": {
                    "notes": "Creating a view template and view for Maine counties",
                    "uuid": "ee79533f-b8c2-41e4-aac9-a2719614f2be",
                    "created_at": "2025-04-26T20:07:43.656305+00:00",
                    "created_by": "peter.r.rock2@gmail.com",
                },
            }
        ],
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")
    with pytest.raises(
        ValueError,
        match="The columns parameter must be a list of paths, a pandas.core.indexes.base.Index, ",
    ):
        ctx._WriteContext__validate_columns(columns={"bad": "columns"})


def test_validate_columns_missing_columns(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8000/api/v1/columns/test_namespace",
        json=[
            {
                "canonical_path": "name",
                "description": "2010 Census area name",
                "source_url": "https://imadeitup.com",
                "kind": "identifier",
                "type": "str",
                "namespace": "census.2010_test1",
                "aliases": ["name10"],
                "meta": {
                    "notes": "Creating a view template and view for Maine counties",
                    "uuid": "ee79533f-b8c2-41e4-aac9-a2719614f2be",
                    "created_at": "2025-04-26T20:07:43.656305+00:00",
                    "created_by": "peter.r.rock2@gmail.com",
                },
            }
        ],
    )

    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")
    with pytest.raises(
        ValueError,
        match=(
            "Some of the columns in the dataframe do not exist in the database. "
            "Please create the missing columns first using the `db.columns.create` method."
        ),
    ):
        ctx._WriteContext__validate_columns(columns=["NAME_10"])


def test_validate_load_types_bad_types(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:8000/api/v1/meta/",
        json={
            "uuid": "00000000-0000-0000-0000-000000000000",
            "notes": "trigger",
            "created_at": "2025-04-26T00:00:00Z",
            "created_by": "test-user",
        },
    )
    db = GerryDB(
        host="localhost:8000",
        key="dummy-key",
        namespace="test_namespace",
        cache_max_size_gb=0.001,
    )
    ctx = WriteContext(db=db, notes="trigger")

    df = gpd.GeoDataFrame(
        {"geometry": [Polygon(), Polygon()]},
        index=["a", "b"],
    )

    with pytest.raises(
        TypeError, match="Cannot create or update geographies from a non-geodataframe. "
    ):
        ctx._WriteContext__validate_load_types(
            df=1,
            namespace="test_namespace",
            locality="test_locality",
            layer="test_layer",
            create_geos=True,
            patch_geos=False,
            upsert_geos=False,
            allow_empty_polys=False,
        )

    with pytest.raises(ValueError, match="No Namespace provided."):
        ctx._WriteContext__validate_load_types(
            df=df,
            namespace=None,
            locality="test_locality",
            layer="test_layer",
            create_geos=True,
            patch_geos=False,
            upsert_geos=False,
            allow_empty_polys=False,
        )

    with pytest.raises(
        ValueError, match="Locality must be provided when creating or upserting Geos"
    ):
        ctx._WriteContext__validate_load_types(
            df=df,
            namespace="test_namespace",
            locality=None,
            layer="test_layer",
            create_geos=True,
            patch_geos=False,
            upsert_geos=False,
            allow_empty_polys=False,
        )

    with pytest.raises(
        ValueError, match="GeoLayer must be provided when creating or upserting Geos"
    ):
        ctx._WriteContext__validate_load_types(
            df=df,
            namespace="test_namespace",
            locality="test_locality",
            layer=None,
            create_geos=True,
            patch_geos=False,
            upsert_geos=False,
            allow_empty_polys=False,
        )

    with pytest.raises(
        ValueError,
        match="Exactly one of `create_geo`, `patch_geos`, or `upsert_geos` must be True",
    ):
        ctx._WriteContext__validate_load_types(
            df=df,
            namespace="test_namespace",
            locality="test_locality",
            layer="test_layer",
            create_geos=True,
            patch_geos=True,
            upsert_geos=False,
            allow_empty_polys=False,
        )
