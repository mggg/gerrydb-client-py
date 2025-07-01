"""Integration/VCR tests for view templates."""

import pytest
from gerrydb.repos.view_template import _normalize_columns, _normalize_column_sets
from gerrydb.schemas import Column, ColumnKind, ColumnType, ObjectMeta, ColumnSet


def test_normalize_column_column_object():
    col = Column(
        canonical_path="total_pop",
        namespace="census.2010_test1",
        description="2010 Census total population",
        kind=ColumnKind.COUNT,
        type=ColumnType.INT,
        aliases=["totpop", "p001001", "p0001001"],
        meta=ObjectMeta(
            uuid="ee79533f-b8c2-41e4-aac9-a2719614f2be",
            created_at="2025-04-26T20:07:43.656305+00:00",
            created_by="test-user@example.com",
            notes="This is a test column.",
        ),
    )
    assert _normalize_columns("census.2010_test1", [col]) == [
        "/columns/census.2010_test1/total_pop"
    ]


def test_normlize_columns_splits_strings_correctly():
    assert _normalize_columns("census.2010_test1", ["total_pop"]) == [
        "/columns/census.2010_test1/total_pop"
    ]
    assert _normalize_columns(
        "census.2010_test_AAAAA", ["census.2010_test1/total_pop"]
    ) == ["/columns/census.2010_test1/total_pop"]
    assert _normalize_columns(
        "census.2010_test_AAAAA", ["columns/census.2010_test1/total_pop"]
    ) == ["/columns/census.2010_test1/total_pop"]


def test_normalize_columns_raises_on_bad_string():
    with pytest.raises(ValueError, match="Invalid column path: /bad/path/column"):
        _normalize_columns("census.2010_test1", ["/bad/path/column"])
    with pytest.raises(ValueError, match="Column path must be in the form of either"):
        _normalize_columns("census.2010_test1", ["/bad/path/to/column"])


def test_normalize_columns_tuples():
    assert _normalize_columns(
        "census.2010_test_AAAAA", [("census.2010_test1", "total_pop")]
    ) == ["/columns/census.2010_test1/total_pop"]

    with pytest.raises(ValueError, match="When passing a tuple"):
        _normalize_columns(
            "census.2010_test_AAAAA", [("census.2010_test1", "total_pop", "extra")]
        )


def test_normalize_columns_bad_column_type():
    with pytest.raises(ValueError, match="Invalid column type: <class 'int'>"):
        _normalize_columns("census.2010_test1", [1])


def test_normalize_column_sets_column_set_object():
    col_set = ColumnSet(
        path="me_set",
        namespace="census.2010_test1",
        columns=[
            Column(
                canonical_path="total_pop",
                namespace="census.2010_test1",
                description="2010 Census total population",
                kind=ColumnKind.COUNT,
                type=ColumnType.INT,
                aliases=["totpop", "p001001", "p0001001"],
                meta=ObjectMeta(
                    uuid="ee79533f-b8c2-41e4-aac9-a2719614f2be",
                    created_at="2025-04-26T20:07:43.656305+00:00",
                    created_by="test-user@example.com",
                    notes="This is a test column.",
                ),
            )
        ],
        refs=["total_pop"],
        meta=ObjectMeta(
            uuid="ee79533f-b8c2-41e4-aac9-a2719614f2be",
            created_at="2025-04-26T20:07:43.656305+00:00",
            created_by="test-user@example.com",
            notes="This is a test column set.",
        ),
        description="2010 Census total population",
    )
    assert _normalize_column_sets("census.2010_test1", [col_set]) == [
        "/column-sets/census.2010_test1/me_set"
    ]


def test_normalize_column_sets_splits_strings_correctly():
    assert _normalize_column_sets("census.2010_test1", ["me_set"]) == [
        "/column-sets/census.2010_test1/me_set"
    ]
    assert _normalize_column_sets(
        "census.2010_test_AAAAA", ["census.2010_test1/me_set"]
    ) == ["/column-sets/census.2010_test1/me_set"]
    assert _normalize_column_sets(
        "census.2010_test_AAAAA", ["column-sets/census.2010_test1/me_set"]
    ) == ["/column-sets/census.2010_test1/me_set"]


def test_normalize_column_sets_raises_on_bad_string():
    with pytest.raises(ValueError, match="Invalid column set path: /bad/path/column"):
        _normalize_column_sets("census.2010_test1", ["/bad/path/column"])
    with pytest.raises(
        ValueError, match="Column_set path must be in the form of either"
    ):
        _normalize_column_sets("census.2010_test1", ["/bad/path/to/column"])


def test_normalize_column_sets_tuples():
    assert _normalize_column_sets(
        "census.2010_test_AAAAA", [("census.2010_test1", "me_set")]
    ) == ["/column-sets/census.2010_test1/me_set"]
    with pytest.raises(ValueError, match="When passing a tuple"):
        _normalize_column_sets(
            "census.2010_test_AAAAA", [("census.2010_test1", "me_set", "extra")]
        )


def test_normalize_column_sets_bad_column_type():
    with pytest.raises(ValueError, match="Invalid column set type: <class 'int'>"):
        _normalize_column_sets("census.2010_test1", [1])


@pytest.mark.vcr
def test_view_template_repo_create_get_missing_columns(client_ns):
    with pytest.raises(
        ValueError, match="Must provide at least one of columns or column_sets."
    ):
        with client_ns.context(notes="adding a view template with two columns") as ctx:
            ctx.view_templates.create(
                path="pops_empty", columns=[], description="Population view."
            )


@pytest.mark.vcr
def test_view_template_repo_create_get__online_columns_only(
    client_ns, pop_column_meta, vap_column_meta
):
    with client_ns.context(notes="adding a view template with two columns") as ctx:
        pop_col = ctx.columns.create(**pop_column_meta)
        vap_col = ctx.columns.create(**vap_column_meta)
        view_template = ctx.view_templates.create(
            path="pops", columns=[pop_col, vap_col], description="Population view."
        )

    assert view_template == client_ns.view_templates["pops"]
