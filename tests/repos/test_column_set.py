"""Integration/VCR tests for columns."""

import pytest
from gerrydb.exceptions import RequestError


@pytest.mark.vcr
def test_column_set_repo_create_get(client_ns, pop_column_meta, vap_column_meta):
    with client_ns.context(notes="adding three columns in a column set") as ctx:
        pop_col = ctx.columns.create(**pop_column_meta)
        ctx.columns.create(**vap_column_meta)
        ctx.columns.create(
            path="total_cvap",
            description="ACS total citizen voting-age population (CVAP)",
            source_url="https://www.census.gov/",
            column_kind="count",
            column_type="int",
        )
        col_set = ctx.column_sets.create(
            path="totals",
            description="Total population columns",
            columns=[pop_col, "total_vap", f"{client_ns.namespace}/total_cvap"],
        )

    col_paths = set(col.canonical_path for col in col_set.columns)
    assert col_paths == {"total_pop", "total_vap", "total_cvap"}
    assert client_ns.column_sets["totals"] == col_set
    assert client_ns.column_sets[(f"{client_ns.namespace}", "totals")] == col_set


@pytest.mark.vcr
def test_column_set_repo_create_all(client_ns, pop_column_meta):
    with client_ns.context(notes="adding one column in a column set") as ctx:
        pop_col = ctx.columns.create(**pop_column_meta)
        ctx.column_sets.create(
            path="totals", description="Total population columns", columns=[pop_col]
        )

    assert "totals" in [col.path for col in client_ns.column_sets.all()]


@pytest.mark.vcr
def test_column_set_repo_create_bad_col_list(client_ns):
    with pytest.raises(
        RequestError, match="Column paths must be in the form of either"
    ):
        with client_ns.context(notes="adding one column in a column set") as ctx:
            ctx.column_sets.create(
                path="totals",
                description="Total population columns",
                columns=["bad/path/for/things"],
            )


@pytest.mark.vcr
def test_column_set_repo_create_bad_col_ns(client_ns):
    with pytest.raises(
        RequestError, match="All columns in a column set must come the same namespace"
    ):
        with client_ns.context(notes="adding one column in a column set") as ctx:
            ctx.column_sets.create(
                path="totals",
                description="Total population columns",
                columns=["bad_ns/bad_path"],
            )
