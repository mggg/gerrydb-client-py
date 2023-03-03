"""Integration/VCR tests for view templates."""
import pytest


@pytest.mark.vcr
def test_view_template_repo_create_get__online_columns_only(
    client_ns, pop_column_meta, vap_column_meta
):
    with client_ns.context(notes="adding a view template with two columns") as ctx:
        pop_col = ctx.columns.create(**pop_column_meta)
        vap_col = ctx.columns.create(**vap_column_meta)
        view_template = ctx.view_templates.create(
            path="pops", members=[pop_col, vap_col], description="Population view."
        )
        print(view_template)
        # TODO: more evaluation here.
