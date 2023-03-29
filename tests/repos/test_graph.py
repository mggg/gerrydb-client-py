"""Integration/VCR tests for districting plans."""
import pytest


def test_plan_repo_create_get__rook(client_with_ia_layer_loc, ia_dataframe):
    client_ns, layer, locality = client_with_ia_layer_loc
    with client_ns.context(notes="Uploading a plan for Iowa counties") as ctx:
        plan = ctx.plans.create(
            path="ia_one_district_complete",
            locality=locality,
            layer=layer,
            description="Test plan for Iowa (one district).",
            assignments={idx: "1" for idx in ia_dataframe.index},
            source_url="https://example.com/",
        )
        assert plan.assignments == {
            f"/{client_ns.namespace}/{idx}": "1" for idx in ia_dataframe.index
        }
        assert plan.complete
        assert plan.num_districts == 1
        assert ctx.plans["ia_one_district_complete"] == plan
