"""Integration/VCR tests for districting plans."""
import os

import pytest

from cherrydb import CherryDB
from cherrydb.exceptions import ResultError


@pytest.fixture(scope="module")
def client_with_ia_layer_loc(ia_dataframe, ia_column_meta):
    """A namespaced client with a `GeoLayer` and `Locality` for Iowa counties."""
    client = CherryDB(
        host=os.environ.get("CHERRY_TEST_SERVER", "localhost:8000"),
        key=os.environ.get("CHERRY_TEST_API_KEY"),
    )

    with client.context(
        notes=f"Test setup for cherrydb-client-py plan repository tests",
    ) as ctx:
        ctx.namespaces.create(
            path="plan",
            description="cherrydb-client-py plan repository tests",
            public=True,
        )

    client.namespace = "plan"

    with client.context(notes="Importing Iowa counties shapefile") as ctx:
        columns = {
            name: ctx.columns.create(**meta) for name, meta in ia_column_meta.items()
        }
        layer = ctx.geo_layers.create(
            path="counties",
            description="2020 U.S. Census counties.",
            source_url="https://www.census.gov/",
        )
        locality = ctx.localities.create(
            canonical_path="iowa", name="State of Iowa", aliases=["ia", "19"]
        )
        ctx.load_dataframe(
            df=ia_dataframe,
            columns=columns,
            create_geo=True,
            namespace=client.namespace,
            layer=layer,
            locality=locality,
        )

    return client, layer, locality


def test_plan_repo_create_get__one_district_complete(
    client_with_ia_layer_loc, ia_dataframe
):
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


def test_plan_repo_create__one_district_incomplete(
    client_with_ia_layer_loc, ia_dataframe
):
    client_ns, layer, locality = client_with_ia_layer_loc
    geos = list(ia_dataframe.index)
    assigned_geos = geos[:50]
    unassigned_geos = geos[50:]

    with client_ns.context(notes="Uploading a plan for Iowa counties") as ctx:
        plan = ctx.plans.create(
            path="ia_one_district_incomplete",
            locality=locality,
            layer=layer,
            description="Test plan for Iowa (one district with missing assignments).",
            assignments={idx: "1" for idx in assigned_geos},
            source_url="https://example.com/",
        )
        assert plan.assignments == {
            **{f"/{client_ns.namespace}/{geo}": "1" for geo in assigned_geos},
            **{f"/{client_ns.namespace}/{geo}": None for geo in unassigned_geos},
        }
        assert not plan.complete
        assert plan.num_districts == 1


def test_plan_repo_create_all__two_districts(client_with_ia_layer_loc, ia_dataframe):
    client_ns, layer, locality = client_with_ia_layer_loc
    geos = list(ia_dataframe.index)
    dist1_geos = geos[:50]
    dist2_geos = geos[50:]

    with client_ns.context(notes="Uploading a plan for Iowa counties") as ctx:
        plan = ctx.plans.create(
            path="ia_two_districts",
            locality=locality,
            layer=layer,
            description="Test plan for Iowa (two districts)",
            assignments={
                **{geo: "1" for geo in dist1_geos},
                **{geo: "2" for geo in dist2_geos},
            },
            source_url="https://example.com/",
            daves_id="123",
            districtr_id="123",
        )
        assert plan.assignments == {
            **{f"/{client_ns.namespace}/{geo}": "1" for geo in dist1_geos},
            **{f"/{client_ns.namespace}/{geo}": "2" for geo in dist2_geos},
        }
        assert plan.complete
        assert plan.num_districts == 2
        assert "ia_two_districts" in [plan.path for plan in ctx.plans.all()]


def test_plan_repo_create__unknown_geos(client_with_ia_layer_loc):
    client_ns, layer, locality = client_with_ia_layer_loc

    with client_ns.context(notes="creating a county-level locality in Iowa") as ctx:
        county_loc = ctx.localities.create(
            canonical_path="iowa/dubuque", name="Dubuque County, Iowa"
        )
        ctx.geo_layers.map_locality(
            layer=layer, locality=county_loc, geographies=["19061"]
        )

    with client_ns.context(notes="Uploading a plan for Iowa counties") as ctx:
        with pytest.raises(ResultError, match="Geographies not in set"):
            ctx.plans.create(
                path="ia_two_county_plan",
                locality=county_loc,
                layer=layer,
                description="Test plan for Iowa (two districts, two counties only)",
                assignments={
                    "19059": "1",  # not mapped!
                    "19061": "2",
                },
                source_url="https://example.com/",
            )
