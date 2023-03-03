"""Tests for high-level load/import operations."""
from pathlib import Path

import geopandas as gpd
import pytest


@pytest.fixture
def ia_dataframe():
    """`GeoDataFrame` of Iowa counties."""
    shp_path = Path(__file__).resolve().parent / "fixtures" / "tl_2020_19_county20.zip"
    return gpd.read_file(shp_path).set_index("GEOID20")


@pytest.fixture
def ia_column_meta():
    """Metadata for selected columns in the Iowa counties fixture."""
    return {
        "NAME20": {
            "path": "name",
            "description": "2020 Census name",
            "source_url": "https://www.census.gov/",
            "column_kind": "identifier",
            "column_type": "str",
        },
        "FUNCSTAT20": {
            "path": "funcstat",
            "description": "2020 Census 2020 Census functional status.",
            "source_url": "https://www.census.gov/",
            "column_kind": "categorical",
            "column_type": "str",
        },
    }


def test_load_dataframe__with_geo__ia_counties(client_ns, ia_dataframe, ia_column_meta):
    with client_ns.context(notes="Importing Iowa counties shapefile") as ctx:
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
            namespace=client_ns.namespace,
            layer=layer,
            locality=locality,
        )


def test_load_dataframe__with_geo_and_view__ia_counties(
    client_ns, ia_dataframe, ia_column_meta
):
    with client_ns.context(notes="Importing Iowa counties shapefile") as ctx:
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
            namespace=client_ns.namespace,
            layer=layer,
            locality=locality,
        )

        view_template = ctx.view_templates.create(
            path="base", members=list(columns.values()), description="Base view."
        )
        view = ctx.views.create(
            path="ia_base",
            template=view_template,
            locality=locality,
            layer=layer,
        )
