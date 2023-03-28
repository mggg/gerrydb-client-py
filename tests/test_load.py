"""Tests for high-level load/import operations."""


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
