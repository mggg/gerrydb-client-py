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
            canonical_path="load_iowa", name="State of Iowa"
        )
        ctx.load_dataframe(
            df=ia_dataframe,
            columns=columns,
            create_geos=True,
            namespace=client_ns.namespace,
            layer=layer,
            locality=locality,
        )
        # TODO: better testing here. Namely, did everything make it in?
