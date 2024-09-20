"""Integration/VCR tests for columns."""

from shapely import box


def test_geography_repo_create(client_ns):
    with client_ns.context(notes="adding a geography") as ctx:
        with ctx.geo.bulk() as bulk_ctx:
            geos = bulk_ctx.create({str(idx): box(0, 0, 1, 1) for idx in range(10000)})
