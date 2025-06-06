"""Integration/VCR tests for geographic import metadata."""

import pytest
from gerrydb.client import gather_batch

# The `GeoImport` object is used for internal tracking, so we don't
# expose it directly via a repository.


@pytest.mark.asyncio
@pytest.mark.slow
async def test_geo_import__client_creates_on_geo_access(client_ns, ia_dataframe):
    with client_ns.context(notes="fishing for a GeoImport") as ctx:
        layer = ctx.geo_layers.create(
            path="counties",
            description="2020 U.S. Census counties.",
            source_url="https://www.census.gov/",
        )
        locality = ctx.localities.create(
            canonical_path="iowa2",
            name="State of Iowa version 2",
            aliases=["ia2", "19p2"],
            default_proj="epsg:26915",  # UTM zone 15N
        )

        geo_ctx = ctx.geo
        df = ia_dataframe.to_crs("epsg:4269")  # import as lat/long
        geos = dict(df.geometry)

        geo_pairs = list(geos.items())
        batch_size = 5000
        max_conns = 1
        tasks = []

        async with geo_ctx.async_bulk(ctx.db.namespace, 1) as async_ctx:
            for idx in range(0, len(geo_pairs), batch_size):
                chunk = dict(geo_pairs[idx : idx + batch_size])
                tasks.append(async_ctx.create(chunk))
                _ = await gather_batch(tasks, max_conns)

        assert async_ctx.client.headers.get("X-GerryDB-Geo-Import-ID", None) is not None
