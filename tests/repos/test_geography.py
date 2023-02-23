"""Integration/VCR tests for columns."""
import pytest
from shapely import box


def test_geography_repo_create(client_ns):
    with client_ns.context(notes="adding a geography") as ctx:
        with ctx.geo.bulk_create() as create_ctx:
            create_ctx.create(
                {
                    "1": box(0, 0, 1, 1),
                }
            )
