"""Integration/VCR tests for geographic import metadata."""
import pytest

# The `GeoImport` object is used for internal tracking, so we don't
# expose it directly via a repository.


@pytest.mark.skip
@pytest.mark.vcr
def test_geo_import__client_creates_on_geo_access(client_ns):
    with client_ns.context(notes="fishing for a GeoImport") as ctx:
        assert ctx.geo_import is None
        ctx.geo.all()
