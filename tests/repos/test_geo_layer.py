"""Integration/VCR tests for geographic layers."""
import pytest


@pytest.mark.vcr
def test_geo_layer_repo_create_get__online(client_ns):
    with client_ns.context(notes="adding a geographic layer") as ctx:
        layer = ctx.geo_layers.create(
            "blocks/2020",
            description="2020 Census blocks",
            source_url="https://www.census.gov/",
        )

    assert layer.description == "2020 Census blocks"
    assert client_ns.geo_layers["blocks/2020"] == layer
    assert client_ns.geo_layers[f"/{client_ns.namespace}/blocks/2020"] == layer


@pytest.mark.vcr
def test_geo_layer_repo_create_get__online_offline(client_ns):
    with client_ns.context(notes="adding a geographic layer") as ctx:
        layer = ctx.geo_layers.create("blocks/2020", description="2020 Census blocks")

    client_ns.offline = True
    assert client_ns.geo_layers["blocks/2020"] == layer


@pytest.mark.vcr
def test_geo_layer_repo_create_all__online(client_ns):
    with client_ns.context(notes="adding a geographic layer") as ctx:
        ctx.geo_layers.create("blocks/2020", description="2020 Census blocks")

    assert "blocks/2020" in [layer.path for layer in client_ns.geo_layers.all()]


@pytest.mark.vcr
def test_geo_layer_repo_create_get__online_offline(client_ns):
    with client_ns.context(notes="adding a geographic layer") as ctx:
        ctx.geo_layers.create("blocks/2020", description="2020 Census blocks")

    client_ns.offline = True
    assert "blocks/2020" in [layer.path for layer in client_ns.geo_layers.all()]
