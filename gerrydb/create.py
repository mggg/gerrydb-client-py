"""CLI for creating GerryDB resources."""
from typing import Optional

import click

from gerrydb import GerryDB


@click.group()
def cli():
    """Creates GerryDB resources."""


@cli.command()
@click.argument("path")
@click.option("--description", required=True)
@click.option("--public", is_flag=True)
def namespace(path: str, description: str, public: bool):
    """Creates a namespace."""
    db = GerryDB()
    with db.context(notes=f'Creating namespace "{path}" from CLI') as ctx:
        ctx.namespaces.create(path=path, description=description, public=public)


@cli.command()
@click.argument("path")
@click.option("--description", required=True)
@click.option("--namespace", required=True)
@click.option("--source-url")
def geo_layer(path: str, description: str, namespace: str, source_url: Optional[str]):
    """Creates a geographic layer."""
    db = GerryDB(namespace=namespace)
    with db.context(notes=f'Creating geographic layer "{path}" from CLI') as ctx:
        ctx.geo_layers.create(path=path, description=description, source_url=source_url)


if __name__ == "__main__":
    cli()
