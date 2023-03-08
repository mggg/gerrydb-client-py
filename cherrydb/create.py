"""CLI for creating CherryDB resources."""
from typing import Optional

import click
from cherrydb import CherryDB


@click.group()
def cli():
    """Creates CherryDB resources."""


@cli.command()
@click.argument("path")
@click.option("--description", required=True)
@click.option("--public", is_flag=True)
def namespace(path: str, description: str, public: bool):
    """Creates a namespace."""
    db = CherryDB()
    with db.context(notes=f'Creating namespace "{path}" from CLI') as ctx:
        ctx.namespaces.create(path=path, description=description, public=public)


@cli.command()
@click.argument("path")
@click.option("--description", required=True)
@click.option("--namespace", required=True)
@click.option("--source-url")
def geo_layer(path: str, description: str, namespace: str, source_url: Optional[str]):
    """Creates a geographic layer."""
    db = CherryDB(namespace=namespace)
    with db.context(notes=f'Creating geographic layer "{path}" from CLI') as ctx:
        ctx.geo_layers.create(path=path, description=description, source_url=source_url)


if __name__ == "__main__":
    cli()
