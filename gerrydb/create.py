"""CLI for creating GerryDB resources."""

from typing import Optional

import click

from gerrydb import GerryDB
from gerrydb.exceptions import ResultError


@click.group()
def cli():
    """Creates GerryDB resources."""
    pass


@cli.command()
@click.argument("path")
@click.option("--description", required=True)
@click.option("--public", is_flag=True)
def namespace(path: str, description: str, public: bool):
    """Creates a namespace."""
    db = GerryDB()
    with db.context(notes=f'Creating namespace "{path}" from CLI') as ctx:
        try:
            ctx.namespaces.create(path=path, description=description, public=public)
        except ResultError as e:
            if "Failed to create namespace" in e.args[0]:
                print(f"Failed to create {path} namespace, already exists")
            else:
                raise e


@cli.command()
@click.argument("path")
@click.option("--description", required=True)
@click.option("--namespace", required=True)
@click.option("--source-url")
def geo_layer(path: str, description: str, namespace: str, source_url: Optional[str]):
    """Creates a geographic layer."""
    db = GerryDB(namespace=namespace)
    with db.context(notes=f'Creating geographic layer "{path}" from CLI') as ctx:
        try:
            ctx.geo_layers.create(
                path=path, description=description, source_url=source_url
            )
        except ResultError as e:
            if "Failed to create geographic layer" in e.args[0]:
                print(f"Failed to create {path} layer, already exists")
            else:
                raise e


if __name__ == "__main__":
    cli()  # pragma: no cover
