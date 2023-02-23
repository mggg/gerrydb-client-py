"""Repository for columns."""
from typing import Optional

from cherrydb.repos.base import (
    ETagObjectRepo,
    err,
    namespaced,
    online,
    parse_etag,
    write_context,
)
from cherrydb.schemas import Column, ColumnCreate, ColumnKind, ColumnPatch, ColumnType


class ColumnRepo(ETagObjectRepo[Column]):
    """Repository for columns."""

    @err("Failed to create column")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        column_kind: ColumnKind,
        column_type: ColumnType,
        description: str,
        source_url: str | None = None,
        aliases: list[str] | None = None,
    ) -> Column:
        """Creates a tabular data column.

        Args:
            canonical_path: A short identifier for the column (e.g. `total_pop`).
            column_kind: Meaning of the column -- is a column value a `count`, a
                `percent`, a `categorical` label, or something `other`?
            column_type: Data type of the column (`int`, `float`, `bool`, `str`,
                or `json`-serializable blob).
            description: Longform description of the column
                (e.g. `2020 U.S. Census total population`).
            source_url: Optional original source of the column
                (e.g. a link to documentation on the U.S. Census Bureau website).
             aliases: Alternate short identifiers for the column.
                For instance, a column might be referred to by its numerical
                Census identifier and a more descriptive name.

        Raises:
            RequestError: If the column cannot be created on the server side,
                if the parameters fail validation, or if no namespace is provided.

        Returns:
            Metadata for the new column.
        """
        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=ColumnCreate(
                canonical_path=path,
                namespace=namespace,
                description=description,
                kind=column_kind,
                type=column_type,
                source_url=source_url,
                aliases=aliases,
            ).dict(),
        )
        response.raise_for_status()

        obj = self.schema(**response.json())
        obj_etag = parse_etag(response)
        self.session.cache.insert(
            obj=obj, path=obj.canonical_path, namespace=namespace, etag=obj_etag
        )
        return obj

    @err("Failed to update column")
    @namespaced
    @write_context
    @online
    def update(
        self, path: str, namespace: Optional[str] = None, *, aliases: list[str]
    ) -> Column:
        """Updates a tabular data column.

        Currently, only adding aliases is supported.

        Args:
            path: Short identifier for the column.
            aliases: Alternate short identifiers to add to the column.

        Raises:
            RequestError: If the column cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The updated column.
        """
        response = self.ctx.client.patch(
            f"{self.base_url}/{namespace}/{path}",
            json=ColumnPatch(aliases=aliases).dict(),
        )
        response.raise_for_status()

        col = Column(**response.json())
        col_etag = parse_etag(response)
        self.session.cache.insert(
            obj=col, path=col.canonical_path, namespace=namespace, etag=col_etag
        )
        return col
