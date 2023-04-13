"""Repository for column sets."""
from typing import Optional, Union

from gerrydb.exceptions import RequestError
from gerrydb.repos.base import (
    NamespacedObjectRepo,
    err,
    namespaced,
    online,
    parse_path,
    write_context,
)
from gerrydb.schemas import Column, ColumnSet, ColumnSetCreate


class ColumnSetRepo(NamespacedObjectRepo[ColumnSet]):
    """Repository for column sets."""

    @err("Failed to create column set")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        columns: list[Union[str, Column]],
        description: str,
    ) -> ColumnSet:
        """Creates a column set.

        Args:
            path: A short identifier for the column set (e.g. `vap`).
            columns: The columns in the column set.
            description: Longform description of the column set.

        Raises:
            RequestError: If the column set cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new column set.
        """
        column_paths = []
        for column in columns:
            if isinstance(column, Column):
                col_namespace = column.namespace
                col_rel_path = column.canonical_path
            elif column.startswith("/"):
                col_namespace, col_rel_path = parse_path(column)
            else:
                col_namespace = namespace
                col_rel_path = column

            if col_namespace != namespace:
                raise RequestError(
                    "All columns in a column set must have the same namespace."
                )
            column_paths.append(col_rel_path)

        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=ColumnSetCreate(
                path=path,
                columns=column_paths,
                description=description,
            ).dict(),
        )
        response.raise_for_status()

        return self.schema(**response.json())
