"""Repository for columns."""
from typing import Any, Optional, Union

import httpx
import numpy as np

from gerrydb.repos.base import (
    NamespacedObjectRepo,
    err,
    namespaced,
    online,
    write_context,
)
from gerrydb.schemas import (
    Column,
    ColumnCreate,
    ColumnKind,
    ColumnPatch,
    ColumnType,
    ColumnValue,
    Geography,
)


class ColumnRepo(NamespacedObjectRepo[Column]):
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
        source_url: Optional[str] = None,
        aliases: Optional[list[str]] = None,
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

        return self.schema(**response.json())

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

        return Column(**response.json())

    @err("Failed to set column values")
    @namespaced
    @write_context
    @online
    def set_values(
        self,
        path_or_col: Union[Column, str],
        namespace: Optional[str] = None,
        *,
        values: dict[Union[str, Geography], Any],
    ) -> None:
        """Sets the values of a column on a collection of geographies.

        Args:
            path_or_col: Short identifier for the column or a `Column` metadata object.
            namespace: Namespace of the column (used when `path_or_col` is a raw path).
            values:
                A mapping from geography paths or `Geography` metadata objects
                to column values.

        Raises:
            RequestError: If the values cannot be set on the server side.
        """
        path = path_or_col.path if isinstance(path_or_col, Column) else path_or_col

        response = self.ctx.client.put(
            f"{self.base_url}/{namespace}/{path}",
            json=[
                ColumnValue(
                    path=(
                        f"/{geo.namespace}/{geo.path}"
                        if isinstance(geo, Geography)
                        else geo
                    ),
                    value=value,
                ).dict()
                for geo, value in values.items()
            ],
        )
        response.raise_for_status()

        # TODO: what's the proper caching behavior here?

    @err("Failed to set column values")
    @namespaced
    @write_context
    @online
    async def async_set_values(
        self,
        path_or_col: Union[Column, str],
        namespace: Optional[str] = None,
        *,
        values: dict[Union[str, Geography], Any],
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        """Asynchronously sets the values of a column on a collection of geographies.

        Args:
            path_or_col: Short identifier for the column or a `Column` metadata object.
            namespace: Namespace of the column (used when `path_or_col` is a raw path).
            values:
                A mapping from geography paths or `Geography` metadata objects
                to column values.
            client: Asynchronous API client to use (for efficient connection pooling
                across batched requests).

        Raises:
            RequestError: If the values cannot be set on the server side.
        """
        path = path_or_col.path if isinstance(path_or_col, Column) else path_or_col

        ephemeral_client = client is None
        if ephemeral_client:
            params = self.ctx.client_params.copy()
            params["transport"] = httpx.AsyncHTTPTransport(retries=1)
            client = httpx.AsyncClient(**params)

        response = await client.put(
            f"{self.base_url}/{namespace}/{path}",
            json=[
                ColumnValue(
                    path=(
                        f"/{geo.namespace}/{geo.path}"
                        if isinstance(geo, Geography)
                        else geo
                    ),
                    value=_coerce(value),
                ).dict()
                for geo, value in values.items()
            ],
        )
        response.raise_for_status()

        if ephemeral_client:
            await client.aclose()

        # TODO: what's the proper caching behavior here?


def _coerce(val: Any) -> Any:
    """Coerces values for JSON serialization."""
    if isinstance(val, np.int64):
        return int(val)
    if isinstance(val, np.float64):
        return float(val)
    return val
