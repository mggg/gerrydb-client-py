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
    normalize_path,
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

import logging

log = logging.getLogger()


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
        path = normalize_path(path)
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
        clean_path = normalize_path(f"{self.base_url}/{namespace}/{path}")
        response = self.ctx.client.patch(
            clean_path,
            json=ColumnPatch(aliases=aliases).dict(),
        )
        response.raise_for_status()

        return Column(**response.json())

    @err("Failed to retrieve column names")
    @online
    def all(self) -> list[str]:
        response = self.session.client.get(f"/columns/{self.session.namespace}")
        response.raise_for_status()

        return [Column(**item) for item in response.json()]

    @err("Failed to retrieve column")
    @online
    def get(self, path: str) -> Column:
        path = normalize_path(path)
        response = self.session.client.get(f"/columns/{self.session.namespace}/{path}")
        response.raise_for_status()
        return Column(**response.json())

    @err("Failed to set column values")
    @namespaced
    @write_context
    @online
    def set_values(
        self,
        path: Optional[str] = None,
        namespace: Optional[str] = None,
        *,
        col: Optional[Column] = None,
        values: dict[Union[str, Geography], Any],
    ) -> None:
        """Sets the values of a column on a collection of geographies.

        Args:
            path: Short identifier for the column. Only this or `col` should be provided.
                If both are provided, the path attribute of `col` will be used in place
                of the passed `path` argument.
            col: `Column` metadata object. If the `path` is not provided, the column's
                path will be used.
            namespace: Namespace of the column (used when `path_or_col` is a raw path).
            values:
                A mapping from geography paths or `Geography` metadata objects
                to column values.

        Raises:
            RequestError: If the values cannot be set on the server side.
        """
        assert path is None or isinstance(path, str)
        assert col is None or isinstance(col, Column)

        if path is None and col is None:
            raise ValueError("Either `path` or `col` must be provided.")

        path = col.path if col is not None else path
        clean_path = normalize_path(f"{self.base_url}/{namespace}/{path}")

        response = self.ctx.client.put(
            clean_path,
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
        path: Optional[str] = None,
        namespace: Optional[str] = None,
        *,
        col: Optional[Column] = None,
        values: dict[Union[str, Geography], Any],
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        """Asynchronously sets the values of a column on a collection of geographies.

        Args:
            path: Short identifier for the column. Only this or `col` should be provided.
                If both are provided, the path attribute of `col` will be used in place
                of the passed `path` argument.
            col: `Column` metadata object. If the `path` is not provided, the column's
                path will be used.
            namespace: Namespace of the column (used when `path_or_col` is a raw path).
            values:
                A mapping from geography paths or `Geography` metadata objects
                to column values.
            client: Asynchronous API client to use (for efficient connection pooling
                across batched requests).

        Raises:
            RequestError: If the values cannot be set on the server side.
        """
        assert path is None or isinstance(path, str)
        assert col is None or isinstance(col, Column)

        if path is None and col is None:
            raise ValueError("Either `path` or `col` must be provided.")

        path = col.path if col is not None else path
        clean_path = normalize_path(f"{self.base_url}/{namespace}/{path}")

        ephemeral_client = client is None
        if ephemeral_client:
            params = self.ctx.client_params.copy()
            params["transport"] = httpx.AsyncHTTPTransport(retries=1)
            client = httpx.AsyncClient(**params)

        json = [
            ColumnValue(
                path=(
                    f"/{geo.namespace}/{geo.path}"
                    if isinstance(geo, Geography)
                    else geo
                ),
                value=_coerce(value),
            ).dict()
            for geo, value in values.items()
        ]
        response = await client.put(
            clean_path,
            json=json,
        )

        if response.status_code != 204:
            log.debug(f"For path = {path} and col = {col} returned {response}")

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
