"""Repository for view templates."""

from typing import Collection, Optional, Union

from gerrydb.repos.base import (
    NamespacedObjectRepo,
    err,
    namespaced,
    online,
    normalize_path,
    write_context,
)
from gerrydb.schemas import Column, ColumnSet, ViewTemplate, ViewTemplateCreate
from gerrydb.logging import log


def _normalize_columns(
    namespace: str, columns: Collection[Union[Column, str]]
) -> list[str]:
    """
    Constructs normalized paths of the columns objects that are passed in.

    Args:
        namespace: Namespace to use for the columns.
        columns: Columns to normalize.

    Returns:
        List of normalized paths of the columns.
    """
    return_list = []

    for item in columns:
        if isinstance(item, Column):
            return_list.append(item.path_with_resource)
        else:
            item = normalize_path(item)
            split_path = item.split("/")
            if len(split_path) == 1:
                return_list.append(f"/columns/{namespace}/{split_path[0]}")
            elif len(split_path) == 2:
                return_list.append(f"columns/{split_path[0]}/{split_path[1]}")
            elif len(split_path) == 3:
                if split_path[0] != "columns":
                    raise ValueError(f"Invalid column path: {item}")
                return_list.append(f"{split_path[0]}/{split_path[1]}/{split_path[2]}")
            else:
                raise ValueError(
                    f"Column path must be in the form of either "
                    "/columns/namespace/column_name or just column_name"
                )

    return return_list


def _normalize_column_sets(
    namespace: str, column_sets: Collection[Union[ColumnSet, str]]
) -> list[str]:
    """
    Constructs normalized paths of the column sets objects that are passed in.

    Args:
        namespace: Namespace to use for the column sets.
        column_sets: Column sets to normalize.

    Returns:
        List of normalized paths of the column sets.
    """
    return_list = []

    for item in column_sets:
        if isinstance(item, ColumnSet):
            return_list.append(item.path_with_resource)
        else:
            item = normalize_path(item)
            split_path = item.split("/")
            if len(split_path) == 1:
                return_list.append(f"/column-sets/{namespace}/{split_path[0]}")
            elif len(split_path) == 2:
                return_list.append(f"/column-sets/{split_path[0]}/{split_path[1]}")
            elif len(split_path) == 3:
                if split_path[0] != "column-sets":
                    raise ValueError(f"Invalid column set path: {item}")
                return_list.append(f"/{split_path[0]}/{namespace}/{split_path[-1]}")
            else:
                raise ValueError(
                    f"Column_set path must be in the form of either "
                    "/column-sets/namespace/column_set_name or just column_set_name"
                )

    log.debug("COLUMN SETS: %s", return_list)
    return return_list


class ViewTemplateRepo(NamespacedObjectRepo[ViewTemplate]):
    """Repository for view templates."""

    @err("Failed to create view template")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        columns: Optional[Collection[Union[Column, str]]] = list(),
        column_sets: Optional[Collection[Union[ColumnSet, str]]] = list(),
        description: str,
    ) -> ViewTemplate:
        """Creates a view template.

        Args:
            path: Short identifier for the view template.
            namespace: Namespace to create the view template in.
            columns: Columns to include in the view template. The columns can be either
                a `Column` object or a string representing the column name of the form
                `{column_name}` or `/columns/{namespace}/{column_name}`. In the event that
                the namespace is not provided, the namespace of the ViewTemplate will be used.
            column_sets: Column sets to include in the view template. The column sets can be either
                a `ColumnSet` object or a string representing the column set name of the form
                `{column_set_name}` or `/column-sets/{namespace}/{column_set_name}`. In the event
                that the namespace is not provided, the namespace of the ViewTemplate will be used.
            description: Description of what is contained in the view template.

        Raises:
            RequestError: If the view template cannot be created on the server side,
                if the parameters fail validation, or if no namespace is provided.

        Returns:
            Metadata for the new column.
        """
        assert (
            isinstance(columns, list)
            or isinstance(columns, set)
            or isinstance(columns, tuple)
        ), "'columns' must be a list, set, or tuple"
        assert (
            isinstance(column_sets, list)
            or isinstance(column_sets, set)
            or isinstance(column_sets, tuple)
        ), "'column_sets' must be a list, set, or tuple"

        if len(columns) == 0 and len(column_sets) == 0:
            raise ValueError("Must provide at least one of columns or column_sets.")

        members = []

        if len(columns) > 0:
            members.extend(_normalize_columns(namespace, columns))
        if len(column_sets) > 0:
            members.extend(_normalize_column_sets(namespace, column_sets))

        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=ViewTemplateCreate(
                path=path,
                namespace=namespace,
                members=members,
                description=description,
            ).dict(),
        )
        response.raise_for_status()

        return self.schema(**response.json())
