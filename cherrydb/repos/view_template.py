"""Repository for view templates."""
from typing import Collection, Optional, Union

from cherrydb.repos.base import (
    TimestampObjectRepo,
    err,
    namespaced,
    online,
    parse_etag,
    write_context,
)
from cherrydb.schemas import Column, ColumnSet, ViewTemplate, ViewTemplateCreate


class ViewTemplateRepo(TimestampObjectRepo[ViewTemplate]):
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
        members: Collection[Union[Column, ColumnSet, str]],
        description: str,
    ) -> ViewTemplate:
        """Creates a view template.

        Args:
            path: Short identifier for the view template.

        Raises:
            RequestError: If the view template cannot be created on the server side,
                if the parameters fail validation, or if no namespace is provided.

        Returns:
            Metadata for the new column.
        """
        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=ViewTemplateCreate(
                path=path,
                namespace=namespace,
                members=[
                    member if isinstance(member, str) else member.path_with_resource
                    for member in members
                ],
                description=description,
            ).dict(),
        )
        response.raise_for_status()

        obj = self.schema(**response.json())
        obj_etag = parse_etag(response)
        self.session.cache.insert(
            obj=obj,
            path=obj.path,
            namespace=namespace,
            valid_from=obj.valid_from,
        )
        return obj
