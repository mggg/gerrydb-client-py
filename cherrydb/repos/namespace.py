"""Repository for namespaces."""
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Optional

from cherrydb.repos.base import (
    ObjectRepo,
    err,
    match_etag,
    normalize_path,
    online,
    parse_etag,
    write_context,
)
from cherrydb.schemas import Namespace, NamespaceCreate

if TYPE_CHECKING:
    from cherrydb.client import CherryDB, WriteContext


@dataclass(frozen=True)
class NamespaceRepo(ObjectRepo):
    """Repository for namespaces."""

    session: "CherryDB"
    ctx: Optional["WriteContext"] = None

    @err("Failed to load namespaces")
    @online
    def all(self) -> list[Namespace]:
        """Gets all accessible namespaces."""
        # Due to our use of a namespace-oriented RBAC model, namespace
        # listings are not currently eligible for caching: unlike most other
        # resources exposed by the CherryDB API, namespace listings are filtered
        # based on a user's permissions, which may change over time.

        response = self.session.client.get("/namespaces/")
        response.raise_for_status()
        return [Namespace(**ns) for ns in response.json()]

    @err("Failed to load namespace")
    def get(self, path: str) -> Optional[Namespace]:
        """Gets a namespace by path.

        Raises:
            RequestError: If the namespace cannot be read on the server side.
        """
        path = normalize_path(path)
        cached = self.session.cache.get(obj=Namespace, path=path, namespace=path)
        if self.session.offline:
            return None if cached is None else cached.result

        response = self.session.client.get(
            f"/namespaces/{path}", headers=match_etag(cached)
        )
        if response.status_code == HTTPStatus.NOT_MODIFIED:
            return cached.result
        response.raise_for_status()

        namespace = Namespace(**response.json())
        namespace_etag = parse_etag(response)
        self.session.cache.insert(
            obj=namespace,
            path=namespace.path,
            namespace=namespace.path,
            etag=namespace_etag,
        )

        return namespace

    @err("Failed to create namespace")
    @write_context
    @online
    def create(
        self,
        path: str,
        *,
        public: bool,
        description: str,
    ) -> Namespace:
        """Creates a namespace.

        Namespaces are logical groupings of CherryDB objects; permissions are
        typically granted at the namespace level.

        Args:
            path: Short descriptor for the namespace (e.g. `census.2020`).
            public: If `True`, the namespace is accessible to all other users.
                If `False`, the namespace is only accessible to superusers
                or users who have explicitly been granted access.
                **Namespaces cannot be converted from public to private.**
            description: Longform description of the scope of the namespace
                (e.g. "2020 Census PL 94-171" or "Litigation work in Georgia").

        Raises:
            RequestError: If the namespace cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new namespace.
        """
        response = self.ctx.client.post(
            "/namespaces/",
            json=NamespaceCreate(
                path=path, public=public, description=description
            ).dict(),
        )
        response.raise_for_status()

        namespace = Namespace(**response.json())
        namespace_etag = parse_etag(response)
        self.session.cache.insert(
            obj=namespace,
            path=namespace.path,
            namespace=namespace.path,
            etag=namespace_etag,
        )
        return namespace

    def __getitem__(self, path: str) -> Optional[Namespace]:
        return self.get(path=path)
