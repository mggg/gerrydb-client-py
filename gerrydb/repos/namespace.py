"""Repository for namespaces."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from gerrydb.repos.base import (
    NamespacedObjectRepo,
    err,
    normalize_path,
    online,
    write_context,
    normalize_path,
)
from gerrydb.schemas import Namespace, NamespaceCreate


if TYPE_CHECKING:
    from gerrydb.client import GerryDB, WriteContext  # pragma: no cover


@dataclass(frozen=True)
class NamespaceRepo(NamespacedObjectRepo):
    """Repository for namespaces."""

    session: "GerryDB"
    ctx: Optional["WriteContext"] = None

    @err("Failed to load namespaces")
    def all(self) -> list[Namespace]:
        """Gets all accessible namespaces."""
        # Due to our use of a namespace-oriented RBAC model, namespace
        # listings are not currently eligible for caching: unlike most other
        # resources exposed by the GerryDB API, namespace listings are filtered
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
        response = self.session.client.get(f"/namespaces/{path}")
        response.raise_for_status()
        return Namespace(**response.json())

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

        Namespaces are logical groupings of GerryDB objects; permissions are
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
        path = normalize_path(path, path_length=1)
        response = self.ctx.client.post(
            "/namespaces/",
            json=NamespaceCreate(
                path=path, public=public, description=description
            ).dict(),
        )

        response.raise_for_status()

        return Namespace(**response.json())

    def __getitem__(self, path: str) -> Optional[Namespace]:
        return self.get(path=path)
