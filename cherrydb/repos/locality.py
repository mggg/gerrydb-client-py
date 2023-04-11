"""Repository for localities."""
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Optional

from cherrydb.repos.base import ObjectRepo, err, normalize_path, online, write_context
from cherrydb.schemas import Locality, LocalityCreate, LocalityPatch

if TYPE_CHECKING:
    from cherrydb.client import CherryDB, WriteContext


@dataclass(frozen=True)
class LocalityRepo(ObjectRepo):
    """Repository for localities."""

    session: "CherryDB"
    ctx: Optional["WriteContext"] = None

    @err("Failed to load localities")
    def all(self) -> list[Locality]:
        """Gets all localities."""
        response = self.session.client.get("/localities/")
        response.raise_for_status()
        return [Locality(**loc) for loc in response.json()]

    @err("Failed to load locality")
    def get(self, path: str) -> Optional[Locality]:
        """Gets a locality by path.

        Raises:
            RequestError: If the locality cannot be read on the server side.
        """
        path = normalize_path(path)
        response = self.session.client.get(f"/localities/{path}", follow_redirects=True)
        response.raise_for_status()
        return Locality(**response.json())

    @err("Failed to create locality")
    @write_context
    @online
    def create(
        self,
        canonical_path: str,
        *,
        name: str,
        parent_path: str | None = None,
        default_proj: str | None = None,
        aliases: list[str] | None = None,
    ) -> Locality:
        """Creates a locality.

        Args:
            canonical_path: A short identifier for the locality (e.g. `massachusetts`).
            name: A full name of the locality (e.g. `Commonwealth of Massachusetts`).
            parent_path: A path to another locality that contains this locality.
            default_proj: A projection to use by default for all geographies
                in the locality, specified in WKT (well-known text) format.
            aliases: Alternate short identifiers for the locality.
                For instance, a state might be referred to by its postal code
                and its FIPS code.

        Raises:
            RequestError: If the locality cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new locality.
        """
        response = self.ctx.client.post(
            "/localities/",
            json=[
                LocalityCreate(
                    canonical_path=canonical_path,
                    name=name,
                    parent_path=parent_path,
                    default_proj=default_proj,
                    aliases=aliases,
                ).dict()
            ],
        )
        response.raise_for_status()

        return Locality(**response.json()[0])

    @err("Failed to create localities")
    @write_context
    @online
    def create_bulk(
        self,
        locs: list[LocalityCreate],
    ) -> list[Locality]:
        """Creates localities in bulk (primarily useful for bootstrapping a database).

        Args:
            locs: New locality requests.

        Raises:
        RequestError: If the localities cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new localities.
        """
        response = self.ctx.client.post(
            "/localities/",
            json=[loc.dict() for loc in locs],
        )
        response.raise_for_status()

        return [Locality(**loc) for loc in response.json()]

    @err("Failed to update locality")
    @write_context
    @online
    def update(self, path: str, *, aliases: list[str]) -> Locality:
        """Updates a locality.

        Currently, only adding aliases is supported.

        Args:
            path: Short identifier for the locality.
            aliases: Alternate short identifiers to add to the locality.

        Raises:
            RequestError: If the locality cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The updated locality.
        """
        response = self.ctx.client.patch(
            f"/localities/{path}", json=LocalityPatch(aliases=aliases).dict()
        )
        response.raise_for_status()

        return Locality(**response.json())

    def __getitem__(self, path: str) -> Optional[Locality]:
        return self.get(path=path)
