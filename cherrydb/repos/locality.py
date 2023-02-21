"""Repository for localities."""
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
        cached = self.session.cache.all(obj=Locality, namespace="")
        if self.session.offline:
            return [] if cached is None else list(cached.result.values())

        response = self.session.client.get("/localities/", headers=match_etag(cached))
        if response.status_code == HTTPStatus.NOT_MODIFIED:
            return list(cached.result.values())
        response.raise_for_status()

        locs = [Locality(**loc) for loc in response.json()]
        loc_etag = parse_etag(response)
        for loc in locs:
            self.session.cache.insert(
                obj=loc,
                path=loc.canonical_path,
                namespace="",
                autocommit=False,
                etag=loc_etag,
            )
        self.session.cache.collect(
            obj=Locality, namespace="", etag=loc_etag, autocommit=False
        )
        self.session.cache.commit()
        return locs

    @err("Failed to load locality")
    def get(self, path: str) -> Optional[Locality]:
        """Gets a locality by path.

        Raises:
            RequestError: If the locality cannot be read on the server side.
        """
        path = normalize_path(path)
        cached = self.session.cache.get(obj=Locality, path=path, namespace="")
        if self.session.offline:
            return None if cached is None else cached.result

        response = self.session.client.get(
            f"/localities/{path}", headers=match_etag(cached), follow_redirects=True
        )
        if response.status_code == HTTPStatus.NOT_MODIFIED:
            return cached.result
        response.raise_for_status()

        loc = Locality(**response.json())
        loc_etag = parse_etag(response)
        self.session.cache.insert(
            obj=loc, path=loc.canonical_path, namespace="", etag=loc_etag
        )

        return loc

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
            json=LocalityCreate(
                canonical_path=canonical_path,
                name=name,
                parent_path=parent_path,
                default_proj=default_proj,
                aliases=aliases,
            ).dict(),
        )
        response.raise_for_status()

        loc = Locality(**response.json())
        loc_etag = parse_etag(response)
        self.session.cache.insert(
            obj=loc, path=loc.canonical_path, namespace="", etag=loc_etag
        )
        return loc

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

        loc = Locality(**response.json())
        loc_etag = parse_etag(response)
        self.session.cache.insert(
            obj=loc, path=loc.canonical_path, namespace="", etag=loc_etag
        )
        return loc

    def __getitem__(self, path: str) -> Optional[Locality]:
        return self.get(path=path)
