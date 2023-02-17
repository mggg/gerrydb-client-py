"""CherryDB API object repositories."""
import httpx
import pydantic
from abc import ABC
from dataclasses import dataclass
from functools import wraps
from typing import Callable, Optional, TYPE_CHECKING

from cherrydb.exceptions import (
    ResultError,
    RequestError,
    OnlineError,
    WriteContextError,
)
from cherrydb.schemas import BaseModel, Locality, LocalityCreate

if TYPE_CHECKING:
    from cherrydb.client import CherryDB, WriteContext


def normalize_path(path: str) -> str:
    """Normalizes a path (removes leading, trailing, and duplicate slashes)."""
    return "/".join(seg for seg in path.lower().split("/") if seg)


def err(message: str) -> Callable:
    """Decorator for handling HTTP request and Pydantic validation errors."""

    def err_decorator(func: Callable) -> Callable:
        @wraps(func)
        def err_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except pydantic.ValidationError as ex:
                raise ResultError(f"{message}: cannot parse response.") from ex
            except httpx.HTTPError as ex:
                raise ResultError(f"{message}: HTTP request failed.") from ex

        return err_wrapper

    return err_decorator


def online(func: Callable) -> Callable:
    """Decorator for marking online-only operations."""

    @wraps(func)
    def online_wrapper(*args, **kwargs):
        if args[0].session.offline:
            raise OnlineError("Operation can only be performed in online mode.")
        return func(*args, **kwargs)

    return online_wrapper


def write_context(func: Callable) -> Callable:
    """Decorator for marking operations that require a write context."""

    @wraps(func)
    def write_context_wrapper(*args, **kwargs):
        if args[0].ctx is None:
            raise WriteContextError("Operation requires a write context.")
        return func(*args, **kwargs)

    return write_context_wrapper


class ObjectRepo(ABC):
    """A repository for a generic CherryDB object."""


@dataclass(frozen=True)
class LocalityRepo(ObjectRepo):
    """Repository for localities."""

    session: "CherryDB"
    ctx: Optional["WriteContext"] = None

    @err("Failed to load localities")
    def all(self) -> list[Locality]:
        cached = self.session.cache.all(obj=Locality)
        if self.session.offline:
            return [] if cached is None else list(cached.result.values())

        response = self.session.client.get(
            "/localities/",
            headers=None if cached.etag is None else {"If-None-Match": cached.etag},
        )
        response.raise_for_status()
        return [Locality(**loc) for loc in response.json()]

    @err("Failed to create locality")
    @write_context
    @online
    def create(
        self,
        *,
        canonical_path: str,
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
        return Locality(**response.json())


@dataclass(frozen=True)
class ETagObjectRepo(ObjectRepo):
    """A repository for a generic ETag-versioned, namespaced CherryDB object."""

    session: "CherryDB"
    obj: BaseModel

    def all(self, namespace: Optional[str] = None) -> list[BaseModel]:
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError("No namespace specified for all() query.")

        if self.session.offline:
            return self.session.cache.all(obj=self.obj, namespace=self.namespace)
