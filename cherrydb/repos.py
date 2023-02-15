"""CherryDB API object repositories."""
import httpx
import pydantic

from abc import ABC
from dataclasses import dataclass
from functools import wraps
from typing import Callable, Optional

from cherrydb.client import CherryDB
from cherrydb.schemas import BaseModel, Locality


class QueryError(ValueError):
    """Raised when a query cannot be translated to an API call."""


class ResultError(ValueError):
    """Raised when a query result cannot be loaded."""


class ObjectRepo(ABC):
    """A repository for a generic CherryDB object."""


def err(message: str) -> Callable:
    """Decorator for handling HTTP request and Pydantic validation errors."""

    def err_decorator(func: Callable) -> Callable:
        @wraps(func)
        def err_wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except pydantic.ValidationError as ex:
                raise ResultError(f"{message}: cannot parse response.") from ex
            except httpx.HTTPError as ex:
                raise ResultError(f"{message}: HTTP request failed.") from ex

        return err_wrapper

    return err_decorator


@dataclass(frozen=True)
class LocalityRepo(ObjectRepo):
    """Repository for localities."""

    session: CherryDB

    @err("Failed to load localities")
    def all(self) -> list[Locality]:
        cached = self.session.cache.all(obj=Locality)
        if self.session.offline:
            return [] if cached is None else list(cached.result.values())

        response = self.session.client.get(f"{self._base_url}/")
        locs = [Locality(**loc) for loc in response] 
        


@dataclass(frozen=True)
class ETagObjectRepo(ObjectRepo):
    """A repository for a generic ETag-versioned, namespaced CherryDB object."""

    session: CherryDB
    obj: BaseModel

    def all(self, namespace: Optional[str] = None) -> list[BaseModel]:
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise QueryError("No namespace specified for all() query.")

        if self.session.offline:
            return self.session.cache.all(obj=self.obj, namespace=self.namespace)
