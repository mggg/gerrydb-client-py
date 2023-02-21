"""Base objects and utilities for CherryDB API object repositories."""
import uuid
from abc import ABC
from dataclasses import dataclass
from functools import wraps
from http import HTTPStatus
from typing import Callable, Generic, Optional, TypeVar, Union, TYPE_CHECKING

import httpx
import pydantic

from cherrydb.cache import CacheCollectionResult, CacheResult
from cherrydb.exceptions import (
    OnlineError,
    ResultError,
    RequestError,
    WriteContextError,
)
from cherrydb.schemas import BaseModel

if TYPE_CHECKING:
    from cherrydb.client import CherryDB, WriteContext


SchemaType = TypeVar("SchemaType", bound=BaseModel)


class ObjectRepo(ABC):
    """A repository for a generic CherryDB object."""


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


def match_etag(
    result: Optional[Union[CacheResult, CacheCollectionResult]]
) -> dict[str, str]:
    """Generates an `If-None-Match` header from a cache result."""
    if result is None or result.etag is None:
        return None
    etag_uuid = uuid.UUID(bytes=result.etag)
    return {"If-None-Match": f'"{etag_uuid}"'}


def parse_etag(response: httpx.Response) -> Optional[bytes]:
    """Parses the `ETag` header from a response, if available."""
    return (
        uuid.UUID(response.headers["ETag"].strip('"')).bytes
        if "ETag" in response.headers
        else None
    )


def normalize_path(path: str) -> str:
    """Normalizes a path (removes leading, trailing, and duplicate slashes)."""
    return "/".join(seg for seg in path.lower().split("/") if seg)


@dataclass(frozen=True)
class ETagObjectRepo(Generic[SchemaType]):
    """A repository for a generic ETag-versioned, namespaced CherryDB object."""

    schema: BaseModel
    base_url: str
    session: "CherryDB"
    ctx: Optional["WriteContext"] = None

    @err("Failed to load objects")
    def all(self, namespace: Optional[str] = None) -> list[SchemaType]:
        """Gets all objects in a namespace."""
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(
                "No namespace specified for all() query, and no default available."
            )

        cached = self.session.cache.all(obj=self.schema, namespace=namespace)
        if self.session.offline:
            return [] if cached is None else list(cached.result.values())

        response = self.session.client.get(
            f"{self.base_url}/{namespace}", headers=match_etag(cached)
        )
        if response.status_code == HTTPStatus.NOT_MODIFIED:
            return list(cached.result.values())
        response.raise_for_status()

        objs = [self.schema(**obj) for obj in response.json()]
        obj_etag = parse_etag(response)
        for obj in objs:
            self.session.cache.insert(
                obj=obj,
                path=obj.path,
                namespace=namespace,
                autocommit=False,
                etag=obj_etag,
            )
        self.session.cache.collect(
            obj=self.schema, namespace=namespace, etag=obj_etag, autocommit=False
        )
        self.session.cache.commit()
        return objs

    @err("Failed to load object")
    def get(self, path: str, namespace: Optional[str] = None) -> Optional[SchemaType]:
        """Gets an object by path.

        Raises:
            RequestError: If the object cannot be read on the server side,
                or if no namespace is specified.
        """
        path = normalize_path(path)
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(
                "No namespace specified for get() query, and no default available."
            )

        cached = self.session.cache.get(obj=self.schema, path=path, namespace=namespace)
        if self.session.offline:
            return None if cached is None else cached.result

        response = self.session.client.get(
            f"{self.base_url}/{namespace}/{path}", headers=match_etag(cached)
        )
        if response.status_code == HTTPStatus.NOT_MODIFIED:
            return cached.result
        response.raise_for_status()

        obj = self.schema(**response.json())
        obj_etag = parse_etag(response)
        self.session.cache.insert(
            obj=obj, path=obj.path, namespace=namespace, etag=obj_etag
        )

        return obj

    def __getitem__(self, path: str) -> Optional[SchemaType]:
        if path.startswith("/"):
            parts = path.split("/")
            try:
                namespace = parts[1]
                path_in_namespace = "/".join(parts[2:])
            except IndexError:
                raise KeyError(
                    "Absolute paths must contain a namespace and a "
                    "namespace-relative path, i.e. /<namespace>/<path>"
                )
            return self.get(path=path_in_namespace, namespace=namespace)

        return self.get(path=path)
