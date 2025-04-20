"""Base objects and utilities for GerryDB API object repositories."""

from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Callable, Generic, Optional, Tuple, TypeVar, Union

import httpx
import pydantic

from gerrydb.exceptions import (
    OnlineError,
    RequestError,
    ResultError,
    WriteContextError,
    GerryPathError,
)
from gerrydb.schemas import BaseModel
from uvicorn.config import logger as log

if TYPE_CHECKING:
    from gerrydb.client import GerryDB, WriteContext


SchemaType = TypeVar("SchemaType", bound=BaseModel)

NAMESPACE_ERR = "No namespace specified for all() query, and no default available."


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
                reason = (
                    f" Reason: {ex.response.json()}" if hasattr(ex, "response") else ""
                )
                raise ResultError(f"{message}: HTTP request failed.{reason}") from ex

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


def namespaced(func: Callable) -> Callable:
    """Decorator for automatic resolution of namespaces from session defaults.

    `func` must follow the convention for CRUD functions within a repository class:
    the first parameter is `self`, the second parameter is the resource `path`,
    and the optional third parameter is the `namespace`.
    """

    @wraps(func)
    def namespaced_wrapper(*args, **kwargs):
        repo_obj = args[0]

        # Both `path` and `namespace` can be passed as positional or
        # keyword arguments, which necessitates this messy parsing.
        if "path" in kwargs:
            path = kwargs["path"]
            del kwargs["path"]
        else:
            path = args[1]

        if "namespace" in kwargs:
            namespace = kwargs["namespace"]
            del kwargs["namespace"]
        else:
            namespace = (
                args[2]
                if len(args) >= 3 and args[2] is not None
                else repo_obj.session.namespace
            )

        if namespace is None:
            raise RequestError(
                "No namespace specified and no session-level default available."
            )

        return func(repo_obj, path, namespace, *args[3:], **kwargs)

    return namespaced_wrapper


def write_context(func: Callable) -> Callable:
    """Decorator for marking operations that require a write context."""

    @wraps(func)
    def write_context_wrapper(*args, **kwargs):
        if args[0].ctx is None:
            raise WriteContextError("Operation requires a write context.")
        return func(*args, **kwargs)

    return write_context_wrapper


# These characters are most likely to appear in the resource_id part of
# a path (typically the last segment). Exclusion of these characters
# prevents ogr2ogr fails and helps protect against malicious code injection.
INVALID_PATH_SUBSTRINGS = set(
    {
        "..",
        " ",
        ";",
    }
)


def normalize_path(
    path: str, case_sensitive_uid: bool = False, path_length: Optional[int] = None
) -> str:
    """Normalizes a path (removes leading, trailing, and duplicate slashes, and
    lowercases the path if `case_sensitive` is `False`).

    Some paths, such as paths containing GEOIDs, are case-sensitive in the last
    segment. In these cases, `case_sensitive` should be set to `True`.
    """
    for item in INVALID_PATH_SUBSTRINGS:
        if item in path:
            raise GerryPathError(
                f"Invalid path: '{path}'. Please remove or replace the following substring "
                f"wherever it occurs: '{item}'"
            )

    if case_sensitive_uid:
        # Don't make a list with empty things. So substrings like "///" don't
        # cause issues
        path_list = [seg for seg in path.strip().split("/") if seg]

        if path_length is not None and len(path_list) != path_length:
            raise GerryPathError(
                f"Invalid path: '{path}'. This path has {len(path_list)} segment(s), but "
                f"should have {path_length}"
            )

        return "/".join(
            seg.lower() if i < len(path_list) - 1 else seg
            for i, seg in enumerate(path_list)
            if seg
        )

    path_list = [seg for seg in path.strip().lower().split("/") if seg]

    if path_length is not None and len(path_list) != path_length:
        raise GerryPathError(
            f"Invalid path: '{path}'. This path has {len(path_list)} segment(s), but "
            f"should have {path_length}."
        )

    return "/".join(path_list)


class ObjectRepo:
    """Base class for object repositories."""


@dataclass(frozen=True)
class NamespacedObjectRepo(Generic[SchemaType]):
    """A repository for a generic namespaced GerryDB object."""

    schema: BaseModel
    base_url: str
    session: "GerryDB"
    ctx: Optional["WriteContext"] = None

    @err("Failed to load objects")
    def all(self, namespace: Optional[str] = None) -> list[SchemaType]:
        """Gets all objects in a namespace."""
        namespace = self.session.namespace if namespace is None else namespace
        if namespace is None:
            raise RequestError(NAMESPACE_ERR)

        response = self.session.client.get(f"{self.base_url}/{namespace}")
        response.raise_for_status()
        return [self.schema(**obj) for obj in response.json()]

    @err("Failed to load object")
    @namespaced
    def get(self, path: str, namespace: Optional[str] = None) -> Optional[SchemaType]:
        """Gets an object by path.

        Raises:
            RequestError: If the object cannot be read on the server side,
                or if no namespace is specified.
        """
        path = normalize_path(path, case_sensitive_uid="geometries" in self.base_url)

        response = self.session.client.get(f"{self.base_url}/{namespace}/{path}")
        response.raise_for_status()
        return self.schema(**response.json())

    def __getitem__(self, key: Union[str, Tuple[str, str]]) -> Optional[SchemaType]:
        path = key
        assert isinstance(key, str) or (
            isinstance(key, tuple)
            and len(key) == 2
            and all(isinstance(val, str) for val in key)
        ), "Key must be a path string or a tuple of two strings (namespace, path)"

        if isinstance(key, str):
            path = key.strip("/")
            if len(path.split("/")) != 1:
                raise GerryPathError(
                    f"Error parsing '{path}'. Path cannot contain slashes. Please either "
                    f"pass the name of the path or a (namespace, path) tuple."
                )
            return self.get(path=path)

        # Desired namespace for retrieval and namespace of path may differ
        namespace, path = key
        return self.get(path=path, namespace=namespace)
