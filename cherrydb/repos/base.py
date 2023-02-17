"""Bsae objects and utilities for CherryDb API object repositories."""
import uuid
from abc import ABC
from functools import wraps
from typing import Callable, Optional, Union

import httpx
import pydantic

from cherrydb.cache import CacheCollectionResult, CacheResult
from cherrydb.exceptions import OnlineError, ResultError, WriteContextError


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


def match(
    result: Optional[Union[CacheResult, CacheCollectionResult]]
) -> dict[str, str]:
    """Generates an `If-None-Match` header from a cache result."""
    return None if result is None else {"If-None-Match": f'"{result.etag}"'}


def etag(response: httpx.Response) -> Optional[bytes]:
    """Parses the `ETag` header from a response, if available."""
    return (
        uuid.UUID(response.headers["ETag"].strip('"')).bytes
        if "ETag" in response.headers
        else None
    )
