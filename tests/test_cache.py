"""Tests for CherryDB's local caching layer."""
import pytest
import uuid
from datetime import datetime, timezone
from cherrydb.schemas import BaseModel, ObjectMeta, ObjectCachePolicy


class UnversionedObject(BaseModel):
    """An unversioned object with metadata."""

    __cache_name__ = "unversioned"
    __cache_policy__ = ObjectCachePolicy.NONE

    foo: str
    meta: ObjectMeta


class ETagObject(BaseModel):
    """A ETag-versioned object with metadata."""

    __cache_name__ = "by_etag"
    __cache_policy__ = ObjectCachePolicy.ETAG

    bar: bytes
    meta: ObjectMeta


class TimestampObject(BaseModel):
    """A timestamp-versioned object with metadata."""

    __cache_name__ = "by_timestamp"
    __cache_policy__ = ObjectCachePolicy.TIMESTAMP

    baz: list[int]
    meta: ObjectMeta


class UncacheableObject(BaseModel):
    """A schema without a caching configuration."""

    bad: str


@pytest.fixture
def meta():
    """An instance of `ObjectMeta`."""
    return ObjectMeta(
        uuid=str(uuid.uuid4()),
        created_at=datetime.now(tz=timezone.utc),
        created_by="test@example.com",
    )


@pytest.fixture
def unversioned_obj(meta):
    """An instance of `UnversionedObject`."""
    return UnversionedObject(foo="test", meta=meta)


@pytest.fixture
def etag_obj(meta):
    """An instance of `ETagObject`."""
    return ETagObject(bar="test".encode("utf-8"), meta=meta)
