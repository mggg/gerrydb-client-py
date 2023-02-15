"""Tests for CherryDB's local caching layer."""
import uuid
import sqlite3
import pytest

from datetime import datetime, timezone, timedelta
from cherrydb.cache import (
    CherryCache,
    CacheInitError,
    CacheObjectError,
    CachePolicyError,
)
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

    bar: str
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


def assert_close_to_now(timestamp: datetime):
    """Is `timestamp` within the last minute?"""
    assert datetime.now(tz=timezone.utc) - timestamp <= timedelta(minutes=1)


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
    return ETagObject(bar="test", meta=meta)


@pytest.fixture
def timestamp_obj(meta):
    """An instance of `TimestampObject`."""
    return TimestampObject(baz=[1, 2, 3], meta=meta)


@pytest.fixture
def uncacheable_obj(meta):
    """An instance of `UncacheableObject`."""
    return UncacheableObject(bad="worse")


@pytest.fixture
def cache():
    """An in-memory instance of `CherryCache`."""
    return CherryCache(":memory:")


def test_cherry_cache_init__no_schema_version(cache):
    cache._conn.execute("DELETE FROM cache_meta")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="no schema version"):
        CherryCache(cache._conn)


def test_cherry_cache_init__bad_schema_version(cache):
    cache._conn.execute("UPDATE cache_meta SET value='bad' WHERE key='schema_version'")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="expected schema version"):
        CherryCache(cache._conn)


def test_cherry_cache_init__missing_table(cache):
    cache._conn.execute("DROP TABLE object")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="missing tables"):
        CherryCache(cache._conn)


@pytest.mark.parametrize("schema", [UnversionedObject, ETagObject, TimestampObject])
def test_cherry_cache_get__missing(cache, schema):
    assert cache.get(schema, "test", "namespace") is None


def test_cherry_cache_insert_get__unversioned(cache, unversioned_obj):
    cache.insert(unversioned_obj, "test", "namespace")
    result = cache.get(UnversionedObject, "test", "namespace")

    assert result.result == unversioned_obj
    assert_close_to_now(result.cached_at)
    assert result.valid_from is None
    assert result.etag is None


def test_cherry_cache_insert_get__unversioned_rollback(cache, unversioned_obj):
    cache.insert(unversioned_obj, "test", "namespace", autocommit=False)
    assert cache.get(UnversionedObject, "test", "namespace") is not None

    cache.rollback()
    assert cache.get(UnversionedObject, "test", "namespace") is None


def test_cherry_cache_insert_get__etag(cache, etag_obj):
    etag_obj_older = etag_obj
    etag_obj_newer = etag_obj.copy(update={"bar": "newer"})
    etag_older = "older".encode("utf-8")
    etag_newer = "newer".encode("utf-8")

    cache.insert(etag_obj_older, "test", "namespace", etag=etag_older)
    cache.insert(etag_obj_newer, "test", "namespace", etag=etag_newer)

    result_no_etag = cache.get(ETagObject, "test", "namespace")
    assert result_no_etag.result == etag_obj_newer
    assert_close_to_now(result_no_etag.cached_at)
    assert result_no_etag.valid_from is None
    assert result_no_etag.etag == etag_newer

    assert cache.get(ETagObject, "test", "namespace", etag=etag_older) is None


def test_cherry_cache_insert_get__timestamp(cache, timestamp_obj):
    timestamp_obj_older = timestamp_obj
    timestamp_obj_newer = timestamp_obj.copy(update={"baz": [4, 5, 6]})
    timestamp_older = datetime(year=2023, month=1, day=1)
    timestamp_newer = datetime(year=2023, month=1, day=3)
    timestamp_middle = datetime(year=2023, month=1, day=2)

    cache.insert(timestamp_obj_newer, "test", "namespace", valid_from=timestamp_newer)
    cache.insert(timestamp_obj_older, "test", "namespace", valid_from=timestamp_older)

    result_no_timestamp = cache.get(TimestampObject, "test", "namespace")
    assert result_no_timestamp.result == timestamp_obj_newer
    assert_close_to_now(result_no_timestamp.cached_at)
    assert result_no_timestamp.valid_from == timestamp_newer
    assert result_no_timestamp.etag is None

    result_timestamp = cache.get(
        TimestampObject, "test", "namespace", at=timestamp_middle
    )
    assert result_timestamp.result == timestamp_obj_older
    assert_close_to_now(result_timestamp.cached_at)
    assert result_timestamp.valid_from == timestamp_older
    assert result_timestamp.etag is None


def test_cherry_cache_insert__uncacheable(cache, uncacheable_obj):
    with pytest.raises(CacheObjectError, match="Schema does not have"):
        cache.insert(uncacheable_obj, "test", "namespace")


def test_cherry_cache_insert__unversioned_with_etag(cache, unversioned_obj):
    with pytest.raises(CachePolicyError, match="is not versioned"):
        cache.insert(unversioned_obj, "test", "namespace", etag="123".encode("utf-8"))


def test_cherry_cache_insert__unversioned_with_timestamp(cache, unversioned_obj):
    with pytest.raises(CachePolicyError, match="is not versioned"):
        cache.insert(
            unversioned_obj,
            "test",
            "namespace",
            valid_from=datetime(year=2023, month=1, day=1),
        )


def test_cherry_cache_insert__etag_with_no_etag(cache, etag_obj):
    with pytest.raises(CachePolicyError, match="is ETag-versioned"):
        cache.insert(etag_obj, "test", "namespace")


def test_cherry_cache_insert__unversioned_with_timestamp(cache, etag_obj):
    with pytest.raises(CachePolicyError, match="is ETag-versioned"):
        cache.insert(
            etag_obj,
            "test",
            "namespace",
            valid_from=datetime(year=2023, month=1, day=1),
        )


def test_cherry_cache_insert__timestamp_with_no_timestamp(cache, timestamp_obj):
    with pytest.raises(CachePolicyError, match="is timestamp-versioned"):
        cache.insert(timestamp_obj, "test", "namespace")


def test_cherry_cache_insert__timestamp_with_etag(cache, timestamp_obj):
    with pytest.raises(CachePolicyError, match="is timestamp-versioned"):
        cache.insert(timestamp_obj, "test", "namespace", etag="123".encode("utf-8"))
