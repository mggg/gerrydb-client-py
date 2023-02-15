"""Tests for CherryDB's local caching layer."""
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from cherrydb.cache import (
    CacheInitError,
    CacheObjectError,
    CachePolicyError,
    CherryCache,
)
from cherrydb.schemas import BaseModel, ObjectCachePolicy, ObjectMeta


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


def test_cherry_cache_insert_collect_all__etag(cache, etag_obj):
    etag_obj2 = etag_obj.copy(update={"bar": "baz"})
    cache.insert(etag_obj, "test1", "namespace", etag=b"1-1")
    cache.insert(etag_obj2, "test2", "namespace", etag=b"2-1")
    cache.collect(ETagObject, "namespace", etag=b"collection1")

    # Take the first snapshot.
    result1 = cache.all(ETagObject, "namespace")
    assert result1.result == {"test1": etag_obj, "test2": etag_obj2}
    assert_close_to_now(result1.cached_at)
    assert result1.valid_at is None
    assert result1.etag == b"collection1"

    # Update and add some objects.
    etag_obj2_v2 = etag_obj.copy(update={"bar": "baz2"})
    etag_obj3 = etag_obj.copy(update={"bar": "none"})
    cache.insert(etag_obj2_v2, "test2", "namespace", etag=b"2-2")
    cache.insert(etag_obj3, "test3", "namespace", etag=b"3-1")
    cache.collect(ETagObject, "namespace", etag=b"collection2")

    # Take another snapshot.
    result2 = cache.all(ETagObject, "namespace")
    assert result2.result == {
        "test1": etag_obj,
        "test2": etag_obj2_v2,
        "test3": etag_obj3,
    }
    assert_close_to_now(result2.cached_at)
    assert result2.valid_at is None
    assert result2.etag == b"collection2"


def test_cherry_cache_insert_collect_all__timestamp(cache, timestamp_obj):
    timestamp_obj2 = timestamp_obj.copy(update={"baz": [2, 3]})
    cache.insert(timestamp_obj, "test1", "namespace", valid_from=datetime(2023, 1, 1))
    cache.insert(timestamp_obj2, "test2", "namespace", valid_from=datetime(2023, 1, 1))

    # Take two content-identical snapshots (same ETag, different points in time).
    cache.collect(
        TimestampObject,
        "namespace",
        etag=b"collection1",
        valid_at=datetime(2023, 1, 1),
    )
    cache.collect(
        TimestampObject,
        "namespace",
        etag=b"collection1",
        valid_at=datetime(2023, 1, 3),
    )

    # Retrieve the snapshot using a specified timestamp.
    result1 = cache.all(TimestampObject, "namespace", at=datetime(2023, 1, 2))
    assert result1.result == {"test1": timestamp_obj, "test2": timestamp_obj2}
    assert_close_to_now(result1.cached_at)
    assert result1.valid_at == datetime(2023, 1, 1)
    assert result1.etag == b"collection1"

    # Retrieve the snapshot without specifying a timestamp (gets the latest).
    result_latest = cache.all(TimestampObject, "namespace")
    assert result_latest.result == result1.result
    assert_close_to_now(result_latest.cached_at)
    assert result_latest.valid_at == datetime(2023, 1, 3)
    assert result_latest.etag == b"collection1"

    # Update and add some objects.
    timestamp_obj2_v2 = timestamp_obj.copy(update={"baz": [2, 3, 4]})
    timestamp_obj3 = timestamp_obj.copy(update={"baz": []})
    cache.insert(
        timestamp_obj2_v2, "test2", "namespace", valid_from=datetime(2023, 1, 4)
    )
    cache.insert(timestamp_obj3, "test3", "namespace", valid_from=datetime(2023, 1, 4))

    # Take another snapshot.
    cache.collect(
        TimestampObject,
        "namespace",
        etag=b"collection2",
        valid_at=datetime(2023, 1, 5),
    )

    # Retrieve the new snapshot.
    result2 = cache.all(TimestampObject, "namespace", at=datetime(2023, 1, 5))
    assert result2.result == {
        "test1": timestamp_obj,
        "test2": timestamp_obj2_v2,
        "test3": timestamp_obj3,
    }
    assert_close_to_now(result2.cached_at)
    assert result2.valid_at == datetime(2023, 1, 5)
    assert result2.etag == b"collection2"

    # It's unknown what happened between 2023-01-03 and 2023-01-05, so we shouldn't
    # be able to get a snapshot in that range (exclusive).
    assert cache.all(TimestampObject, "namespace", at=datetime(2023, 1, 4)) is None


def test_cherry_cache_collect__etag_no_etag(cache):
    with pytest.raises(CachePolicyError, match="ETag-versioned"):
        cache.collect(ETagObject)


def test_cherry_cache_collect__timestamp_no_etag(cache):
    with pytest.raises(CachePolicyError, match="timestamp-versioned"):
        cache.collect(TimestampObject, valid_at=datetime(2023, 1, 1))


def test_cherry_cache_collect__timestamp_no_valid_at(cache):
    with pytest.raises(CachePolicyError, match="timestamp-versioned"):
        cache.collect(TimestampObject, etag=b"123")


def test_cherry_cache_collect__unversioned(cache):
    with pytest.raises(CachePolicyError, match="does not support collection"):
        cache.collect(UnversionedObject)


def test_cherry_cache_all__etag_at(cache):
    with pytest.raises(CachePolicyError, match="ETag-versioned"):
        cache.all(ETagObject, at=datetime(2023, 1, 1))


def test_cherry_cache_alll__unversioned(cache):
    with pytest.raises(CachePolicyError, match="does not support collection"):
        cache.all(UnversionedObject)
