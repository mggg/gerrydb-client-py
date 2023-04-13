"""Tests for GerryDB's local caching layer."""
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from gerrydb.cache import CacheInitError, CacheObjectError, CachePolicyError, GerryCache
from gerrydb.schemas import BaseModel, ObjectCachePolicy, ObjectMeta


@pytest.fixture
def cache():
    """An in-memory instance of `GerryCache`."""
    return GerryCache(":memory:")


def test_gerry_cache_init__no_schema_version(cache):
    cache._conn.execute("DELETE FROM cache_meta")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="no schema version"):
        GerryCache(cache._conn)


def test_gerry_cache_init__bad_schema_version(cache):
    cache._conn.execute("UPDATE cache_meta SET value='bad' WHERE key='schema_version'")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="expected schema version"):
        GerryCache(cache._conn)


def test_gerry_cache_init__missing_table(cache):
    cache._conn.execute("DROP TABLE object")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="missing tables"):
        GerryCache(cache._conn)
