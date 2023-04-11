"""Tests for CherryDB's local caching layer."""
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
