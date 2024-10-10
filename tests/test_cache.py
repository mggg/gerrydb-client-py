"""Tests for GerryDB's local caching layer."""

import pytest

from gerrydb.cache import CacheInitError, GerryCache
from tempfile import TemporaryDirectory
from pathlib import Path


@pytest.fixture
def cache():
    """An in-memory instance of `GerryCache`."""
    cache_dir = TemporaryDirectory()
    return GerryCache(
        ":memory:",
        data_dir=Path(cache_dir.name),
    )


def test_gerry_cache_init__no_schema_version(cache):
    cache._conn.execute("DELETE FROM cache_meta")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="no schema version"):
        GerryCache(cache._conn, cache.data_dir)


def test_gerry_cache_init__bad_schema_version(cache):
    cache._conn.execute("UPDATE cache_meta SET value='bad' WHERE key='schema_version'")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="expected schema version"):
        GerryCache(cache._conn, cache.data_dir)


def test_gerry_cache_init__missing_table(cache):
    cache._conn.execute("DROP TABLE view")
    cache._conn.commit()
    with pytest.raises(CacheInitError, match="missing table"):
        GerryCache(cache._conn, cache.data_dir)
