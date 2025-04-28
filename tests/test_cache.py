"""Tests for GerryDB's local caching layer."""

import pytest

from gerrydb.cache import CacheInitError, GerryCache
from tempfile import TemporaryDirectory
from pathlib import Path
from datetime import datetime
import os
import logging
import sqlite3


@pytest.fixture
def cache():
    """An in-memory instance of `GerryCache`."""
    cache_dir = TemporaryDirectory()
    return GerryCache(
        ":memory:",
        data_dir=Path(cache_dir.name),
    )


@pytest.fixture
def cache_small(tmp_path):
    """An in-memory instance of `GerryCache`."""
    return GerryCache(
        ":memory:",
        data_dir=Path(tmp_path),
        max_size_gb=0.000001,
    )


def test_gerry_cache_bad_init__no_conn():
    with pytest.raises(CacheInitError, match="Failed to load/initialize"):
        GerryCache(
            database="/bad/path/to/database",
            data_dir=Path("/bad/path/to/data"),
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


def test_get_missing_graph_gpkg(cache):
    assert cache.get_graph_gpkg("foo", "bar") is None


def test_get_bad_graph_gpkg(cache):
    cache._conn.execute(
        "INSERT INTO graph (namespace, render_id, path, cached_at, file_size_kb) VALUES (?, ?, ?, ?, ?)",
        ("foo", "bar", "bad_path.gpkg", datetime.now().isoformat(), 1024),
    )

    assert cache.get_graph_gpkg("foo", "bar") is None


def test_basic_upsert_and_get_graph(tmp_path, cache_small):
    ns, path, rid = "ns1", "p1", "r1"
    content = b"hello world"

    gpkg_path = cache_small.upsert_graph_gpkg(ns, path, rid, content)
    assert gpkg_path.exists()
    assert (tmp_path / "r1.gpkg").read_bytes() == content

    cur = cache_small._conn.execute(
        "SELECT namespace, path, render_id, file_size_kb FROM graph"
    )
    row = cur.fetchone()
    assert row[0] == ns and row[1] == path and row[2] == rid
    assert row[3] == 1

    got = cache_small.get_graph_gpkg(ns, path)
    assert got == gpkg_path
    assert cache_small.get_graph_gpkg("no", "no") is None


def test_upsert_replaces_previous_and_deletes_old_files(tmp_path, cache_small):
    ns, path = "ns2", "p2"
    cache_small.upsert_graph_gpkg(ns, path, "old", b"a" * 100)
    assert (tmp_path / "old.gpkg").exists()

    cache_small.upsert_graph_gpkg(ns, path, "new", b"b" * 200)
    assert not (tmp_path / "old.gpkg").exists()
    assert (tmp_path / "new.gpkg").exists()

    rows = cache_small._conn.execute("SELECT render_id FROM graph").fetchall()
    assert rows == [("new",)]


def test_eviction_policy(tmp_path, cache_small):
    ns, p = "ns3", "p"
    cache_small.upsert_graph_gpkg(ns, f"{p}1", "r1", b"x" * 512)  # 512 bytes ⇒ 1 KB
    cache_small.upsert_graph_gpkg(ns, f"{p}2", "r2", b"y" * 512)

    assert not (tmp_path / "r1.gpkg").exists()
    assert (tmp_path / "r2.gpkg").exists()

    rows = cache_small._conn.execute("SELECT render_id FROM graph").fetchall()
    assert rows == [("r2",)]


def test_upsert_and_get_view(tmp_path, cache_small):
    ns, path, rid = "ns4", "vpath", "v1"
    content = b"viewdata"

    gpkg = cache_small.upsert_view_gpkg(ns, path, rid, content)
    assert gpkg.exists()
    assert (tmp_path / "v1.gpkg").read_bytes() == content

    assert cache_small.get_view_gpkg(ns, path) == gpkg

    os.remove(tmp_path / "v1.gpkg")
    assert cache_small.get_view_gpkg(ns, path) is None


def test_invalid_schema_raises(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE cache_meta(key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    with pytest.raises(CacheInitError):
        GerryCache(database=conn, data_dir=tmp_path)


def test_eviction_missing_file_graph(tmp_path, cache_small, monkeypatch, caplog):
    ns = "ns_ev"
    cache_small.upsert_graph_gpkg(ns, "p1", "r1", b"x" * 512)

    monkeypatch.setattr(
        os, "remove", lambda p: (_ for _ in ()).throw(FileNotFoundError)
    )

    caplog.set_level(logging.DEBUG, logger="gerrydb")
    logging.getLogger("gerrydb").addHandler(caplog.handler)

    cache_small.upsert_graph_gpkg(ns, "p2", "r2", b"y" * 512)

    assert any(
        "Could not find the render file: r1.gpkg" in rec.getMessage()
        for rec in caplog.records
    ), f"got: {[r.getMessage() for r in caplog.records]}"

    assert (tmp_path / "r2.gpkg").exists()


def test_eviction_missing_file_view(tmp_path, cache_small, monkeypatch, caplog):
    ns = "nsV"
    cache_small.upsert_view_gpkg(ns, "p1", "v1", b"x" * 512)

    monkeypatch.setattr(
        os, "remove", lambda p: (_ for _ in ()).throw(FileNotFoundError)
    )
    caplog.set_level(logging.DEBUG, logger="gerrydb")
    logging.getLogger("gerrydb").addHandler(caplog.handler)

    cache_small.upsert_view_gpkg(ns, "p2", "v2", b"y" * 512)

    assert any(
        "Could not find the render file: v1.gpkg" in rec.getMessage()
        for rec in caplog.records
    ), f"got: {[r.getMessage() for r in caplog.records]}"

    assert (tmp_path / "v2.gpkg").exists()


def test_upsert_view_removes_old_extensions(tmp_path, cache_small):
    ns, path = "ns_view", "p_view"
    cache_small.upsert_view_gpkg(ns, path, "v1", b"a" * 100)
    (tmp_path / "v1.pkl.gz").write_bytes(b"dummy")
    assert (tmp_path / "v1.gpkg").exists()
    assert (tmp_path / "v1.pkl.gz").exists()

    cache_small.upsert_view_gpkg(ns, path, "v2", b"b" * 100)
    assert not (tmp_path / "v1.gpkg").exists()
    assert not (tmp_path / "v1.pkl.gz").exists()
    assert (tmp_path / "v2.gpkg").exists()
    rows = cache_small._conn.execute("SELECT render_id FROM view").fetchall()
    assert rows == [("v2",)]


def test_get_missing_view_gpkg(cache):
    assert cache.get_view_gpkg("no_ns", "no_path") is None


def test_get_bad_view_gpkg(cache):
    cache._conn.execute(
        "INSERT INTO view (namespace, render_id, path, cached_at, file_size_kb) VALUES (?, ?, ?, ?, ?)",
        ("foo", "bar", "bp", datetime.now().isoformat(), 1024),
    )
    cache._conn.commit()
    assert cache.get_view_gpkg("foo", "bp") is None


def test_get_graph_gpkg_after_file_deletion(tmp_path, cache_small):
    ns, path, rid = "ns_del", "p_del", "rd"
    gpkg = cache_small.upsert_graph_gpkg(ns, path, rid, b"hello")
    assert gpkg.exists()
    os.remove(gpkg)
    assert cache_small.get_graph_gpkg(ns, path) is None


def test_get_view_gpkg_after_file_deletion(tmp_path, cache_small):
    ns, path, rid = "ns_del_v", "p_del_v", "rdv"
    gpkg = cache_small.upsert_view_gpkg(ns, path, rid, b"world")
    assert gpkg.exists()
    os.remove(gpkg)
    assert cache_small.get_view_gpkg(ns, path) is None


def test_commit_executes_sql(cache):
    """Ensure that _commit calls the SQL COMMIT statement."""

    # Prepare a dummy connection that records the last SQL executed
    class DummyConn:
        def __init__(self):
            self.last_sql = None

        def execute(self, sql, *args, **kwargs):
            self.last_sql = sql

    dummy = DummyConn()
    # Inject our dummy connection into the cache
    cache._conn = dummy

    cache._commit()

    assert dummy.last_sql == "COMMIT"
