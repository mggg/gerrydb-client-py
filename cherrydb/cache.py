"""Internal cache operations for CherryDB."""
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from os import PathLike
from typing import Generic, Optional, TypeVar, Union
from uuid import UUID

import orjson as json
import shapely.wkb
from dateutil.parser import parse as ts_parse

from cherrydb.exceptions import CacheInitError, CacheObjectError, CachePolicyError
from cherrydb.schemas import BaseModel, Geography, ObjectCachePolicy

_REQUIRED_TABLES = {"cache_meta", "view"}
_CACHE_SCHEMA_VERSION = "0"

SchemaType = TypeVar("SchemaType", bound=BaseModel)


class CherryCache:
    """Caching layer for CherryDB."""

    _conn: sqlite3.Connection

    def __init__(self, database: Union[str, PathLike, sqlite3.Connection]):
        """Loads or initializes a cache."""
        if isinstance(database, sqlite3.Connection):
            self._conn = database
        else:
            try:
                self._conn = sqlite3.connect(database)
            except sqlite3.OperationalError as ex:
                raise CacheInitError(
                    "Failed to load to initialize CherryDB cache ({database})."
                ) from ex

        if not self._tables():
            self._init_db()
        else:
            self._assert_clean()

    def _tables(self) -> set[str]:
        """Fetches a list of user-defined tables in the cache database."""
        # see https://www.sqlitetutorial.net/sqlite-show-tables/
        tables = self._conn.execute(
            """SELECT name FROM sqlite_schema
               WHERE type ='table' AND name NOT LIKE 'sqlite_%';"""
        ).fetchall()
        return {table[0] for table in tables}

    def _init_db(self) -> None:
        """Initializes CherryDB cache tables."""
        # Use a big cache (128 MB) and WAL mode.
        # Performance tips: https://news.ycombinator.com/item?id=26108042
        self._conn.execute("PRAGMA cache_size = -128000")
        self._conn.execute("PRAGMA temp_store = 2")
        self._conn.execute("PRAGMA journal_mode = 'WAL'")
        self._conn.execute("PRAGMA synchronous = 1")

        self._conn.execute(
            """CREATE TABLE cache_meta(
                key   TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            )"""
        )
        self._conn.execute(
            """CREATE TABLE object_meta(
                meta_id BLOB PRIMARY KEY,
                data    TEXT NOT NULL
            )"""
        )
        self._conn.execute(
            """CREATE TABLE object(
                type       TEXT NOT NULL,
                path       TEXT NOT NULL, 
                namespace  TEXT NOT NULL,
                data       TEXT NOT NULL,
                meta_id    BLOB,
                etag       BLOB,
                valid_from TEXT,
                cached_at  TEXT NOT NULL,
                FOREIGN KEY(meta_id) REFERENCES object_meta(meta_id),
                UNIQUE(type, path, namespace, etag),
                UNIQUE(type, path, namespace, valid_from)
            )"""
        )
        self._conn.execute(
            """CREATE TABLE object_alias(
                type            TEXT NOT NULL,
                namespace       TEXT NOT NULL,
                canonical_path  TEXT NOT NULL, 
                alias_path      TEXT NOT NULL, 
                UNIQUE(type, namespace, canonical_path, alias_path),
                PRIMARY KEY(type, namespace, alias_path)
            )"""
        )
        self._conn.execute(
            """CREATE TABLE collection(
                type      TEXT NOT NULL,
                namespace TEXT NOT NULL,
                etag      BLOB,
                valid_at  TEXT,
                cached_at TEXT NOT NULL,
                UNIQUE(type, namespace, etag, valid_at)
            )"""
        )
        self._conn.execute(
            "INSERT INTO cache_meta (key, value) VALUES ('schema_version', ?)",
            _CACHE_SCHEMA_VERSION,
        )
        for ext in self.extensions.values():
            ext(self._conn).init_db()
        self._conn.commit()
