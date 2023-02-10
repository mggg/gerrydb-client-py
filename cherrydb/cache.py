"""Internal cache operations for CherryDB."""
import sqlite3
import pydantic
import orjson as json
from enum import Enum
from os import PathLike
from typing import Any, Optional, Union, Tuple

_REQUIRED_TABLES = {"cache_meta", "etag", "object", "object_meta"}
_CACHE_SCHEMA_VERSION = "0"


class CacheError(Exception):
    """Raised for generic caching errors."""


class CacheInitError(CacheError):
    """Raised when a CherryDB cache cannot be initialized."""


class CacheObjectError(CacheError):
    """
    Raised when an object type has not been registered with the cache
    or when an object's registered schema does not match the provided schema.
    """


class CachePolicyError(CacheError):
    """Raised when an cache operation does not match an object's cache policy."""


class ObjectCachePolicy(str, Enum):
    """An object type's single-object caching policy."""

    ETAG = "etag"
    TIMESTAMP = "timestamp"
    NONE = "none"


class CherryCache:
    """Pydantic-enabled caching layer for CherryDB.

    ETag versioning (primarily for collections) and timestamp versioning
    are both supported.
    """

    _conn: sqlite3.Connection
    _schemas: dict[str, Tuple[type, ObjectCachePolicy]]

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
        self._schemas = {}

    def register_schema(
        self, obj: str, schema: type, policy: ObjectCachePolicy
    ) -> None:
        """Registers a Pydantic schema with the cache."""
        self._schemas[obj] = (schema, policy)

    def get(
        self,
        obj: str,
        path: str,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        etag: Optional[bytes] = None,
    ) -> Optional[Any]:
        try:
            schema, policy = self._schemas[obj]
        except KeyError:
            raise CacheObjectError(f'Object type "{obj}" not registered with cache.')

        if policy == ObjectCachePolicy.ETAG and (etag is None or version is not None):
            raise CacheObjectError(f'Object type "{obj}" not registered with cache.')

    def all(
        self, obj: str, namespace: Optional[str] = None, etag: Optional[bytes] = None
    ) -> list[Any]:
        pass

    def upsert(
        self,
        obj: str,
        path: str,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        etag: Optional[bytes] = None,
    ) -> Optional[Any]:
        pass

    def _assert_clean(self) -> None:
        """Asserts that the cache's schema matches the current schema version.

        Raises:
            CacheInitError: If the cache is invalid.
        """
        table_diff = _REQUIRED_TABLES - self._tables()
        if table_diff:
            missing_tables = ", ".join(table_diff)
            raise CacheInitError(f"Invalid cache: missing tables {missing_tables}.")

        schema_version = self._conn.execute(
            "SELECT value FROM cache_meta WHERE key='cache_version'"
        ).fetchone()
        if schema_version is None:
            raise CacheInitError("Invalid cache: no schema version in cache metadata.")
        if schema_version[0] != _CACHE_SCHEMA_VERSION:
            raise CacheInitError(
                f"Invalid cache: expected schema version {_CACHE_SCHEMA_VERSION}, "
                f"but got schema version {schema_version}."
            )

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
                type      TEXT,
                path      TEXT NOT NULL, 
                namespace TEXT,
                version   TEXT,
                data      TEXT NOT NULL,
                meta_id   BLOB,
                FOREIGN KEY(meta_id) REFERENCES object_meta(meta_id),
                UNIQUE(type, path, namespace, version)
            )"""
        )
        self._conn.execute(
            "CREATE UNIQUE INDEX idx_object ON object(type, path, namespace)"
        )
        self._conn.execute(
            """CREATE TABLE object(
                type      TEXT,
                path      TEXT NOT NULL, 
                namespace TEXT,
                data      TEXT NOT NULL,
                timestamp TEXT,
                etag      BLOB,
                meta_id   BLOB,
                FOREIGN KEY(meta_id) REFERENCES object_meta(meta_id),
                UNIQUE(type, path, namespace, version)
            )"""
        )
        self._conn.execute(
            "CREATE UNIQUE INDEX idx_object ON object(type, path, namespace)"
        )
        self._conn.execute(
            """CREATE TABLE etag(
                type      TEXT NOT NULL,
                namespace TEXT,
                etag      BLOB
                UNIQUE(type, namespace)
            )"""
        )
        self._conn.execute("CREATE UNIQUE INDEX idx_etag ON etag(type, namespace)")
        self._conn.execute(
            "INSERT INTO cache_meta (key, value) VALUES ('schema_version', ?)",
            _CACHE_SCHEMA_VERSION,
        )
        self._conn.commit()
