"""Internal cache operations for CherryDB."""
import sqlite3
from os import PathLike
from typing import TypeVar, Union

from cherrydb.exceptions import CacheInitError
from cherrydb.schemas import BaseModel

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

    def _commit(self) -> bool:
        """Commits the cache transaction."""
        self._conn.execute("COMMIT")

    def _tables(self) -> set[str]:
        """Fetches a list of user-defined tables in the cache database."""
        # see https://www.sqlitetutorial.net/sqlite-show-tables/
        tables = self._conn.execute(
            """SELECT name FROM sqlite_schema
               WHERE type ='table' AND name NOT LIKE 'sqlite_%';"""
        ).fetchall()
        return {table[0] for table in tables}

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
            "SELECT value FROM cache_meta WHERE key='schema_version'"
        ).fetchone()
        if schema_version is None:
            raise CacheInitError("Invalid cache: no schema version in cache metadata.")
        if schema_version[0] != _CACHE_SCHEMA_VERSION:
            raise CacheInitError(
                f"Invalid cache: expected schema version {_CACHE_SCHEMA_VERSION}, "
                f"but got schema version {schema_version[0]}."
            )

    def _init_db(self) -> None:
        """Initializes CherryDB cache tables."""
        self._conn.execute(
            """CREATE TABLE cache_meta(
                key   TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            )"""
        )
        self._conn.execute(
            """CREATE TABLE view(
                namespace        TEXT NOT NULL,
                path             TEXT NOT NULL,
                geopackage_uuid  TEXT NOT NULL,
                cached_at        TEXT NOT NULL,
                UNIQUE(namespace, path)
            )"""
        )
        self._conn.execute(
            "INSERT INTO cache_meta (key, value) VALUES ('schema_version', ?)",
            _CACHE_SCHEMA_VERSION,
        )
        self._conn.commit()
