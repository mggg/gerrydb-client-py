"""Internal cache operations for GerryDB."""
import gzip
import pickle
import sqlite3
from datetime import datetime
from os import PathLike
from pathlib import Path
from typing import Optional, TypeVar, Union

from gerrydb.schemas import BaseModel, ViewMeta

from .exceptions import CacheInitError

_REQUIRED_TABLES = {"cache_meta", "graph", "view"}
_CACHE_SCHEMA_VERSION = "0"
CACHE_EXTENSIONS = (
    "gpkg",  # view archive
    "pkl.gz",  # graph (derived from view archive)
)

SchemaType = TypeVar("SchemaType", bound=BaseModel)


class GerryCache:
    """Caching layer for GerryDB."""

    _conn: sqlite3.Connection
    data_dir: Path

    def __init__(
        self, database: Union[str, PathLike, sqlite3.Connection], data_dir: Path
    ):
        """Loads or initializes a cache."""
        if isinstance(database, sqlite3.Connection):
            self._conn = database
        else:
            try:
                self._conn = sqlite3.connect(database)
            except sqlite3.OperationalError as ex:
                raise CacheInitError(
                    "Failed to load to initialize GerryDB cache ({database})."
                ) from ex

        if not self._tables():
            self._init_db()
        else:
            self._assert_clean()

        self.data_dir = data_dir

    def upsert_view_gpkg(
        self, namespace: str, path: str, render_id: str, content: bytes
    ) -> Path:
        """Upserts a view's GeoPackage into the cache.

        Returns:
            Path of the cached GeoPackage.
        """
        gpkg_path = self.data_dir / f"{render_id}.gpkg"
        with open(gpkg_path, "wb") as gpkg_fp:
            gpkg_fp.write(content)

        with self._conn:
            # Register the new render.
            prev_render_id = self._conn.execute(
                "SELECT render_id FROM view WHERE namespace = ? AND path = ?",
                (namespace, path),
            ).fetchone()
            if prev_render_id is not None:
                self._conn.execute(
                    "DELETE FROM view WHERE namespace = ? AND path = ?",
                    (namespace, path),
                )
                for ext in CACHE_EXTENSIONS:
                    Path(self.data_dir / f"{prev_render_id[0]}.{ext}").unlink(
                        missing_ok=True
                    )

            self._conn.execute(
                (
                    "INSERT INTO view (namespace, path, render_id, cached_at) "
                    "VALUES (?, ?, ?, ?)"
                ),
                (namespace, path, render_id, datetime.now().isoformat()),
            )

        return gpkg_path

    def get_view_gpkg(self, namespace: str, path: str) -> Optional[Path]:
        """Returns the path to a view's cached GeoPackage, if available."""
        render_id = self._conn.execute(
            "SELECT render_id FROM view WHERE namespace = ? AND path = ?",
            (namespace, path),
        ).fetchone()
        if render_id is None:
            return None

        gpkg_path = self.data_dir / f"{render_id[0]}.gpkg"
        if not gpkg_path.is_file():
            # TODO: this implies a corrupt cache index.
            # What's the right way to handle that?
            return None
        return gpkg_path

    def _commit(self) -> bool:
        """Commits the cache transaction."""
        self._conn.execute("COMMIT")

    def _tables(self) -> set[str]:
        """Fetches a list of user-defined tables in the cache database."""
        # see https://www.sqlitetutorial.net/sqlite-show-tables/
        try:
            tables = self._conn.execute(
                """SELECT name FROM sqlite_schema
                WHERE type ='table' AND name NOT LIKE 'sqlite_%';"""
            ).fetchall()
            return {table[0] for table in tables}
        except sqlite3.OperationalError:
            # The `sqlite_schema` table may not exist yet.
            return set()

    def _assert_clean(self) -> None:
        """Asserts that the cache's schema matches the current schema version.
        Raises:
            CacheInitError: If the cache is invalid.
        """
        table_diff = _REQUIRED_TABLES - self._tables()
        if table_diff:
            missing_tables = ", ".join(table_diff)
            raise CacheInitError(f"Invalid cache: missing table(s) {missing_tables}.")

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
        """Initializes GerryDB cache tables."""
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
                render_id        TEXT NOT NULL,
                cached_at        TEXT NOT NULL,
                UNIQUE(namespace, path)
            )"""
        )
        self._conn.execute(
            """CREATE TABLE graph(
                render_id   TEXT    NOT NULL REFERENCES view(render_id),
                plans       INTEGER NOT NULL, 
                geometry    INTEGER NOT NULL, 
                cached_at   TEXT    NOT NULL,
                UNIQUE(render_id, plans, geometry)
            )"""
        )
        self._conn.execute(
            "INSERT INTO cache_meta (key, value) VALUES ('schema_version', ?)",
            _CACHE_SCHEMA_VERSION,
        )
        self._conn.commit()
