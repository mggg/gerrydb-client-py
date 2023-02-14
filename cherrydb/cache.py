"""Internal cache operations for CherryDB."""
import sqlite3
import orjson as json
from dataclasses import dataclass
from os import PathLike
from datetime import datetime, timezone
from typing import Any, Optional, Union
from uuid import UUID

from dateutil.parser import parse as ts_parse
from cherrydb.schemas import BaseModel, ObjectCachePolicy

_REQUIRED_TABLES = {"cache_meta", "etag", "object", "object_meta"}
_CACHE_SCHEMA_VERSION = "0"


class CacheError(Exception):
    """Raised for generic caching errors."""


class CacheInitError(CacheError):
    """Raised when a CherryDB cache cannot be initialized."""


class CacheObjectError(CacheError):
    """Raised when a schema has not been registered with the cache."""


class CachePolicyError(CacheError):
    """Raised when an cache operation does not match an object's cache policy."""


def cache_name(obj: BaseModel | type) -> str:
    """Gets a schema's cached name.

    Raises:
        CacheObjectError: If the schema does not have a `__cache_name__` attribute.
    """
    try:
        return getattr(obj, "__cache_name__")
    except AttributeError:
        raise CacheObjectError("Schema does not have a __cache_name__.")


def cache_policy(obj: BaseModel | type) -> ObjectCachePolicy:
    """Gets a schema's cache policy.

    Raises:
        CacheObjectError: If the schema does not have a `__cache_policy__` attribute.
    """
    try:
        return getattr(obj, "__cache_policy__")
    except AttributeError:
        raise CacheObjectError("Schema does not have a __cache_policy__.")


@dataclass(frozen=True)
class CacheResult:
    """Result of a successful cache retrieval operation.

    Attributes:
        result: The cached object or objects.
        cached_at: Local system time the object(s) were fetched from the API.
        stale: Is there any risk that the object is stale wrt the API?
        valid_from: Start of version time range for timestamp-versioned objects.
        etag: ETag for collection results or ETag-versioned objects.
    """

    result: BaseModel | list[BaseModel]
    cached_at: datetime
    stale: bool
    valid_from: Optional[datetime] = None
    etag: Optional[bytes] = None


class CherryCache:
    """Pydantic-enabled caching layer for CherryDB.

    ETag versioning (primarily for collections) and timestamp versioning
    are both supported.
    """

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

    def get(
        self,
        obj: type,
        path: str,
        namespace: Optional[str] = None,
        *,
        valid_from: Optional[datetime] = None,
        etag: Optional[bytes] = None,
    ) -> Optional[CacheResult]:
        name = cache_name(obj)
        policy = cache_policy(obj)

        params = {
            "type": name,
            "path": path,
            "namespace": namespace,
            "valid_from": valid_from,
            "etag": etag,
        }
        where_clauses = [
            "type=:type",
            "path=:path",
            "namespace IS NULL" if namespace is None else "namespace=:namespace",
        ]
        order_by_col = "cached_at"
        if policy == ObjectCachePolicy.ETAG and etag is not None:
            where_clauses.append("etag=:etag")
        elif policy == ObjectCachePolicy.TIMESTAMP and valid_from is not None:
            where_clauses += ["valid_from <= :valid_from"]
        elif policy == ObjectCachePolicy.TIMESTAMP and valid_from is None:
            order_by_col = "valid_from"

        query = f""""
            SELECT object.data, meta.data AS metadata,
                   object.cached_at, object.valid_from, object.etag
            FROM object 
            WHERE {' AND '.join(where_clauses)} ORDER BY {order_by_col} LIMIT 1
            LEFT JOIN meta
            ON object.meta_id = meta.meta_id
        """
        cur = self._conn.execute(query, params)
        result_row = cur.fetchone()
        if result_row is None:
            return None

        result_data = json.loads(result_row[0])
        if result_row[1] is not None:
            result_data["meta"] = json.loads(result_row[1])

        return CacheResult(
            result=obj(**result_data),
            cached_at=ts_parse(result_row[2]),
            stale=True,  # TODO
            valid_from=None if result_row[3] is None else ts_parse(result_row[3]),
            etag=result_row[4],
        )

    def insert(
        self,
        obj: BaseModel | list[BaseModel],
        path: str,
        namespace: Optional[str] = None,
        *,
        valid_from: Optional[datetime] = None,
        etag: Optional[bytes] = None,
        autocommit: bool = True,
    ) -> Optional[Any]:
        self._assert_write_policy(obj, valid_from, etag)
        name = cache_name(obj)
        policy = cache_policy(obj)

        obj_data = json.dumps(obj.dict())
        meta_id = None
        if "meta" in obj_data:
            obj_meta = obj_data["meta"]
            meta_id = UUID(obj_meta["uuid"]).bytes
            del obj_data["meta"]
            self._conn.execute(
                "INSERT INTO object_meta(meta_id, data) VALUES (?, ?) "
                "ON CONFLICT IGNORE",
                meta_id,
                json.dumps(obj_meta),
            )

        obj_stmt = """
        INSERT INTO object(
            type, path, namespace, data, meta_id, etag, valid_from, cached_at
        )
        VALUES(
            :type, :path, :namespace, :data, :meta_id,
            :etag, :valid_from, :cached_at
        )
        """
        obj_params = {
            "type": name,
            "path": path,
            "namespace": namespace,
            "data": json.dumps(obj_data),
            "meta_id": meta_id,
            "etag": etag,
            "valid_from": valid_from.isoformat(),
            "cached_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self._conn.execute(obj_stmt, obj_params)

        if autocommit:
            self.commit()

    def commit(self) -> bool:
        """Flushes the cache state."""
        self._conn.commit()

    def all(
        self, obj: type, namespace: Optional[str] = None, etag: Optional[bytes] = None
    ) -> CacheResult:
        """Gets the latest versions of all objects of type `obj` in `namespace`."""
        name = cache_name(obj)
        policy = cache_policy(obj)

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

    def _assert_write_policy(
        self, obj: str, valid_from: Optional[datetime], etag: Optional[bytes]
    ) -> None:
        """Checks that an object type is registered and properly referenced on write.

        Raises:
            CacheObjectError: If `obj` is unknown.
            CachePolicyError:
                If `obj` is improperly referenced.
                An object is properly referenced when
                    * Only an ETag is provided for an ETag-versioned object type.
                    * Only a timestamp is provided for an timestamp-versioned object type.
                    * No version information is provided for an unversioned object type.
        """
        try:
            _, policy = self._schemas[obj]
        except KeyError:
            raise CacheObjectError(f'Object type "{obj}" not registered with cache.')

        if policy == ObjectCachePolicy.ETAG and (
            etag is None or valid_from is not None
        ):
            raise CachePolicyError(f'Object type "{obj}" is ETag-versioned.')

        if policy == ObjectCachePolicy.TIMESTAMP and (
            etag is not None or valid_from is None
        ):
            raise CachePolicyError(f'Object type "{obj}" is timestamp-versioned.')

        if policy == ObjectCachePolicy.NONE and (
            etag is not None or valid_from is not None
        ):
            raise CachePolicyError(f'Object type "{obj}" is not versioned.')

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
                type       TEXT,
                path       TEXT NOT NULL, 
                namespace  TEXT,
                data       TEXT NOT NULL,
                meta_id    BLOB,
                etag       BLOB,
                valid_from TEXT,
                cached_at TEXT NOT NULL,
                FOREIGN KEY(meta_id) REFERENCES object_meta(meta_id),
                UNIQUE(type, path, namespace, version)
            )"""
        )
        self._conn.execute(
            "CREATE UNIQUE INDEX idx_object ON object(type, path, namespace)"
        )
        self._conn.execute(
            """CREATE TABLE collection(
                type      TEXT NOT NULL,
                namespace TEXT,
                etag      BLOB
                UNIQUE(type, namespace)
            )"""
        )
        self._conn.execute(
            "CREATE UNIQUE INDEX idx_collection ON collection(type, namespace)"
        )
        self._conn.execute(
            "INSERT INTO cache_meta (key, value) VALUES ('schema_version', ?)",
            _CACHE_SCHEMA_VERSION,
        )
        self._conn.commit()
