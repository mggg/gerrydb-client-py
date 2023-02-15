"""Internal cache operations for CherryDB."""
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from os import PathLike
from typing import Optional, Union
from uuid import UUID

import orjson as json
from dateutil.parser import parse as ts_parse

from cherrydb.schemas import BaseModel, ObjectCachePolicy

_REQUIRED_TABLES = {"cache_meta", "collection", "object", "object_meta"}
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
    """Result of a successful single-object cache retrieval operation.

    Attributes:
        result: The cached object or objects.
        cached_at: Local system time the object(s) were fetched from the API.
        valid_from: Start of version time range for timestamp-versioned objects.
        etag: ETag for ETag-versioned objects.
    """

    result: BaseModel
    cached_at: datetime
    valid_from: Optional[datetime] = None
    etag: Optional[bytes] = None


@dataclass(frozen=True)
class CacheCollectionResult:
    """Result of a successful cache collection retrieval operation.

    Attributes:
        result: The cached objects by path.
        cached_at: Local system time the object(s) were fetched from the API.
        valid_at: collection snapshot time for timestamp-versioned objects.
        etag: collection ETag for ETag-versioned objects.
    """

    result: dict[str, BaseModel]
    cached_at: datetime
    valid_at: Optional[datetime] = None
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

    def commit(self) -> bool:
        """Commits the cache transaction."""
        self._conn.execute("COMMIT")

    def rollback(self) -> bool:
        """Rolls back the cache transaction."""
        self._conn.execute("ROLLBACK")

    def get(
        self,
        obj: type,
        path: str,
        namespace: Optional[str] = None,
        *,
        at: Optional[datetime] = None,
        etag: Optional[bytes] = None,
    ) -> Optional[CacheResult]:
        """Retrieves an object from the cache.

        Args:
            obj: The object's Pydantic schema.
            path: The object's namespace-relative path.
            namespace: The object's namespace.
            at: (For timestamp-versioned objects.)
                An upper bound on the object's `valid_from` tag.
                If not provided, the latest version by `valid_from` is retrieved.
            etag: (For ETag-versioned objects.)
                The object's exact ETag. If not provided, any version is retrieved.

        Returns:
            The cached object wrapped in a `CacheResult`, if available.
        """

        name = cache_name(obj)
        policy = cache_policy(obj)

        params = {
            "type": name,
            "path": path,
            "namespace": namespace,
            "at": at,
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
        elif policy == ObjectCachePolicy.TIMESTAMP and at is not None:
            where_clauses += ["valid_from <= :at"]
        elif policy == ObjectCachePolicy.TIMESTAMP and at is None:
            order_by_col = "valid_from"

        query = f"""
            SELECT object.data, object_meta.data AS metadata,
                   object.cached_at, object.valid_from, object.etag
            FROM object 
            LEFT JOIN object_meta
            ON object.meta_id = object_meta.meta_id
            WHERE {' AND '.join(where_clauses)} 
            ORDER BY {order_by_col} DESC LIMIT 1
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
            valid_from=None if result_row[3] is None else ts_parse(result_row[3]),
            etag=result_row[4],
        )

    def insert(
        self,
        obj: BaseModel,
        path: str,
        namespace: Optional[str] = None,
        *,
        valid_from: Optional[datetime] = None,
        etag: Optional[bytes] = None,
        autocommit: bool = True,
    ) -> None:
        """Caches an object.

        Args:
            obj: A version of the object to cache.
            path: The object's namespace-relative path.
            namespace: The object's namespace.
            valid_from: (For timestamp-versioned objects.)
                The beginning of the object version's lifetime.
            etag: (For ETag-versioned objects.) The object's ETag.
            autocommit: Determines if the transaction with the SQLite backend
                must be explicitly closed with `commit()`. Set this
                to `False` for bulk insertions.

        Raises:
            CachePolicyError: If the `valid_from` and `etag` parameters do
                not match the caching policy of `obj`.
        """
        self._assert_write_policy(obj, valid_from, etag)
        name = cache_name(obj)
        policy = cache_policy(obj)

        obj_data = obj.dict()
        meta_id = None
        if "meta" in obj_data:
            obj_meta = obj_data["meta"]
            meta_id = UUID(obj_meta["uuid"]).bytes
            del obj_data["meta"]
            self._conn.execute(
                "INSERT OR IGNORE INTO object_meta(meta_id, data) VALUES (?, ?)",
                (meta_id, json.dumps(obj_meta).decode("utf-8")),
            )

        if policy == ObjectCachePolicy.ETAG:
            # For ETag-versioned objects, we always prefer the newest version.
            self._conn.execute(
                "DELETE FROM object WHERE type = ? AND path = ? AND namespace = ?",
                (name, path, namespace),
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
            "data": json.dumps(obj_data).decode("utf-8"),
            "meta_id": meta_id,
            "etag": etag,
            "valid_from": None if valid_from is None else valid_from.isoformat(),
            "cached_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self._conn.execute(obj_stmt, obj_params)

        if autocommit:
            self.commit()

    def collect(
        self,
        obj: type,
        namespace: Optional[str] = None,
        *,
        valid_at: Optional[datetime] = None,
        etag: Optional[bytes] = None,
        autocommit: bool = True,
    ) -> None:
        """Inserts a complete namespaced-scoped collection at an ETag or point in time.

        Args:
            obj: The object collection's schema.
            namespace: The object collection's namespace.
            valid_at: (For timestamp-versioned objects.)
                The point in time at which the collection is complete.
            etag: (For ETag-versioned objects.) The collection's ETag.
            autocommit: Determines if the transaction with the SQLite backend
                must be explicitly closed with `commit()`.

        Raises:
            CachePolicyError: If the `valid_at` and `etag` parameters do not match
                the caching policy of `obj`, or if `obj` cannot be cached at the
                collection level.
        """
        name = cache_name(obj)
        policy = cache_policy(obj)

        if policy == ObjectCachePolicy.ETAG and (etag is None or valid_at is not None):
            raise CachePolicyError(f'Object type "{obj}" is ETag-versioned.')
        elif policy == ObjectCachePolicy.TIMESTAMP and (
            etag is None or valid_at is None
        ):
            raise CachePolicyError(
                f'Object type "{obj}" is timestamp-versioned: for collections, '
                "specify a collection ETag and a snapshot timestamp."
            )
        elif policy == ObjectCachePolicy.NONE:
            raise CachePolicyError(
                f'Object type "{obj}" does not support collection-level caching.'
            )

        if policy == ObjectCachePolicy.ETAG:
            # For ETag-versioned objects, we always prefer the newest version.
            self._conn.execute(
                "DELETE FROM collection WHERE type = ? AND namespace = ?",
                (name, namespace),
            )

        valid_at_iso = None if valid_at is None else valid_at.isoformat()
        cached_at_iso = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            "INSERT INTO collection(type, namespace, etag, valid_at, cached_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (name, namespace, etag, valid_at_iso, cached_at_iso),
        )

        if autocommit:
            self.commit()

    def all(
        self,
        obj: type,
        namespace: Optional[str] = None,
        *,
        at: Optional[datetime] = None,
    ) -> Optional[CacheCollectionResult]:
        """Gets the latest versions of all objects of type `obj` in `namespace`.

        Args:
            obj: The object collection's schema.
            namespace: The object collection's namespace.
            at: (For timestamp-versioned objects.) The point in time to retrieve
                the collection at.

        Returns:
            The cached object collection wrapped in a `CacheCollectionResult`,
            if available.

        Raises:
            CachePolicyError: If the `at` parameter does not match the caching policy
                of `obj`, or if `obj` cannot be cached at the collection level.
        """
        name = cache_name(obj)
        policy = cache_policy(obj)
        if policy == ObjectCachePolicy.ETAG and at is not None:
            raise CachePolicyError(f'Object type "{obj}" is ETag-versioned.')
        if policy == ObjectCachePolicy.NONE:
            raise CachePolicyError(
                f'Object type "{obj}" does not support collection-level caching.'
            )

        if policy == ObjectCachePolicy.ETAG:
            collection_meta = self._conn.execute(
                "SELECT etag, cached_at, valid_at FROM collection "
                "WHERE type = ? AND namespace = ?",
                (name, namespace),
            ).fetchone()
            if collection_meta is None:
                return None
            etag, cached_at, valid_at = collection_meta

            members_query = """
            SELECT object.path, object.data, object_meta.data AS metadata
            FROM object 
            LEFT JOIN object_meta
            ON object.meta_id = object_meta.meta_id
            WHERE object.type = ? AND object.namespace = ?
            """
            collection_raw = self._conn.execute(
                members_query, (name, namespace)
            ).fetchall()
        else:
            base_collection_query = """
            SELECT etag, cached_at, valid_at FROM collection
            WHERE type = ? AND namespace = ?
            """

            if at is None:
                # If `at` is unspecified, get the latest snapshot available.
                collection_meta_before = self._conn.execute(
                    base_collection_query + " ORDER BY valid_at DESC LIMIT 1",
                    (name, namespace),
                ).fetchone()
                if collection_meta_before is None:
                    return None
            else:
                # Attempt inclusive timestamp-based matching on `at`.
                collection_meta_before = self._conn.execute(
                    base_collection_query
                    + " AND valid_at <= ?"
                    + " ORDER BY valid_at DESC LIMIT 1",
                    (name, namespace, at.isoformat()),
                ).fetchone()
                collection_meta_after = self._conn.execute(
                    base_collection_query
                    + " AND valid_at >= ?"
                    + " ORDER BY valid_at ASC LIMIT 1",
                    (name, namespace, at.isoformat()),
                ).fetchone()

                # We look for either a collection snapshot exactly at `at`
                # or two snapshots sandwiching `at` with the same ETag.
                exact_match = (
                    collection_meta_before is not None
                    and collection_meta_before[2] == at.isoformat()
                )
                sandwich_match = (
                    collection_meta_before is not None
                    and collection_meta_after is not None
                    and collection_meta_before[0] == collection_meta_after[0]
                )
                if not (exact_match or sandwich_match):
                    return None

            etag, cached_at, valid_at = collection_meta_before
            members_query = """
            SELECT object.path, object.data, object_meta.data AS metadata
            FROM object 
            LEFT JOIN object_meta
            ON object.meta_id = object_meta.meta_id
            WHERE object.type = ? AND object.namespace = ? AND object.valid_from <= ?
            GROUP BY object.path
            HAVING MAX(object.valid_from)
            """
            collection_raw = self._conn.execute(
                members_query, (name, namespace, valid_at)
            ).fetchall()

        collection = {}
        for row in collection_raw:
            path = row[0]
            data = json.loads(row[1])
            if row[2] is not None:
                data["meta"] = json.loads(row[2])
            collection[path] = obj(**data)

        return CacheCollectionResult(
            result=collection,
            cached_at=ts_parse(cached_at),
            valid_at=None if valid_at is None else ts_parse(valid_at),
            etag=etag,
        )

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

    def _assert_write_policy(
        self, obj: type, valid_from: Optional[datetime], etag: Optional[bytes]
    ) -> None:
        """Checks that an object type is registered and properly referenced on write.

        Raises:
            CachePolicyError:
                If `obj` is improperly referenced.
                An object is properly referenced when
                    * Only an ETag is provided for an ETag-versioned object type.
                    * Only a timestamp is provided for an timestamp-versioned object type.
                    * No version information is provided for an unversioned object type.
        """
        policy = cache_policy(obj)

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
                cached_at  TEXT NOT NULL,
                FOREIGN KEY(meta_id) REFERENCES object_meta(meta_id),
                UNIQUE(type, path, namespace, etag),
                UNIQUE(type, path, namespace, valid_from)
            )"""
        )
        self._conn.execute(
            """CREATE TABLE collection(
                type      TEXT NOT NULL,
                namespace TEXT,
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
        self._conn.commit()
