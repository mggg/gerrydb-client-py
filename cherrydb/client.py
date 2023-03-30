"""CherryDB session management."""
import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

import geopandas as gpd
import httpx
import pandas as pd
import tomlkit
from shapely import Point
from shapely.geometry.base import BaseGeometry

from cherrydb.cache import CherryCache
from cherrydb.exceptions import ConfigError
from cherrydb.repos import (
    ColumnRepo,
    ColumnSetRepo,
    GeographyRepo,
    GeoLayerRepo,
    GraphRepo,
    LocalityRepo,
    NamespaceRepo,
    PlanRepo,
    ViewRepo,
    ViewTemplateRepo,
)
from cherrydb.repos.geography import GeoValType
from cherrydb.schemas import (
    Column,
    ColumnSet,
    Geography,
    GeoImport,
    GeoLayer,
    Graph,
    Locality,
    ObjectMeta,
    ObjectMetaCreate,
    Plan,
    View,
    ViewTemplate,
)

DEFAULT_CHERRY_ROOT = Path(os.path.expanduser("~")) / ".cherry"


class CherryDB:
    """CherryDB session."""

    client: Optional[httpx.Client]
    cache: CherryCache
    namespace: Optional[str]
    offline: bool
    _base_url: str
    timeout: int

    def __init__(
        self,
        profile: Optional[str] = None,
        host: Optional[str] = None,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        offline: bool = False,
        timeout: int = 180,
    ):
        """Creates a CherryDB session.

        If `host` and `key` are specified, an ephemeral session is created
        with an in-memory cache. Otherwise, session configuration is loaded
        for `profile` from the configuration in the directory specified by
        the `CHERRYDB_ROOT` environment variable. If this variable is not
        available, `~/.cherry` is used.

        If `namespace` is specified, object references without a namespace
        will implicitly refer to `namespace`.

        If `offline` is `True`, cached results from the API are accessible
        in a limited read-only mode; CherryDB will not attempt to fetch
        the latest versions of versioned objects. This mode is suitable
        for isolated use cases where a CherryDB server is not necessarily
        accessible--for instance, within code in a replication repository
        for a scientific paper.

        Raises:
            ConfigError:
                If the configuration is invalid--for instance, if only
                one of `host` and `key` are specified, or a CherryDB
                directory cannot be found.
        """
        self.namespace = namespace
        self.offline = offline
        self.timeout = timeout

        if profile is None:
            profile = os.getenv("CHERRY_PROFILE", "default")
        if host is not None and key is None:
            raise ConfigError(f'No API key specified for host "{host}".')
        if host is None and key is not None:
            raise ConfigError("No host specified for API key.")

        if host is not None and key is not None:
            self.cache = CherryCache(":memory:")
        else:
            cherry_root = Path(os.getenv("CHERRY_ROOT", DEFAULT_CHERRY_ROOT))
            try:
                with open(cherry_root / "config", encoding="utf-8") as config_fp:
                    cherry_config_raw = config_fp.read()
            except IOError as ex:
                raise ConfigError(
                    "Failed to read CherryDB configuration at "
                    f"{cherry_root.resolve()}. "
                    "Does a CherryDB configuration directory exist?"
                ) from ex

            try:
                configs = tomlkit.parse(cherry_config_raw)
            except tomlkit.exceptions.TOMLKitError as ex:
                raise ConfigError(
                    "Failed to parse CherryDB configuration at "
                    f"{cherry_root.resolve()}."
                ) from ex

            try:
                config = configs[profile]
            except KeyError:
                raise ConfigError(
                    f'Profile "{profile}" not found in configuration '
                    f"at {cherry_root.resolve()}."
                )

            for field in ("host", "key"):
                if field not in config:
                    raise ConfigError(
                        f'Field "{field}" not in profile "{profile}" '
                        f"in configuration at {cherry_root.resolve()}."
                    )

            try:
                Path(cherry_root / "caches").mkdir(exist_ok=True)
            except IOError as ex:
                raise ConfigError("Failed to create cache directory.") from ex
            self.cache = CherryCache(cherry_root / "caches" / f"{profile}.db")

            host = config["host"]
            key = config["key"]

        self._base_url = (
            f"http://{host}/api/v1"
            if host.startswith("localhost")
            else f"https://{host}/api/v1"
        )
        self._base_headers = {"User-Agent": "cherrydb-client-py", "X-API-Key": key}
        self._transport = httpx.HTTPTransport(retries=1)

        self.client = httpx.Client(
            base_url=self._base_url,
            headers=self._base_headers,
            timeout=timeout,
            transport=self._transport,
        )

    def context(self, notes: str = "") -> "WriteContext":
        """Creates a write context with session-level metadata.

        Args:
            notes: Freeform notes to associate with the write session.

        Returns:
            A context manager for CherryDB writes.
        """
        return WriteContext(db=self, notes=notes)

    @property
    def columns(self) -> ColumnRepo:
        """Tabular column metadata."""
        return ColumnRepo(schema=Column, base_url="/columns", session=self)

    @property
    def column_sets(self) -> ColumnSetRepo:
        """Column sets."""
        return ColumnSetRepo(schema=ColumnSet, base_url="/column-sets", session=self)

    @property
    def geo(self) -> GeoLayerRepo:
        """Geographies."""
        return GeographyRepo(schema=Geography, base_url="/geographies", session=self)

    @property
    def geo_layers(self) -> GeoLayerRepo:
        """Geographic layers."""
        return GeoLayerRepo(schema=GeoLayer, base_url="/layers", session=self)

    @property
    def graphs(self) -> GraphRepo:
        """Dual graphs."""
        return GraphRepo(schema=Graph, base_url="/graphs", session=self)

    @property
    def localities(self) -> LocalityRepo:
        """Localities."""
        return LocalityRepo(session=self)

    @property
    def namespaces(self) -> NamespaceRepo:
        """Namespaces."""
        return NamespaceRepo(session=self)

    @property
    def plans(self) -> PlanRepo:
        """Districting plans."""
        return PlanRepo(schema=Plan, base_url="/plans", session=self)

    @property
    def views(self) -> ViewRepo:
        """Views."""
        return ViewRepo(schema=View, base_url="/views", session=self)

    @property
    def view_templates(self) -> ViewTemplateRepo:
        """View templates."""
        return ViewTemplateRepo(
            schema=ViewTemplate, base_url="/view-templates", session=self
        )


@dataclass
class WriteContext:
    """Context for a CherryDB transaction."""

    db: CherryDB
    notes: str
    meta: Optional[ObjectMeta] = None
    client: Optional[httpx.Client] = None
    client_params: Optional[dict[str, Any]] = None
    geo_import: Optional[GeoImport] = None

    def __enter__(self) -> "WriteContext":
        """Creates a write context with metadata."""
        response = self.db.client.post(
            "/meta/", json=ObjectMetaCreate(notes=self.notes).dict()
        )
        response.raise_for_status()  # TODO: refine?

        self.meta = ObjectMeta(**response.json())
        self.client_params = {
            "base_url": self.db._base_url,
            "headers": {
                **self.db._base_headers,
                "X-Cherry-Meta-ID": str(self.meta.uuid),
            },
            "timeout": self.db.timeout,
            "transport": self.db._transport,
        }
        self.client = httpx.Client(**self.client_params)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()

    @property
    def columns(self) -> ColumnRepo:
        """Tabular column metadata."""
        return ColumnRepo(schema=Column, base_url="/columns", session=self.db, ctx=self)

    @property
    def column_sets(self) -> ColumnSetRepo:
        """Column sets."""
        return ColumnSetRepo(
            schema=ColumnSet, base_url="/column-sets", session=self.db, ctx=self
        )

    @property
    def geo(self) -> GeoLayerRepo:
        """Geographies."""
        return GeographyRepo(
            schema=Geography, base_url="/geographies", session=self.db, ctx=self
        )

    @property
    def geo_layers(self) -> GeoLayerRepo:
        """Geographic layers."""
        return GeoLayerRepo(
            schema=GeoLayer, base_url="/layers", session=self.db, ctx=self
        )

    @property
    def graphs(self) -> GraphRepo:
        """Dual graphs."""
        return GraphRepo(schema=Graph, base_url="/graphs", session=self.db, ctx=self)

    @property
    def localities(self) -> LocalityRepo:
        """Localities."""
        return LocalityRepo(session=self.db, ctx=self)

    @property
    def namespaces(self) -> NamespaceRepo:
        """Namespaces."""
        return NamespaceRepo(session=self.db, ctx=self)

    @property
    def plans(self) -> PlanRepo:
        """Districting plans."""
        return PlanRepo(schema=Plan, base_url="/plans", session=self.db, ctx=self)

    @property
    def views(self) -> ViewRepo:
        """Views."""
        return ViewRepo(schema=View, base_url="/views", session=self.db, ctx=self)

    @property
    def view_templates(self) -> ViewTemplateRepo:
        """View templates."""
        return ViewTemplateRepo(
            schema=ViewTemplate, base_url="/view-templates", session=self.db, ctx=self
        )

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        columns: dict[str, Column],
        *,
        create_geo: bool = False,
        namespace: Optional[str] = None,
        locality: Optional[Union[str, Locality]] = None,
        layer: Optional[Union[str, GeoLayer]] = None,
        batch_size: int = 5000,
        max_conns: int = 1,
    ) -> None:
        """Imports a DataFrame to CherryDB.

        Plain DataFrames do not include rich column metadata, so the columns used
        in the DataFrame must be defined before import.

        Given column metadata, this function imports column values. It also optionally
        creates geographies.

            * If `create_geo` is `True` and a `geometry` column is present in `df`,
                geographies are imported in addition to tabular data.
            * If `create_geo` is `True` and a `geometry` column is not present in `df`,
                empty geographies are created in addition to tabular data.
            * If `create_geo` is `False`, it is assumed that the rows in the DataFrame
            correspond to geographies that already exist in CherryDB.

        In all cases, the index of `df` is used as the key for geographies within
        `namespace`. Typically, the `GEOID` column or equivalent should be used as
        the index.

        If `layer` and `locality` are provided, a `GeoSet` is created from the
        rows in `df`.

        Args:
            db: CherryDB client instance.
            df: DataFrame to import column values and geographies from.
            columns: Mapping between column names in `df` and CherryDB column metadata.
                Only columns included in the mapping will be imported.
            create_geo: Determines whether to create geographies from the DataFrame.
            namespace: Namespace to load geographies into.
            locality: `Locality` to associate a new `GeoSet` with.
            layer: `GeoLayer` to associate a new `GeoSet` with.
            batch_size: Number of rows to import per API request batch.
            max_conns: Maximum number of simultaneous API connections.
        """
        namespace = self.db.namespace if namespace is None else namespace
        if namespace is None:
            raise ValueError("No namespace available.")

        if create_geo:
            if "geometry" in df.columns:
                df = df.to_crs("epsg:4269")  # import as lat/long
                geos = dict(df.geometry)
            else:
                geos = {key: None for key in df.index}

            # Augment geographies with internal points if available.
            if "internal_point" in df.columns:
                internal_points = dict(df.internal_point)
                geos = {
                    path: (geo, internal_points[path]) for path, geo in geos.items()
                }

            asyncio.run(_load_geos(self.geo, geos, namespace, batch_size, max_conns))

        asyncio.run(
            _load_column_values(self.columns, df, columns, batch_size, max_conns)
        )

        if create_geo and locality is not None and layer is not None:
            self.geo_layers.map_locality(
                layer=layer,
                locality=locality,
                geographies=[f"/{namespace}/{key}" for key in df.index],
            )


# based on https://stackoverflow.com/a/61478547
async def gather_batch(coros, n):
    """Limits concurrency of a batch of coroutines."""
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros))


async def _load_geos(
    repo: GeographyRepo,
    geos: dict[str, GeoValType],
    namespace: str,
    batch_size: int,
    max_conns: Optional[int],
) -> list[Geography]:
    """Asynchronously loads geographies in batches."""
    geo_pairs = list(geos.items())
    tasks = []
    async with repo.async_bulk(namespace, max_conns) as ctx:
        for idx in range(0, len(geo_pairs), batch_size):
            chunk = dict(geo_pairs[idx : idx + batch_size])
            tasks.append(ctx.create(chunk))
        results = await gather_batch(tasks, max_conns)

    # TODO: more sophisticated error handling -- which batches were successful?
    # what can be retried? etc.
    for result in results:
        if isinstance(result, Exception):
            raise result


async def _load_column_values(
    repo: ColumnRepo,
    df: pd.DataFrame,
    columns: dict[str, Column],
    batch_size: int,
    max_conns: Optional[int],
) -> None:
    """Asynchronously loads column values from a DataFrame in batches."""
    params = repo.ctx.client_params.copy()
    params["transport"] = httpx.AsyncHTTPTransport(retries=1)

    val_batches: list[tuple[Column, dict[str, Any]]] = []
    for col_name, col_meta in columns.items():
        col_vals = list(dict(df[col_name]).items())
        for idx in range(0, len(df), batch_size):
            val_batches.append((col_meta, dict(col_vals[idx : idx + batch_size])))

    async with httpx.AsyncClient(**params) as client:
        tasks = [
            repo.async_set_values(col, col.namespace, values=batch, client=client)
            for col, batch in val_batches
        ]
        results = await gather_batch(tasks, max_conns)

    # TODO: more sophisticated error handling -- which batches were successful?
    # what can be retried? etc.
    for result in results:
        if isinstance(result, Exception):
            raise result
