"""GerryDB session management."""

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional, Union

import geopandas as gpd
import httpx
import pandas as pd
from pandas.core.indexes.base import Index as pdIndex
import tomlkit
from rapidfuzz import process, fuzz
from geoalchemy2.elements import WKBElement
from shapely.geometry import Polygon
import hashlib

from gerrydb.cache import GerryCache
from gerrydb.exceptions import ConfigError
from gerrydb.repos import (
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
from gerrydb.repos.base import normalize_path
from gerrydb.repos.geography import GeoValType
from gerrydb.schemas import (
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
    ViewMeta,
    ViewTemplate,
)
from gerrydb.logging import log

DEFAULT_GERRYDB_ROOT = Path(os.path.expanduser("~")) / ".gerrydb"


class GerryDB:
    """GerryDB session."""

    client: Optional[httpx.Client]
    cache: GerryCache
    namespace: Optional[str]
    offline: bool
    timeout: int

    _base_url: str
    _temp_dir: Optional[TemporaryDirectory]

    def __init__(
        self,
        profile: Optional[str] = None,
        host: Optional[str] = None,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        offline: bool = False,
        timeout: int = 600,
        cache_max_size_gb: float = 10,
    ):
        """Creates a GerryDB session.

        If `host` and `key` are specified, an ephemeral session is created
        with an in-memory cache. Otherwise, session configuration is loaded
        for `profile` from the configuration in the directory specified by
        the `GERRYDB_ROOT` environment variable. If this variable is not
        available, `~/.gerrydb` is used.

        If `namespace` is specified, object references without a namespace
        will implicitly refer to `namespace`.

        If `offline` is `True`, cached results from the API are accessible
        in a limited read-only mode; GerryDB will not attempt to fetch
        the latest versions of versioned objects. This mode is suitable
        for isolated use cases where a GerryDB server is not necessarily
        accessible--for instance, within code in a replication repository
        for a scientific paper.

        Raises:
            ConfigError:
                If the configuration is invalid--for instance, if only
                one of `host` and `key` are specified, or a GerryDB
                directory cannot be found.
        """
        self.namespace = namespace
        self.offline = offline
        self.timeout = timeout

        if profile is None:
            profile = os.getenv("GERRYDB_PROFILE", "default")
        if host is not None and key is None:
            raise ConfigError(f'No API key specified for host "{host}".')
        if host is None and key is not None:
            raise ConfigError("No host specified for API key.")

        if host is not None and key is not None:
            self._temp_dir = TemporaryDirectory()
            self.cache = GerryCache(
                ":memory:", Path(self._temp_dir.name), max_size_gb=cache_max_size_gb
            )
        else:
            GERRYDB_ROOT = Path(os.getenv("GERRYDB_ROOT", DEFAULT_GERRYDB_ROOT))
            try:
                with open(GERRYDB_ROOT / "config", encoding="utf-8") as config_fp:
                    config_raw = config_fp.read()
            except IOError as ex:
                raise ConfigError(
                    "Failed to read GerryDB configuration at "
                    f"{GERRYDB_ROOT.resolve()}. "
                    "Does a GerryDB configuration directory exist?"
                ) from ex

            try:
                configs = tomlkit.parse(config_raw)
            except tomlkit.exceptions.TOMLKitError as ex:
                raise ConfigError(
                    "Failed to parse GerryDB configuration at "
                    f"{GERRYDB_ROOT.resolve()}."
                ) from ex

            try:
                config = configs[profile]
            except KeyError:
                raise ConfigError(
                    f'Profile "{profile}" not found in configuration '
                    f"at {GERRYDB_ROOT.resolve()}."
                )

            for field in ("host", "key"):
                if field not in config:
                    raise ConfigError(
                        f'Field "{field}" not in profile "{profile}" '
                        f"in configuration at {GERRYDB_ROOT.resolve()}."
                    )

            profile_cache_dir = Path(GERRYDB_ROOT / "caches" / profile)
            try:
                profile_cache_dir.mkdir(parents=True, exist_ok=True)
            except IOError as ex:
                raise ConfigError("Failed to create cache directory.") from ex

            self._temp_dir = None
            self.cache = GerryCache(
                database=GERRYDB_ROOT / "caches" / f"{profile}.db",
                data_dir=profile_cache_dir,
                max_size_gb=cache_max_size_gb,
            )

            host = config["host"]
            key = config["key"]

        self._base_url = (
            f"http://{host}/api/v1"
            if host.startswith("localhost")
            else f"https://{host}/api/v1"
        )
        self._base_headers = {"User-Agent": "gerrydb-client-py", "X-API-Key": key}
        self._transport = httpx.HTTPTransport(retries=1)

        self.client = httpx.Client(
            base_url=self._base_url,
            headers=self._base_headers,
            timeout=timeout,
            transport=self._transport,
        )

    # TODO: add a flag to all methods to force the use of the context manager
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client is not None:
            self.client.close()
            self.client = None
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None

        return False

    def context(self, notes: str = "") -> "WriteContext":
        """Creates a write context with session-level metadata.

        Args:
            notes: Freeform notes to associate with the write session.

        Returns:
            A context manager for GerryDB writes.
        """
        log.info("Creating a write context with notes: %s", notes)
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
        return NamespaceRepo(schema=None, base_url=None, session=self)

    @property
    def plans(self) -> PlanRepo:
        """Districting plans."""
        return PlanRepo(schema=Plan, base_url="/plans", session=self)

    @property
    def views(self) -> ViewRepo:
        """Views."""
        return ViewRepo(schema=ViewMeta, base_url="/views", session=self)

    @property
    def view_templates(self) -> ViewTemplateRepo:
        """View templates."""
        return ViewTemplateRepo(
            schema=ViewTemplate, base_url="/view-templates", session=self
        )


@dataclass
class WriteContext:
    """Context for a GerryDB transaction."""

    db: GerryDB
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
                "X-GerryDB-Meta-ID": str(self.meta.uuid),
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
        return NamespaceRepo(schema=None, base_url=None, session=self.db, ctx=self)

    @property
    def plans(self) -> PlanRepo:
        """Districting plans."""
        return PlanRepo(schema=Plan, base_url="/plans", session=self.db, ctx=self)

    @property
    def views(self) -> ViewRepo:
        """Views."""
        return ViewRepo(schema=ViewMeta, base_url="/views", session=self.db, ctx=self)

    @property
    def view_templates(self) -> ViewTemplateRepo:
        """View templates."""
        return ViewTemplateRepo(
            schema=ViewTemplate, base_url="/view-templates", session=self.db, ctx=self
        )

    def __create_geos(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        *,
        namespace: str,
        locality: Locality,
        layer: GeoLayer,
        batch_size: int,
        max_conns: int,
        allow_empty_polys: bool = False,
    ) -> None:
        """
        Private method called by the `load_dataframe` method to load geometries
        into the database.

        Adds the geometries in the 'geometry' column of the dataframe to the database.

        Args:
            df: The dataframe containing the geometries to be added.
            namespace: The namespace to which the geometries belong.
            locality: The locality to which the geometries belong. (e.g. 'pennsylvania')
            layer: The layer to which the geometries belong. (e.g. 'vtd')
            batch_size: The number of rows to import per batch.
            max_conns: The maximum number of simultaneous connections to the API.

        """
        if "geometry" in df.columns:
            df = df.to_crs("epsg:4269")  # import as lat/long
            geos = dict(df.geometry)
        else:
            geos = {key: Polygon() for key in df.index}

        # Augment geographies with internal points if available.
        if "internal_point" in df.columns:
            internal_points = dict(df.internal_point)
            geos = {path: (geo, internal_points[path]) for path, geo in geos.items()}

        if layer.namespace != namespace:
            self.db.geo.fork_geos(
                path=locality.canonical_path,
                namespace=namespace,
                layer_name=layer.path,
                source_namespace=layer.namespace,
                source_layer_name=layer.path,
                allow_extra_source_geos=True,
                allow_empty_polys=allow_empty_polys,
            )
        else:
            try:
                asyncio.run(
                    _load_geos(self.geo, geos, namespace, batch_size, max_conns)
                )

            except Exception as e:
                if str(e) == "Cannot create geographies that already exist.":
                    # TODO: Make this error more specific maybe?
                    raise e
                raise e

        # Update the layer to be in the correct namespace before you map it
        ns_layer = self.db.geo_layers[layer.path]
        self.geo_layers.map_locality(
            layer=ns_layer,
            locality=locality,
            geographies=[f"/{namespace}/{key}" for key in df.index],
        )

    def __update_geos(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        *,
        namespace: str,
        locality: Union[str, Locality],
        layer: Union[str, GeoLayer],
        batch_size: int,
        max_conns: int,
        allow_empty_polys: bool,
    ) -> None:
        """
        Private method called by the `load_dataframe` method to update geos
        that are already present in the database. If some geos are not present
        in the database, this will raise an error.

        Args:
            df: The dataframe containing the geometries to be added.
            namespace: The namespace to which the geometries belong.
            locality: The locality to which the geometries belong. (e.g. 'pennsylvania')
            layer: The layer to which the geometries belong. (e.g. 'vtd')
            batch_size: The number of rows to import per batch.
            max_conns: The maximum number of simultaneous connections to the API.

        """
        if "geometry" in df.columns:
            df = df.to_crs("epsg:4269")  # import as lat/long
            geos = dict(df.geometry)
        else:
            geos = {key: None for key in df.index}

        # Augment geographies with internal points if available.
        if "internal_point" in df.columns:
            internal_points = dict(df.internal_point)
            geos = {path: (geo, internal_points[path]) for path, geo in geos.items()}

        if layer.namespace != namespace:
            self.db.geo.fork_geos(
                path=locality.canonical_path,
                namespace=namespace,
                layer_name=layer.path,
                source_namespace=layer.namespace,
                source_layer_name=layer.path,
            )
        else:
            try:
                asyncio.run(
                    _update_geos(
                        repo=self.geo,
                        geos=geos,
                        namespace=namespace,
                        batch_size=batch_size,
                        max_conns=max_conns,
                        allow_empty_polys=allow_empty_polys,
                    )
                )

            except Exception as e:
                if str(e) == "Cannot create geographies that already exist.":
                    # TODO: Make this error more specific maybe?
                    raise e
                raise e

        if locality is not None and layer is not None:
            # Update the layer to be in the correct namespace before you map it
            ns_layer = self.db.geo_layers[layer.path]
            self.geo_layers.map_locality(
                layer=ns_layer,
                locality=locality,
                geographies=[f"/{namespace}/{key}" for key in df.index],
            )

    # TODO: this is not finished yet
    def __upsert_geos(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        *,
        namespace: str,
        locality: Union[str, Locality],
        layer: Union[str, GeoLayer],
        batch_size: int,
        max_conns: int,
    ) -> None:
        raise NotImplementedError("This method is not finished yet.")

    def __validate_geos_compatabilty(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        locality: Locality,
        layer: GeoLayer,
        namespace: str,
        allow_extra_source_geos: bool = False,
        allow_empty_polys: bool = False,
    ):
        """
        A private method called by the `load_dataframe` method to validate that the passed
        geometry paths exist in the database. This method also checks to make sure that empty
        geometries are not uploaded unless explicitly allowed.

        All of the geometry paths in the dataframe must exist in a single locality and in a
        single layer. If they do not, this method will raise an error.

        The geometries themselves will be created before this is called and this will
        then be called in the same transaction to validate that we can create the corresponding
        geo version for the geography. If the paths do not line up, that means that WKBs cannot
        be loaded since the Geographies (paths) that they should bind to are not present in the
        layer or namespace.

        Args:
            df: The dataframe containing the geometries to be added.
            locality: The locality to which the geometries belong.
            layer: The layer to which the geometries belong.

        Raises:
            ValueError: If the locality or layer is not provided.
            IndexError: If the paths in the index of the dataframe do not match any of the paths in
                the database.
            ValueError: If there are paths missing from the dataframe compared to the paths for
                the given locality and layer in the database. All geometries must be updated
                at the same time to avoid unintentional null values.
        """

        locality_path = locality.canonical_path
        layer_path = layer.path

        log.debug(
            "Checking known paths for locality '%s' in layer '%s' belonging to namespace '%s'",
            locality_path,
            layer_path,
            layer.namespace,
        )

        # Grab all paths for the layer in the namespace
        known_paths = set(
            self.db.geo.all_paths(
                path=locality_path,
                layer_name=layer_path,
                namespace=layer.namespace,
            )
        )

        df_paths = set(df.index)
        empty_polygon_wkb = Polygon().wkb
        empty_hash = hashlib.md5(empty_polygon_wkb).hexdigest()

        if "geometry" in df.columns:
            df_path_hash_dict = df.geometry.apply(
                lambda x: hashlib.md5(WKBElement(x.wkb, srid=4269).data).hexdigest()
            ).to_dict()
        else:
            df_path_hash_dict = {idx: empty_hash for idx in df.index}

        layer_path_hash_dict = df_path_hash_dict.copy()
        if layer.namespace != namespace:
            layer_path_hash_dict = dict(
                self.db.geo.check_forkability(
                    path=locality_path,
                    namespace=namespace,
                    layer_name=layer_path,
                    source_namespace=layer.namespace,
                    source_layer_name=layer.path,
                    allow_extra_source_geos=allow_extra_source_geos,
                    allow_empty_polys=allow_empty_polys,
                )
            )

        if df_path_hash_dict != layer_path_hash_dict:
            if "geometry" in df.columns:
                conflicting_paths = list(
                    path
                    for path in df_path_hash_dict
                    if layer_path_hash_dict[path] != df_path_hash_dict[path]
                )
                raise ValueError(
                    "Conflicting geometries found in dataframe and passed layer. "
                    "The following paths have conflicting geometries in the dataframe's "
                    f"geometry column and the layer {layer.path} in the namespace "
                    f"{layer.namespace}: "
                    f"{conflicting_paths}"
                )
        else:
            if (
                layer.namespace == namespace
                and any([val == empty_hash for val in df_path_hash_dict.values()])
                and not allow_empty_polys
            ):
                if "geometry" in df.columns:
                    raise ValueError(
                        "The 'geometry' column in the dataframe contains empty polygons "
                        "but empty polygons have not been allowed explicitly. If you would like "
                        "to allow for the creation of empty polygons, please pass the value "
                        "`allow_empty_polys=True` to the `load_dataframe` method."
                    )

                raise ValueError(
                    "No 'geometry' column found in dataframe and empty polygons "
                    "have not been allowed explicitly. If you would like to allow"
                    "for the creation of empty polygons, please pass the value "
                    "`allow_empty_polys=True` to the `load_dataframe` method."
                )

            if (
                layer.namespace != namespace
                and any([val == empty_hash for val in layer_path_hash_dict.values()])
                and not allow_empty_polys
            ):
                raise ValueError(
                    f"Attempted to fork geometries from layer {layer.path} in namespace "
                    f"{layer.namespace} to layer {layer.path} in namespace {namespace}. "
                    f"However, some of the source geometries in '{layer.namespace}/{layer.path}' "
                    f"are empty polygons and empty polygons have not been allowed explicitly. "
                    "If you would like to allow for the creation of empty polygons, please pass "
                    "the value `allow_empty_polys=True` to the `load_dataframe` method."
                )

        log.debug("Known paths: %s", known_paths)

        if df_paths - known_paths == df_paths:
            example_found_path = (
                list(known_paths)[0]
                if len(known_paths) > 0
                else "NO GEOGRAPHIES FOUND IN NAMESPACE MATCHING GIVEN LOCALITY AND LAYER"
            )

            raise IndexError(
                f"The index of the dataframe does not appear to match any geographies in the namespace "
                f"which have the following geoid format: '{example_found_path}'. "
                f"Please ensure that the index of the dataframe matches the format of the geoid."
            )

        if df_paths - known_paths != set():
            raise ValueError(
                f"Failure in load_dataframe. Tried to import geographies for layer "
                f"'{layer_path}' and locality '{locality_path}', but the following geographies "
                f"do not exist in the namespace "
                f"'{self.db.namespace}': {df_paths - known_paths}"
            )

        if known_paths - df_paths != set():
            raise ValueError(
                f"Failure in load_dataframe. Tried to import geographies for layer "
                f"'{layer_path}' and locality '{locality_path}', but the passed dataframe "
                f"does not contain the following geographies: "
                f"{known_paths - df_paths}. "
                f"Please provide values for these geographies in the dataframe "
                f"or create a new locality with only the relevant geographies."
            )

    def __validate_columns(self, columns):
        """
        Private method called by the `load_dataframe` method to validate the columns
        passed to the method.

        This method makes sure that the columns passed to the method have the permissible
        data types and that they exist in the database before we attempt to load values for
        those columns.

        Args:
            columns: The columns to be loaded.

        Raises:
            ValueError: If the columns parameter is not a list, a pandas Index, or a dictionary.
            ValueError: If the columns parameter is a dictionary and contains a value that is
                not a Column object.
            ValueError: If some of the columns in `columns` do not exist in the database.
                This also looks for close matches to the columns in the database
                and prints them out for the user.
        """

        if not (
            isinstance(columns, list)
            or isinstance(columns, pdIndex)
            or isinstance(columns, dict)
        ):
            raise ValueError(
                f"The columns parameter must be a list of paths, a pandas.core.indexes.base.Index, "
                f"or a dictionary of paths to Column objects. "
                f"Received type {type(columns)}."
            )

        if isinstance(columns, list) or isinstance(columns, pdIndex):
            column_paths = []
            for col in self.db.columns.all():
                column_paths.append(col.canonical_path)
                column_paths.extend(col.aliases)

            column_paths = set(column_paths)
            cur_columns = set([normalize_path(col) for col in columns])

            for col in cur_columns:
                if "/" in col:
                    raise ValueError(
                        f"Column paths passed to the `load_dataframe` function "
                        f"cannot contain '/'. Column '{col}' is invalid."
                    )

        else:
            for item in columns.values():
                if not isinstance(item, Column):
                    raise ValueError(
                        f"The columns parameter must be a list of paths, a pandas.core.indexes.base.Index, "
                        f"or a dictionary of paths to Column objects. "
                        f"Found a dictionary with a value of type {type(item)}."
                    )

            column_paths = {col.canonical_path for col in self.db.columns.all()}
            cur_columns = set([v.canonical_path for v in columns.values()])

        missing_cols = cur_columns - column_paths

        if missing_cols != set():
            for path in missing_cols:
                best_matches = process.extract(path, column_paths, limit=5)
                print(
                    f"Could not find column corresponding to '{path}', the best matches "
                    f"are: {[match[0] for match in best_matches]}"
                )
            raise ValueError(
                f"Some of the columns in the dataframe do not exist in the database. "
                f"Please create the missing columns first using the `db.columns.create` method."
            )

    def __validate_load_types(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        *,
        namespace: Optional[str],
        locality: Optional[Union[str, Locality]],
        layer: Optional[Union[str, GeoLayer]],
        create_geos: bool,
        patch_geos: bool,
        upsert_geos: bool,
        allow_empty_polys: bool,
    ):
        if namespace is None:
            raise ValueError("No Namespace provided.")

        if create_geos or upsert_geos:
            if locality is None:
                raise ValueError(
                    "Locality must be provided when creating or upserting Geos"
                )

            if layer is None:
                raise ValueError(
                    "GeoLayer must be provided when creating or upserting Geos"
                )

        assert (
            isinstance(create_geos, bool)
            and isinstance(patch_geos, bool)
            and isinstance(upsert_geos, bool)
        )

        if not ([create_geos, patch_geos, upsert_geos].count(True) <= 1):
            raise ValueError(
                "Exactly one of `create_geo`, `patch_geos`, or `upsert_geos` must be True, or"
                " all must be False."
            )

        if patch_geos or not (create_geos | upsert_geos | patch_geos):
            # This checks that the geos are good in the locality
            # If we plan to fork the geos, we need to check that the source and target
            # namespaces are compatible.
            log.debug("VALIDATING GEOS BETWEEN SOURCE AND TARGET NAMESPACES")
            self.__validate_geos_compatabilty(
                df=df,
                namespace=namespace,
                locality=locality,
                layer=layer,
                allow_empty_polys=allow_empty_polys,
            )

        if create_geos and (layer.namespace != namespace):
            self.__validate_geos_compatabilty(
                df=df,
                namespace=namespace,
                locality=locality,
                layer=layer,
                allow_extra_source_geos=True,
                allow_empty_polys=allow_empty_polys,
            )

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        columns: Union[pdIndex, list[str], dict[str, Column]],
        *,
        create_geos: bool = False,
        patch_geos: bool = False,
        upsert_geos: bool = False,
        allow_empty_polys: bool = False,
        namespace: Optional[str] = None,
        locality: Optional[Union[str, Locality]] = None,
        layer: Optional[Union[str, GeoLayer]] = None,
        batch_size: int = 5000,
        max_conns: int = 1,
    ) -> None:
        """
        Imports a DataFrame to GerryDB.

        Plain DataFrames do not include rich column metadata, so the columns used
        in the DataFrame must be defined before import.

        Given column metadata, this function imports column values. It also optionally
        creates geographies.

            * If `create_geo` is `True` and a `geometry` column is present in `df`,
                geographies are imported in addition to tabular data.
            * If `create_geo` is `True` and a `geometry` column is not present in `df`,
                empty geographies are created in addition to tabular data.
            * If `create_geo` is `False`, it is assumed that the rows in the DataFrame
            correspond to geographies that already exist in GerryDB.

        In all cases, the index of `df` is used as the key for geographies within
        `namespace`. Typically, the `GEOID` column or equivalent should be used as
        the index.

        If `layer` and `locality` are provided, a `GeoSet` is created from the
        rows in `df`.

        Args:
            df: DataFrame to import column values and geographies from. The df MUST be indexed
                by the geoid or the import will fail.
            columns: Mapping between column names in `df` and GerryDB column metadata.
                Only columns included in the mapping will be imported.
            create_geo: Determines whether to create geographies from the DataFrame.
            patch_geos: Determines whether to update existing geographies from the DataFrame.
            upsert_geos: Combination of create and patch. If the geographies exist, they will be
                updated. If they do not exist, they will be created.
            namespace: Namespace to load geographies into.
            locality: `Locality` to associate a new `GeoSet` with.
            layer: `GeoLayer` to associate a new `GeoSet` with.
            batch_size: Number of rows to import per API request batch.
            max_conns: Maximum number of simultaneous API connections.
        """
        log.debug("IN THE LOAD FUNCTION")
        namespace = self.db.namespace if namespace is None else namespace

        self.__validate_load_types(
            df=df,
            namespace=namespace,
            locality=locality,
            layer=layer,
            create_geos=create_geos,
            patch_geos=patch_geos,
            upsert_geos=upsert_geos,
            allow_empty_polys=allow_empty_polys,
        )

        if create_geos or upsert_geos:
            if not isinstance(locality, Locality):
                locality = self.db.localities[locality]

            if not isinstance(layer, GeoLayer):
                layer = self.db.geo_layers[layer]

        if create_geos:
            self.__create_geos(
                df=df,
                namespace=namespace,
                locality=locality,
                layer=layer,
                batch_size=batch_size,
                max_conns=max_conns,
                allow_empty_polys=allow_empty_polys,
            )

        if patch_geos:
            self.__update_geos(
                df=df,
                namespace=namespace,
                locality=locality,
                layer=layer,
                batch_size=batch_size,
                max_conns=max_conns,
                allow_empty_polys=allow_empty_polys,
            )
        if upsert_geos:
            self.__upsert_geos(
                df=df,
                namespace=namespace,
                locality=locality,
                layer=layer,
                batch_size=batch_size,
                max_conns=max_conns,
            )

        log.debug("VALIDATING COLUMNS")
        self.__validate_columns(columns)

        # TODO: Check to see if grabbing all of the columns and then filtering
        # is significantly different from a data transfer perspective in the
        # average case.
        if not isinstance(columns, dict):
            columns = {c: self.columns.get(c) for c in df.columns}

        asyncio.run(
            _load_column_values(self.columns, df, columns, batch_size, max_conns)
        )

        log.debug("FINISHED LOADING DATAFRAME")


# based on https://stackoverflow.com/a/61478547
async def gather_batch(coros, n):
    """Limits concurrency of a batch of coroutines."""
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros), return_exceptions=True)


async def _load_geos(
    repo: GeographyRepo,
    geos: dict[str, GeoValType],
    namespace: str,
    batch_size: int,
    max_conns: Optional[int],
) -> list[Geography]:
    """Asynchronously loads geographies in batches."""
    log.debug("IN THE LOAD GEOS FUNCTION")
    geo_pairs = list(geos.items())
    tasks = []
    log.debug("BEFORE ASYNC BULK FOR GEOS %s", geo_pairs[0])
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


async def _update_geos(
    *,
    repo: GeographyRepo,
    geos: dict[str, GeoValType],
    namespace: str,
    batch_size: int,
    max_conns: Optional[int],
    allow_empty_polys: bool,
) -> list[Geography]:
    """Asynchronously loads geographies in batches."""
    log.debug("IN THE LOAD GEOS FUNCTION")
    geo_pairs = list(geos.items())
    tasks = []
    log.debug("BEFORE ASYNC BULK FOR GEOS %s", geo_pairs[0])
    async with repo.async_bulk(namespace, max_conns) as ctx:
        for idx in range(0, len(geo_pairs), batch_size):
            chunk = dict(geo_pairs[idx : idx + batch_size])
            tasks.append(ctx.update(chunk, allow_empty_polys=allow_empty_polys))
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
        # This only works because the df is indexed by geography path.
        col_vals = list(dict(df[col_name]).items())
        for idx in range(0, len(df), batch_size):
            val_batches.append((col_meta, dict(col_vals[idx : idx + batch_size])))

    async with httpx.AsyncClient(**params) as client:
        tasks = [
            repo.async_set_values(
                path=col.path,
                namespace=col.namespace,
                col=col,
                values=batch,
                client=client,
            )
            for col, batch in val_batches
        ]
        results = await gather_batch(tasks, max_conns)

    # TODO: more sophisticated error handling -- which batches were successful?
    # what can be retried? etc.
    for result in results:
        if isinstance(result, Exception):
            raise result
