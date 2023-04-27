"""Schemas for GerryDB objects.

This file should be kept in sync with the server-side version.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union

import pyproj
from pydantic import AnyUrl
from pydantic import BaseModel as PydanticBaseModel
from pydantic import constr
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

UserEmail = constr(max_length=254)

GerryPath = constr(regex=r"[a-z0-9][a-z0-9-_/]*")
NamespacedGerryPath = constr(regex=r"[a-z0-9/][a-z0-9-_/]*")

NATIVE_PROJ = pyproj.CRS("EPSG:4269")


class ColumnKind(str, Enum):
    """Meaning of a column."""

    COUNT = "count"
    PERCENT = "percent"
    CATEGORICAL = "categorical"
    IDENTIFIER = "identifier"
    AREA = "area"
    OTHER = "other"


class ColumnType(str, Enum):
    """Data type of a column."""

    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STR = "str"
    JSON = "json"


class ScopeType(str, Enum):
    """An abstract scope (no namespace information)."""

    NAMESPACE_READ = "namespace:read:*"
    NAMESPACE_WRITE = "namespace:write:*"
    NAMESPACE_WRITE_DERIVED = "namespace:write_derived:*"
    NAMESPACE_CREATE = "namespace:create"
    LOCALITY_READ = "locality:read"
    LOCALITY_WRITE = "locality:write"
    META_READ = "meta:read"
    META_WRITE = "meta:write"
    ALL = "all"

    def __str__(self):
        return self.value


class BaseModel(PydanticBaseModel):
    """Base model for GerryDB objects."""

    class Config:
        frozen = True


class NamespaceGroup(str, Enum):
    """A namespace group.

    Namespace groups only exist for authorization and are not intended to change
    over time---they simply allow us to distinguish between public namespaces
    (more or less visible to anyone with access to the GerryDB instance)
    and private namespaces (visible only to users with explicit permissions).
    """

    PUBLIC = "public"
    PRIVATE = "private"
    ALL = "all"


class ObjectMetaBase(BaseModel):
    """Base model for object metadata."""

    notes: Optional[str]


class ObjectMetaCreate(ObjectMetaBase):
    """Object metadata received on creation."""


class ObjectMeta(ObjectMetaBase):
    """Object metadata returned by the database."""

    uuid: str
    created_at: datetime
    created_by: UserEmail


class LocalityBase(BaseModel):
    """Base model for locality metadata."""

    canonical_path: GerryPath
    parent_path: Optional[GerryPath]
    default_proj: Optional[str]
    name: str


class LocalityCreate(LocalityBase):
    """Locality metadata received on creation."""

    aliases: Optional[list[GerryPath]]


class LocalityPatch(BaseModel):
    """Locality metadata received on PATCH."""

    aliases: list[GerryPath]


class Locality(LocalityBase):
    """A locality returned by the database."""

    aliases: list[GerryPath]
    meta: ObjectMeta

    def __repr__(self):
        if self.parent_path is None:
            return f"Locality: {self.name} ({self.canonical_path})"
        return f"Locality: {self.name} ({self.canonical_path} → {self.parent_path})"


class NamespaceBase(BaseModel):
    """Base model for namespace metadata."""

    path: constr(regex=r"[a-zA-Z0-9-]+")
    description: str
    public: bool


class NamespaceCreate(NamespaceBase):
    """Namespace metadata received on creation."""


class Namespace(NamespaceBase):
    """A namespace returned by the database."""

    meta: ObjectMeta

    class Config:
        orm_mode = True


class ColumnBase(BaseModel):
    """Base model for locality metadata."""

    canonical_path: GerryPath
    namespace: str
    description: str
    source_url: Optional[AnyUrl]
    kind: ColumnKind
    type: ColumnType


class ColumnCreate(ColumnBase):
    """Column metadata received on creation."""

    aliases: Optional[list[GerryPath]]


class ColumnPatch(BaseModel):
    """Column metadata received on PATCH."""

    aliases: list[GerryPath]


class Column(ColumnBase):
    """A column returned by the database."""

    aliases: list[GerryPath]
    meta: ObjectMeta

    @property
    def path(self):
        """The column's canonical path."""
        return self.canonical_path

    @property
    def full_path(self):
        """The path of the column, including its namespace."""
        return f"/{self.namespace}/{self.path}"

    @property
    def path_with_resource(self) -> str:
        """The column's absolute path, including its resource name and namespace."""
        return f"/columns/{self.namespace}/{self.path}"


class ColumnValue(BaseModel):
    """Value of a column for a geography."""

    path: str  # of geography
    value: Any


class GeoLayerBase(BaseModel):
    """Base model for geographic layer metadata."""

    path: GerryPath
    description: Optional[str]
    source_url: Optional[AnyUrl]


class GeoLayerCreate(GeoLayerBase):
    """Geographic layer metadata received on creation."""


class GeoSetCreate(BaseModel):
    """Paths to geographies in a `GeoSet`."""

    paths: list[str]


class GeoLayer(GeoLayerBase):
    """Geographic layer metadata returned by the database."""

    meta: ObjectMeta
    namespace: str

    @property
    def full_path(self) -> str:
        """The path of the geographic layers, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class GeoImportBase(BaseModel):
    """Base model for a geographic unit import."""


class GeoImport(GeoImportBase):
    """Geographic unit import metadata returned by the database."""

    uuid: str
    namespace: str
    created_at: datetime
    created_by: str
    meta: ObjectMeta


class GeographyBase(BaseModel):
    """Base model for a geographic unit."""

    path: GerryPath
    geography: Optional[BaseGeometry]
    internal_point: Optional[Point] = None

    class Config:
        arbitrary_types_allowed = True


class GeographyCreate(BaseModel):
    """Geographic unit data received on creation (geography as raw WKB bytes)."""

    path: GerryPath
    geography: Optional[bytes]
    internal_point: Optional[bytes]


class Geography(GeographyBase):
    """Geographic unit data returned by the database."""

    meta: ObjectMeta
    namespace: str
    valid_from: datetime

    @property
    def full_path(self):
        """The path of the geography, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class ColumnSetBase(BaseModel):
    """Base model for a logical column grouping."""

    path: GerryPath
    description: str


class ColumnSetCreate(ColumnSetBase):
    """Column grouping data received on creation."""

    columns: list[NamespacedGerryPath]


class ColumnSet(ColumnSetBase):
    """Logical column grouping returned by the database."""

    meta: ObjectMeta
    namespace: str
    columns: list[Column]
    refs: list[str]

    @property
    def path_with_resource(self) -> str:
        """The column set's absolute path."""
        return f"/column-sets/{self.namespace}/{self.path}"


class ViewTemplateBase(BaseModel):
    """Base model for a view template."""

    path: GerryPath
    description: str


class ViewTemplateCreate(ViewTemplateBase):
    """View template data received on creation."""

    members: list[str]


class ViewTemplatePatch(ViewTemplateBase):
    """View template data received on update."""

    members: list[str]


class ViewTemplate(ViewTemplateBase):
    """View template returned by the database."""

    namespace: str
    members: list[Union[Column, ColumnSet]]
    meta: ObjectMeta
    valid_from: datetime

    @property
    def full_path(self) -> str:
        """The path of the view template, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class PlanBase(BaseModel):
    """Base model for a districting plan."""

    path: GerryPath
    description: str
    source_url: Optional[AnyUrl] = None
    districtr_id: Optional[str] = None
    daves_id: Optional[str] = None


class PlanCreate(PlanBase):
    """Districting plan definition received on creation."""

    locality: NamespacedGerryPath
    layer: NamespacedGerryPath
    assignments: dict[NamespacedGerryPath, str]


class Plan(PlanBase):
    """Rendered districting plan."""

    namespace: str
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime
    num_districts: int
    complete: bool
    assignments: dict[NamespacedGerryPath, Optional[str]]


class GraphBase(BaseModel):
    """Base model for a dual graph."""

    path: GerryPath
    description: str
    proj: Optional[str] = None


WeightedEdge = tuple[NamespacedGerryPath, NamespacedGerryPath, Optional[dict]]


class GraphCreate(GraphBase):
    """Dual graph definition received on creation."""

    locality: NamespacedGerryPath
    layer: NamespacedGerryPath
    edges: list[WeightedEdge]


class GraphMeta(GraphBase):
    """Dual graph metadata."""

    namespace: str
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime

    @property
    def full_path(self):
        """The path of the geography, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class Graph(GraphMeta):
    """Rendered dual graph without node attributes."""

    edges: list[WeightedEdge]


class ViewBase(BaseModel):
    """Base model for a view."""

    path: GerryPath


class ViewCreate(ViewBase):
    """View definition received on creation."""

    template: NamespacedGerryPath
    locality: NamespacedGerryPath
    layer: NamespacedGerryPath
    graph: Optional[NamespacedGerryPath] = None

    valid_at: Optional[datetime] = None
    proj: Optional[str] = None


class ViewMeta(ViewBase):
    """View metadata."""

    namespace: str
    template: ViewTemplate
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    valid_at: datetime
    proj: Optional[str]
    graph: Optional[GraphMeta]
