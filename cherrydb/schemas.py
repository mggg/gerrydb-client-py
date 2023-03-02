"""Schemas for CherryDB objects.

This file should be kept in sync with the server-side version.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union

from pydantic import AnyUrl
from pydantic import BaseModel as PydanticBaseModel
from pydantic import constr
from shapely.geometry.base import BaseGeometry

UserEmail = constr(max_length=254)

CherryPath = constr(regex=r"[a-z0-9][a-z0-9-_/]*")
NamespacedCherryPath = constr(regex=r"[a-z0-9/][a-z0-9-_/]*")


class ObjectCachePolicy(str, Enum):
    """A schema's single-object caching policy."""

    ETAG = "etag"
    TIMESTAMP = "timestamp"
    NONE = "none"


class ColumnKind(str, Enum):
    """Meaning of a column."""

    COUNT = "count"
    PERCENT = "percent"
    CATEGORICAL = "categorical"
    IDENTIFIER = "identifier"
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
    """Base model for CherryDB objects."""

    class Config:
        frozen = True


class NamespaceGroup(str, Enum):
    """A namespace group.

    Namespace groups only exist for authorization and are not intended to change
    over time---they simply allow us to distinguish between public namespaces
    (more or less visible to anyone with access to the Cherry instance)
    and private namespaces (visible only to users with explicit permissions).
    """

    PUBLIC = "public"
    PRIVATE = "private"
    ALL = "all"


class ObjectMetaBase(BaseModel):
    """Base model for object metadata."""

    notes: str | None


class ObjectMetaCreate(ObjectMetaBase):
    """Object metadata received on creation."""


class ObjectMeta(ObjectMetaBase):
    """Object metadata returned by the database."""

    uuid: str
    created_at: datetime
    created_by: UserEmail


class LocalityBase(BaseModel):
    """Base model for locality metadata."""

    canonical_path: CherryPath
    parent_path: CherryPath | None
    default_proj: str | None
    name: str


class LocalityCreate(LocalityBase):
    """Locality metadata received on creation."""

    aliases: list[CherryPath] | None


class LocalityPatch(BaseModel):
    """Locality metadata received on PATCH."""

    aliases: list[CherryPath]


class Locality(LocalityBase):
    """A locality returned by the database."""

    __cache_name__ = "locality"
    __cache_policy__ = ObjectCachePolicy.ETAG
    __cache_aliased__ = True

    aliases: list[CherryPath]
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

    __cache_name__ = "namespace"
    __cache_policy__ = ObjectCachePolicy.ETAG

    meta: ObjectMeta

    class Config:
        orm_mode = True


class ColumnBase(BaseModel):
    """Base model for locality metadata."""

    canonical_path: CherryPath
    namespace: str
    description: str
    source_url: AnyUrl | None
    kind: ColumnKind
    type: ColumnType


class ColumnCreate(ColumnBase):
    """Column metadata received on creation."""

    aliases: list[CherryPath] | None


class ColumnPatch(BaseModel):
    """Column metadata received on PATCH."""

    aliases: list[CherryPath]


class Column(ColumnBase):
    """A column returned by the database."""

    __cache_name__ = "column"
    __cache_policy__ = ObjectCachePolicy.ETAG
    __cache_aliased__ = True

    aliases: list[CherryPath]
    meta: ObjectMeta

    @property
    def path(self):
        """The column's canonical path."""
        return self.canonical_path

    @property
    def path_with_resource(self) -> str:
        """The column's absolute path."""
        return f"/columns/{self.namespace}/{self.path}"


class ColumnValue(BaseModel):
    """Value of a column for a geography."""

    path: str  # of geography
    value: Any


class GeoLayerBase(BaseModel):
    """Base model for geographic layer metadata."""

    path: CherryPath
    description: str | None
    source_url: AnyUrl | None


class GeoLayerCreate(GeoLayerBase):
    """Geographic layer metadata received on creation."""


class GeoSetCreate(BaseModel):
    """Paths to geographies in a `GeoSet`."""

    paths: list[str]


class GeoLayer(GeoLayerBase):
    """Geographic layer metadata returned by the database."""

    __cache_name__ = "geo_layer"
    __cache_policy__ = ObjectCachePolicy.ETAG

    meta: ObjectMeta
    namespace: str


class GeoImportBase(BaseModel):
    """Base model for a geographic unit import."""


class GeoImport(GeoImportBase):
    """Geographic unit import metadata returned by the database."""

    __cache_name__ = "geo_import"

    uuid: str
    namespace: str
    created_at: datetime
    created_by: str
    meta: ObjectMeta


class GeographyBase(BaseModel):
    """Base model for a geographic unit."""

    path: CherryPath
    geography: Optional[BaseGeometry]

    class Config:
        arbitrary_types_allowed = True


class GeographyCreate(BaseModel):
    """Geographic unit data received on creation (geography as raw WKB bytes)."""

    path: CherryPath
    geography: Optional[bytes]


class Geography(GeographyBase):
    """Geographic unit data returned by the database."""

    __cache_name__ = "geography"
    __cache_policy__ = ObjectCachePolicy.TIMESTAMP

    meta: ObjectMeta
    namespace: str
    valid_from: datetime

    @property
    def full_path(self):
        """The path of the geography, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class ColumnSetBase(BaseModel):
    """Base model for a logical column grouping."""

    path: CherryPath
    description: str


class ColumnSetCreate(ColumnSetBase):
    """Column grouping data received on creation."""

    columns: list[NamespacedCherryPath]


class ColumnSet(ColumnSetBase):
    """Logical column grouping returned by the database."""

    __cache_name__ = "column_set"
    __cache_policy__ = ObjectCachePolicy.ETAG

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

    path: CherryPath
    description: str


class ViewTemplateCreate(ViewTemplateBase):
    """View template data received on creation."""

    members: list[str]


class ViewTemplatePatch(ViewTemplateBase):
    """View template data received on update."""

    members: list[str]


class ViewTemplate(ViewTemplateBase):
    """View template returned by the database."""
    
    __cache_name__ = "view_template"
    __cache_policy__ = ObjectCachePolicy.TIMESTAMP

    members: list[Union[Column, ColumnSet]]
    meta: ObjectMeta
    valid_from: datetime