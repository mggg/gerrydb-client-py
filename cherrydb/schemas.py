"""Schemas for CherryDB objects.

This file should be kept in sync with the server-side version.
"""
from datetime import datetime
from enum import Enum

from pydantic import AnyUrl, constr
from pydantic import BaseModel as PydanticBaseModel
from shapely.geometry.base import BaseGeometry


UserEmail = constr(max_length=254)

CherryPath = constr(regex=r"[a-z0-9][a-z0-9-_/]*")
NamespacedCherryPath = constr(regex=r"[a-z0-9/][a-z0-9-_/]*")


class ObjectCachePolicy(str, Enum):
    """A schema's single-object caching policy."""

    ETAG = "etag"
    TIMESTAMP = "timestamp"
    NONE = "none"


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

    aliases: list[CherryPath]
    meta: ObjectMeta


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

    aliases: list[CherryPath]
    meta: ObjectMeta


class GeoLayerBase(BaseModel):
    """Base model for geographic layer metadata."""

    path: CherryPath
    description: str | None
    source_url: AnyUrl | None


class GeoLayerCreate(GeoLayerBase):
    """Geographic layer metadata received on creation."""


class GeoLayer(GeoLayerBase):
    """Geographic layer metadata returned by the database."""

    __cache_name__ = "geo_layer"

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
    meta: ObjectMeta


class GeographyBase(BaseModel):
    """Base model for a geographic unit."""

    path: CherryPath
    geography: BaseGeometry

    class Config:
        arbitrary_types_allowed = True


class GeographyCreateRaw(GeographyBase):
    """Geographic unit data received on creation (geography as raw WKB bytes)."""

    path: CherryPath
    geography: bytes


class GeographyCreate(GeographyBase):
    """Geographic unit data received on creation."""


class GeographyPatch(BaseModel):
    """Geographic unit data received on PATCH."""

    class Config:
        arbitrary_types_allowed = True

    geography: BaseGeometry


class Geography(GeographyBase):
    """Geographic unit data returned by the database."""

    __cache_name__ = "geography"

    meta: ObjectMeta
    namespace: str
    modified_at: datetime


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
    __cache_policy__ = ObjectCachePolicy.TIMESTAMP

    meta: ObjectMeta
    namespace: str
    columns: list[Column]