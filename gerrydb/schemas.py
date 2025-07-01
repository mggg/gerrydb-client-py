"""Schemas for GerryDB objects.

This file should be kept in sync with the server-side version.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union, Annotated
from uuid import UUID

import pyproj
from pydantic import AnyUrl
from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

UserEmail = Annotated[
    str,
    Field(
        max_length=255, min_length=3, pattern=r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
    ),
]
# / allowed at start. 1-2 segments. Used for objects that are not namespaced like
# localities.
GerryPath = Annotated[
    str,
    Field(
        pattern=r"^/?[a-z0-9][a-z0-9-_.]+(?:/[a-z0-9][a-z0-9-_.]+){0,1}$",
        max_length=255,
        min_length=2,
    ),
]
# / allowed at start. 1-3 segments. Leading character from each segment must be a-z0-9. No
# uppercase characters allowed. Used for namespaced objects like columns, column sets, etc.
NamespacedGerryPath = Annotated[
    str,
    Field(
        pattern=r"^/?[a-z0-9][a-z0-9-_.]+(?:/[a-z0-9][a-z0-9-_.]+){0,2}$",
        max_length=255,
        min_length=2,
    ),
]
# / allowed at start. 1-3 segments. Leading character from each segment must be a-z0-9 and A-Z
# allowed in last segment for weird GEOIDs.
NamespacedGerryGeoPath = Annotated[
    str,
    Field(
        pattern=r"^/?[a-z0-9][a-z0-9-_.]+(?:/[a-z0-9][a-z0-9-_.]+){0,1}"
        r"(?:/[a-zA-Z0-9][a-zA-Z0-9-_.]+){0,1}$",
        max_length=255,
        min_length=2,
    ),
]
# No capital letters allowed
NameStr = Annotated[
    str,
    Field(
        pattern=r"^[a-z0-9][a-z0-9-_.]+$",
        max_length=100,
        min_length=2,
    ),
]
# Capital letters allowed because some vtds suck
GeoNameStr = Annotated[
    str,
    Field(pattern=r"^[a-z0-9][a-zA-Z0-9-_.]+$", max_length=100, min_length=2),
]
Description = Optional[
    Annotated[
        str,
        Field(max_length=5000, min_length=1),
    ]
]
ShortStr = Optional[
    Annotated[
        str,
        Field(max_length=100, min_length=1),
    ]
]
UUIDStr = Annotated[
    str,
    Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]

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
        return self.value  # pragma: no cover


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

    notes: Description


class ObjectMetaCreate(ObjectMetaBase):
    """Object metadata received on creation."""


class ObjectMeta(ObjectMetaBase):
    """Object metadata returned by the database."""

    uuid: UUID
    created_at: datetime
    created_by: UserEmail


class LocalityBase(BaseModel):
    """Base model for locality metadata."""

    canonical_path: GerryPath
    parent_path: Optional[GerryPath] = None
    default_proj: ShortStr = "EPSG:4269"  # Default to NAD83
    name: ShortStr


class LocalityCreate(LocalityBase):
    """Locality metadata received on creation."""

    aliases: Optional[list[NameStr]] = None


class LocalityPatch(BaseModel):
    """Locality metadata received on PATCH."""

    aliases: list[NameStr]


class Locality(LocalityBase):
    """A locality returned by the database."""

    aliases: list[NameStr]
    meta: ObjectMeta

    def __repr__(self):  # pragma: no cover
        if self.parent_path is None:
            return f"Locality: {self.name} ({self.canonical_path})"
        return f"Locality: {self.name} ({self.canonical_path} → {self.parent_path})"


class NamespaceBase(BaseModel):
    """Base model for namespace metadata."""

    path: NameStr
    description: Description
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

    canonical_path: NamespacedGerryPath
    namespace: NameStr
    description: Description
    source_url: Optional[AnyUrl] = None
    kind: ColumnKind
    type: ColumnType


class ColumnCreate(ColumnBase):
    """Column metadata received on creation."""

    aliases: Optional[list[NameStr]]


class ColumnPatch(BaseModel):
    """Column metadata received on PATCH."""

    aliases: list[NameStr]


class Column(ColumnBase):
    """A column returned by the database."""

    aliases: list[NameStr]
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

    path: NamespacedGerryPath  # of geography
    value: Any


class GeoLayerBase(BaseModel):
    """Base model for geographic layer metadata."""

    path: NamespacedGerryPath
    description: Description
    source_url: Optional[AnyUrl]


class GeoLayerCreate(GeoLayerBase):
    """Geographic layer metadata received on creation."""


class GeoSetCreate(BaseModel):
    """Paths to geographies in a `GeoSet`."""

    paths: list[NamespacedGerryGeoPath]


class GeoLayer(GeoLayerBase):
    """Geographic layer metadata returned by the database."""

    meta: ObjectMeta
    namespace: NameStr

    @property
    def full_path(self) -> str:
        """The path of the geographic layers, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class GeoImportBase(BaseModel):
    """Base model for a geographic unit import."""


class GeoImport(GeoImportBase):
    """Geographic unit import metadata returned by the database."""

    uuid: UUIDStr
    namespace: NameStr
    created_at: datetime
    created_by: UserEmail
    meta: ObjectMeta


class GeographyBase(BaseModel):
    """Base model for a geographic unit."""

    path: GeoNameStr
    geography: Optional[BaseGeometry]
    internal_point: Optional[Point] = None

    class Config:
        arbitrary_types_allowed = True


class GeographyCreate(BaseModel):
    """Geographic unit data received on creation (geography as raw WKB bytes)."""

    path: GeoNameStr
    geography: Optional[bytes]
    internal_point: Optional[bytes]


class Geography(GeographyBase):
    """Geographic unit data returned by the database."""

    meta: ObjectMeta
    namespace: NameStr
    valid_from: datetime

    @property
    def full_path(self):  # pragma: no cover
        """The path of the geography, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class ColumnSetBase(BaseModel):
    """Base model for a logical column grouping."""

    path: NameStr
    description: Description


class ColumnSetCreate(ColumnSetBase):
    """Column grouping data received on creation."""

    columns: list[NamespacedGerryPath]


class ColumnSet(ColumnSetBase):
    """Logical column grouping returned by the database."""

    meta: ObjectMeta
    namespace: NameStr
    columns: list[Column]
    refs: list[NameStr]

    @property
    def path_with_resource(self) -> str:
        """The column set's absolute path."""
        return f"/column-sets/{self.namespace}/{self.path}"


class ViewTemplateBase(BaseModel):
    """Base model for a view template."""

    path: NameStr
    description: Description


class ViewTemplateCreate(ViewTemplateBase):
    """View template data received on creation."""

    members: list[NamespacedGerryPath]


class ViewTemplatePatch(ViewTemplateBase):
    """View template data received on update."""

    members: list[NamespacedGerryPath]


class ViewTemplate(ViewTemplateBase):
    """View template returned by the database."""

    namespace: NameStr
    members: list[Union[Column, ColumnSet]]
    meta: ObjectMeta
    valid_from: datetime

    @property
    def full_path(self) -> str:
        """The path of the view template, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class PlanBase(BaseModel):
    """Base model for a districting plan."""

    path: NameStr
    description: Description
    source_url: Optional[AnyUrl] = None
    districtr_id: ShortStr = None
    daves_id: ShortStr = None


class PlanCreate(PlanBase):
    """Districting plan definition received on creation."""

    locality: GerryPath
    layer: NamespacedGerryPath
    assignments: dict[NamespacedGerryPath, str]


class Plan(PlanBase):
    """Rendered districting plan."""

    namespace: NameStr
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime
    num_districts: int
    complete: bool
    assignments: dict[NamespacedGerryPath, Optional[str]]


class GraphBase(BaseModel):
    """Base model for a dual graph."""

    path: NameStr
    description: Description = None
    proj: ShortStr = None


WeightedEdge = tuple[NamespacedGerryPath, NamespacedGerryPath, Optional[dict]]


class GraphCreate(GraphBase):
    """Dual graph definition received on creation."""

    locality: GerryPath
    layer: NamespacedGerryPath
    edges: list[WeightedEdge]


class GraphMeta(GraphBase):
    """Dual graph metadata returned by the database."""

    namespace: NameStr
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    created_at: datetime

    @property
    def full_path(self):  # pragma: no cover
        """The path of the geography, including its namespace."""
        return f"/{self.namespace}/{self.path}"


class Graph(GraphMeta):
    """Rendered dual graph without node attributes."""

    edges: list[WeightedEdge]


class ViewBase(BaseModel):
    """Base model for a view."""

    path: NameStr


class ViewCreate(ViewBase):
    """View definition received on creation."""

    template: NamespacedGerryPath
    locality: NamespacedGerryPath
    layer: NamespacedGerryPath
    graph: Optional[NamespacedGerryPath] = None

    valid_at: Optional[datetime] = None
    proj: ShortStr = None

    class Config:
        # Whenever you call model.json(), turn datetimes into ISO strings
        json_encoders = {datetime: lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}

    def model_dump(self, *args, **kwargs):
        data = super().model_dump(*args, **kwargs)

        # now post‑process valid_at if it's still a datetime
        va = data.get("valid_at")
        if isinstance(va, datetime):
            data["valid_at"] = va.strftime("%Y-%m-%d %H:%M:%S.%fZ")
        return data


class ViewMeta(ViewBase):
    """View metadata."""

    namespace: NameStr
    template: ViewTemplate
    locality: Locality
    layer: GeoLayer
    meta: ObjectMeta
    valid_at: datetime
    proj: ShortStr
    graph: Optional[GraphMeta]
