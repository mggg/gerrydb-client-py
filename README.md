# GerryDB User Guide

## Background

### What is GerryDB?
GerryDB is a lightweight redistricting and voting analytics platform developed at MGGG.  We intend it to replace legacy redistricting GIS workflows and to serve as a single source of truth for our research efforts.

GerryDB is _not_ a general-purpose GIS platform like [ArcGIS](https://www.arcgis.com/index.html). It does not have data visualization tools, a sophisticated web interface, or a custom query language. Rather, GerryDB indexes and exposes geospatial, demographic, and election data for the purpose of studying U.S. democracy. Analysts are free to use whatever tools they wish (we recommend [GeoPandas](https://geopandas.org/en/stable/), [QGIS](https://www.qgis.org/en/site/), and [SQLite](https://www.sqlite.org/index.html)) to read and manipulate GerryDB data.


### What is in GerryDB?
MGGG's production instance currently contains 2010 and 2020 U.S. Census [P.L. 94-171](https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html) data on most key Census units. We intend to import Census ACS population data and block-level election data.

### How do I access GerryDB?
GerryDB access currently requires an API key. Contact [code@mggg.org](mailto:code@mggg.org) for more information. (If you are an MGGG summer 2023 student researcher, you should be issued an API key during onboarding.)

## Core concepts

### Views

_(For an interactive introduction to views, see the [tutorial notebook](tutorial.ipynb).)_

The most important GerryDB object is the **view**. A view is a collection of geographies (represented as polygons) with tabular attributes. In this way, a view is roughly analogous to a [shapefile](https://en.wikipedia.org/wiki/Shapefile) or a [GeoPackage](http://www.geopackage.org/). However, views also contain rich metadata that is difficult to store using these traditional file formats. Views contain descriptors for every geography and tabular column, allowing analysts to determine the precise meaning and lineage of the data they are using without needing to track down external documentation. A view also typically contains an adjacency graph (also known as a "dual graph") of its geographies, which is useful for running ensemble analyses with [GerryChain](https://github.com/mggg/gerrychain). All views are immutable (with the exception of some auxiliary metadata); this constraint is intended to encourage reproducible analysis.

This bundling of geospatial data, tabular data, graph data, and metadata is inspired by our experience with storing these artifacts separately. In the midst of a time-sensitive litigation project or ahead of a conference deadline, it is easy to lose track of where a shapefile came from or how an adjacency graph was derived from a shapefile. This drift toward entropy is particularly problematic for a long-term, multi-person project, as corrections to a dataset distributed as a shapefile or the like must be propagated carefully to avoid inconsistent results. A version control tool like Git might seem like an obvious way to mitigate drift, but these tools are poorly suited to large binary files. It is far more tenable to store a _definition_ of a dataset in source control. This is the approach GerryDB takes: analysts define views in code by combining _view templates_, _geographic layers_, and _localities_. These view definitions are then instantiated as large datasets stored outside of source control.


### Localities and geographic layers
We consider districting problems with respect to a particular place: we might be interested in Massachusetts' State Senate districts or Chicago's city council wards. However, the precise boundaries of these places can shift over time: cities sometimes [annex unincorporated land](https://en.wikipedia.org/wiki/Municipal_annexation_in_the_United_States), for instance. For this reason, it is useful to have an abstract definition of a place that is not tied to a fixed boundary. In GerryDB, these abstract definitions are called _localities_. At a minimum, GerryDB contains locality definitions for all states, territories, and county or county equivalents referenced in the U.S. Census 2010 and 2020 county shapefiles.

Geographic layers are collections of geographic units that make localities concrete. For instance, GerryDB contains layer definitions from the U.S. Census central spine, which is a sequence of nesting geographic units. (In order from most to least granular, the units on the central spine are blocks, block groups, tracts, and counties.) We can associate units in a layer with a locality. For instance, we might build the city of Boston out of 2010 Census tracts, 2020 Census blocks, or 2022 city precincts. The city boundary induced by these units need not line up exactly, but each set of units approximates some Platonic ideal of Boston.


### Columns, column sets, and view templates
Geospatial redistricting data is typically bundled with tabular data; for instance, every U.S. Census block has associated columns of population statistics, and every voting precinct has associated columns of election results. A typical redistricting analysis combines multiple kinds of tabular data. U.S. Census data is released with a uniform schema across all states and territories, so we strive to maintain this uniformity as much as possible across units and localities. This is enabled by _view templates_, which are simply reusable collections of columns with some additional metadata.  For convenience, columns can be further grouped in _column sets_ that can be reused in multiple view templates.

### Graphs
The Markov chain methods implemented in GerryChain and similar packages rely on _dual graphs_ that encode adjacencies between geographic units in a locality. GerryDB supports associating graphs with views for ease of use with GerryChain.

### Districting plans
GerryDB supports storing districting plans, which are assignments of units within a geographic layer to districts. All public districting plans compatible with a view's locality and layer are automatically attached to the view on creation.

### Namespaces
Most data in GerryDB exists within a [_namespace_](https://en.wikipedia.org/wiki/Namespace). Namespaces are primarily useful for managing permissions: sensitive or restrictively licensed data such as [incumbent addresses](https://redistrictingdatahub.org/data/about-our-data/incumbent-address/) should always be stored in a private namespace. Namespaces also allow the reuse of naming schemes across data vintages. For instance, the `total_pop` column and the `block` layer are defined similarly in the `census.2010` and `census.2020` namespaces.

## Python client

GerryDB is primarily exposed to end users by a Python client library. This library communicates via a REST API with a [PostGIS](https://postgis.net/)-based server; the client library is also responsible for caching view data to performance an avoid excess calls to the server. Finally, the client library converts between GerryDB's internal format for rendered view data and more common formats: it supports loading view data as GeoPandas `GeoDataFrame` objects, [NetworkX](https://networkx.org/) graphs, and GerryChain `Partition` objects.

### Installing and configuring the client
By default, GerryDB caches view data in the `.gerrydb` directory in your home directory. The `config` file in this directory contains API credentials. A minimally viable `config` looks like

```toml
[default]
host = "cherrydb-meta-prod-7bvdnucjva-uk.a.run.app"
key = "<YOUR API KEY HERE>"
```

### Viewing views

To load a view, create a GerryDB client object and index into the `views` repository.

```python
from gerrydb import GerryDB

db = GerryDB()
view = db.views["/your_namespace/your_view"]
```

For convenience, GerryDB clients can be configured with a default namespace.
```python
from gerrydb import GerryDB

db = GerryDB(namespace="your_namespace")
view = db.views["your_view"]
```

The `to_df()` method converts a view to a `GeoDataFrame`.

```python
view = db.views["/your_namespace/your_view"]
view_df = view.to_df()  # returns `geopandas.GeoDataFrame`
```

Similarly, the `to_graph()` method converts a view to a NetworkX graph. (An adjacency graph must be associated with the view.)

```python
view = db.views["/your_namespace/your_view"]
view_graph = view.to_graph()  # returns `networkx.Graph`
```

The `to_chain()` method returns a mapping from districting plan names to GerryChain `Partition` objects. (An adjacency graph must be associated with the view.)

```python
view = db.views["/your_namespace/your_view"]
partitions = view.to_chain()  # returns dict[str, gerrychain.Partition]
```

The `.template`, `.layer`, `.locality`, and `.meta` attributes expose a view's metadata. For instance, we can pretty-print the names and descriptions of the columns associated with a view.

```python
from gerrydb.schemas import Column

view = db.views["/your_namespace/your_view"]

for member in bg_view.template.members:
    if isinstance(member, Column)
        print(member.canonical_path, "-", member.description, f"({member.kind}, {member.type})")
```


### Creating objects
Writing data to CherryDB requires a _write context_. Write contexts allow us to keep track of who added what data to GerryDB, why it was added, and where it came from. The Python client library supports creating a write context with a context manager. The following snippet illustrates the creation of a basic view consisting of 2020 block groups in Massachusetts.

```python
from gerrydb import GerryDB

db = GerryDB(namespace="census.2020")

with db.context(notes="Creating MA blocks group view for demo") as ctx:
    view = ctx.views.create(
        path="ma_bg_demo",
        template="basic_pops",
        locality="massachusetts",
        layer="bg",
        graph="ma_bg_rook",
    )
```

Other objects, such as localities and geographic layers, can be created through a similar interface. A full list of object repositories is available in the inline documentation for the `GerryDB` client class, and many examples of object creation are provided in the `tests/repos` directory.


### Importing large datasets (advanced)
GerryDB's REST API is optimized for data integrity and read performance; bulk data ingestion through this API can be slow and occasionally unreliable due to the inherent properties of large HTTP requests. For loading core datasets in bulk, we use a specialized write context that loads data directly into the PostGIS database in large transactions. For more details, see [gerrydb-etl](https://github.com/mggg/gerrydb-etl).