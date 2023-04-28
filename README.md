# GerryDB User Guide

## Background

### Who is this for?
This document is intended as a reference for anyone who wants to ...

### What is GerryDB?
GerryDB is a lightweight redistricting and voting analytics platform developed internally at MGGG.  We intend it to replace legacy redistricting GIS workflows ...

GerryDB is _not_ a general-purpose GIS platform like [ArcGIS](https://www.arcgis.com/index.html). It does not have data visualization tools, a sophisticated web interface, or a custom query language. Rather, GerryDB indexes and exposes geospatial, demographic, and voting data for the purpose of studying U.S. democracy. Analysts are free to use whatever tools they wish (we recommend [GeoPandas](https://geopandas.org/en/stable/), [QGIS](https://www.qgis.org/en/site/), and [SQLite](https://www.sqlite.org/index.html)) to read GerryDB views.


### What is in GerryDB?


## Core concepts

### Views
The most important GerryDB object is the **view**. A view is a collection of geographies (represented as polygons) with tabular attributes. In this way, a view is roughly analogous to a [shapefile](https://en.wikipedia.org/wiki/Shapefile) or a [GeoPackage](http://www.geopackage.org/). However, views also contain rich metadata that is difficult to store using these traditional file formats. Views contain descriptors for every geography and tabular column, allowing analysts to determine the precise meaning and lineage of the data they are using without needing to track down external documentation. A view also typically contains an adjacency graph (also known as a "dual graph") of its geographies, which is useful for running ensemble analyses with [GerryChain](https://github.com/mggg/gerrychain).

#### History and motivation
This bundling of geospatial data, tabular data, graph data, and metadata is inspired by our experience with storing these artifacts separately. In the midst of a time-sensitive litigation project or ahead of a conference deadline, it is easy to lose track of where a shapefile came from or how an adjacency graph was derived from a shapefile. This drift toward entropy is particularly problematic fora long-term, multi-person project, as corrections to a dataset distributed as a shapefile or the like must be propagated carefully to avoid inconsistent results. A version control tool like Git might seem like an obvious way to mitigate drift, but these tools are poorly suited to large binary files. (GitHub imposes a soft limit of 50 MB on Git objects, but Census block-level shapefiles often exceed this limit by an order of magnitude.) It is far more tenable to store a _definition_ of a dataset in source control. This is the approach GerryDB takes: analysts define views in code by combining _view templates_, _geographic layers_, and _localities_. These view definitions are then instantiated as large datasets stored outside of source control.

[more here: what should a view be?]

### Localities and geographic layers
We consider districting problems with respect to a particular place: we might be interested in Massachusetts' State Senate districts or Chicago's city council wards. However, the precise boundaries of these places can shift over time: cities sometimes [annex unincorporated land](https://en.wikipedia.org/wiki/Municipal_annexation_in_the_United_States), for instance. For this reason, it is useful to have an abstract definition of a place that is not tied to a fixed boundary. In GerryDB, these abstract definitions are called _localities_. At a minimum, GerryDB contains locality definitions for all states, territories, and county or county equivalents referenced in the U.S. Census 2010 and 2020 county shapefiles.

Geographic layers are collections of geographic units that make localities concrete. For instance, GerryDB contains layer definitions from the U.S. Census central spine, which is a sequence of nesting geographic units. (In order from most to least granular, the units on the central spine are blocks, block groups, tracts, and counties.) We can associate units in a layer with a locality. For instance, we might build the city of Boston out of 2010 Census tracts, 2020 Census blocks, or 2022 city precincts. The city boundary induced by these units need not line up exactly, but each set of units approximates some Platonic ideal Boston.


### Columns and column sets

For convenience, columns can be grouped in _column sets_

### View templates

### Graphs

### Districting plans

### Namespaces
Most data in GerryDB exists within a [_namespace_](https://en.wikipedia.org/wiki/Namespace). Namespaces are primarily useful for managing permissions: sensitive or restrictively licensed data such as [incumbent addresses](https://redistrictingdatahub.org/data/about-our-data/incumbent-address/) should always be stored in a private namespace. 

## Python client

GerryDB is primarily exposed to end users by a Python client library. This library communicates via a REST API with a [PostGIS](https://postgis.net/)-based server; the client library is also responsible for caching view data to performance an avoid excess calls to the server. Finally, the client library converts between GerryDB's internal format for rendered view data and more common formats: it supports loading view data as GeoPandas `GeoDataFrame` objects, [NetworkX](https://networkx.org/) graphs, and GerryChain `Partition` objects.

### Installing and configuring the client

### Viewing views

### Creating objects

### Importing large datasets (advanced)
