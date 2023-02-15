"""Generic CherryDB API object wrappers."""
from dataclasses import dataclass

from cherrydb.client import CherryDB


@dataclass(frozen=True)
class ObjectRepository:
    """A repository for a generic CherryDB object."""
    session: CherryDB