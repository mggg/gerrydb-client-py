"""CherryDB client exceptions."""


class CherryError(Exception):
    """Abstract CherryDB error."""


class ConfigError(CherryError):
    """Raised when a CherryDB session configuration is invalid."""


class ClientError(CherryError):
    """A generic CherryDB client error."""


class RequestError(ClientError):
    """Raised when a query cannot be translated to an API call."""


class ResultError(ClientError):
    """Raised when a query result cannot be loaded."""


class OnlineError(ClientError):
    """Raised when an operation cannot be performed offline."""


class WriteContextError(CherryError):
    """Raised when an operation requires a write context."""


class CacheError(CherryError):
    """Raised for generic caching errors."""


class CacheInitError(CacheError):
    """Raised when a CherryDB cache cannot be initialized."""


class CacheObjectError(CacheError):
    """Raised when a schema has not been registered with the cache."""


class CachePolicyError(CacheError):
    """Raised when an cache operation does not match an object's cache policy."""
