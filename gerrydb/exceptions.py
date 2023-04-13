"""GerryDB client exceptions."""


class GerryDBError(Exception):
    """Abstract GerryDB error."""


class ConfigError(GerryDBError):
    """Raised when a GerryDB session configuration is invalid."""


class ClientError(GerryDBError):
    """A generic GerryDB client error."""


class OnlineError(ClientError):
    """Raised when an operation cannot be performed offline."""


class RequestError(ClientError):
    """Raised when a query cannot be translated to an API call."""


class ResultError(ClientError):
    """Raised when a query result cannot be loaded."""


class WriteContextError(GerryDBError):
    """Raised when an operation requires a write context."""


class CacheError(GerryDBError):
    """Raised for generic caching errors."""


class CacheInitError(CacheError):
    """Raised when a GerryDB cache cannot be initialized."""
