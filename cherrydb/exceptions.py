"""CherryDB client exceptions."""


class ConfigError(Exception):
    """Raised when a CherryDB session configuration is invalid."""


class ClientError(Exception):
    """A generic CherryDB client error."""


class RequestError(ClientError):
    """Raised when a query cannot be translated to an API call."""


class ResultError(ClientError):
    """Raised when a query result cannot be loaded."""


class OnlineError(ClientError):
    """Raised when an operation cannot be performed offline."""


class WriteContextError(Exception):
    """Raised when an operation requires a write context."""
