"""CherryDB session management."""
import os
import tomlkit
from pathlib import Path
from typing import Optional

from cherrydb.cache import CherryCache

DEFAULT_CHERRY_ROOT = Path(os.path.expanduser("~")) / ".cherry"


class CherryConfigError(Exception):
    """Raised when a CherryDB session configuration is invalid."""


class CherryDB:
    """CherryDB session."""

    host: str
    key: str
    cache: CherryCache
    namespace: Optional[str]
    offline: bool

    def __init__(
        self,
        profile: Optional[str] = "default",
        host: Optional[str] = None,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        offline: bool = False,
    ):
        """Creates a CherryDB session.

        If `host` and `key` are specified, an ephemeral session is created
        with an in-memory cache. Otherwise, session configuration is loaded
        for `profile` from the configuration in the directory specified by
        the `CHERRYDB_ROOT` environment variable. If this variable is not
        available, `~/.cherry` is used.

        If `namespace` is specified, object references without a namespace
        will implicitly refer to `namespace`.

        If `offline` is true, cached results from the API are accessible
        in a limited read-only mode; CherryDB will not attempt to fetch
        the latest versions of versioned objects. This mode is suitable
        for isolated use cases where a CherryDB server is not necessarily
        accessible--for instance, within code in a replication repository
        for a scientific paper.

        Raises:
            CherryConfigError:
                If the configuration is invalid--for instance, if only
                one of `host` and `key` are specified, or a CherryDB
                directory cannot be found.
        """
        self.namespace = namespace
        self.offline = offline

        if host is not None and key is None:
            raise CherryConfigError('No API key specified for host "{host}".')
        if host is None and key is not None:
            raise CherryConfigError("No host specified for API key.")

        if host is not None and key is not None:
            self.host = host
            self.key = key
            self.cache = CherryCache(":memory:")
            return

        cherry_root = Path(os.getenv("CHERRY_ROOT", DEFAULT_CHERRY_ROOT))
        try:
            with open(cherry_root / "config", encoding="utf-8") as config_fp:
                cherry_config_raw = config_fp.read()
        except IOError as ex:
            raise CherryConfigError(
                f"Failed to read CherryDB configuration at {cherry_root.resolve()}. "
                "Does a CherryDB configuration directory exist?"
            ) from ex

        try:
            configs = tomlkit.parse(cherry_config_raw)
        except tomlkit.exceptions.TOMLKitError as ex:
            raise CherryConfigError(
                f"Failed to parse CherryDB configuration at {cherry_root.resolve()}."
            ) from ex

        try:
            config = configs[profile]
        except KeyError:
            raise CherryConfigError(
                f'Profile "{profile}" not found in configuration '
                f"at {cherry_root.resolve()}."
            )

        for field in ("host", "key"):
            if field not in config:
                raise CherryConfigError(
                    f'Field "{field}" not in profile "{profile}" '
                    f"in configuration at {cherry_root.resolve()}."
                )

        self.host = config["host"]
        self.key = config["key"]
        self.cache = CherryCache(cherry_root / "caches" / f"{profile}.db")

    @property
    def base_url(self) -> str:
        """Base URL of the CherryDB API."""
        return f"https://{self.host}/api/v1"
