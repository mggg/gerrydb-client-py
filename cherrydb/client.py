"""CherryDB session management."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx
import tomlkit

from cherrydb.cache import CherryCache
from cherrydb.exceptions import ConfigError
from cherrydb.repos import LocalityRepo
from cherrydb.schemas import ObjectMeta, ObjectMetaCreate

DEFAULT_CHERRY_ROOT = Path(os.path.expanduser("~")) / ".cherry"


class CherryDB:
    """CherryDB session."""

    client: Optional[httpx.Client]
    cache: CherryCache
    namespace: Optional[str]
    offline: bool
    _base_url: str
    timeout: int

    def __init__(
        self,
        profile: Optional[str] = "default",
        host: Optional[str] = None,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        offline: bool = False,
        timeout: int = 60,
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
            ConfigError:
                If the configuration is invalid--for instance, if only
                one of `host` and `key` are specified, or a CherryDB
                directory cannot be found.
        """
        self.namespace = namespace
        self.offline = offline
        self.timeout = timeout

        if host is not None and key is None:
            raise ConfigError('No API key specified for host "{host}".')
        if host is None and key is not None:
            raise ConfigError("No host specified for API key.")

        if host is not None and key is not None:
            self.cache = CherryCache(":memory:")
        else:
            cherry_root = Path(os.getenv("CHERRY_ROOT", DEFAULT_CHERRY_ROOT))
            try:
                with open(cherry_root / "config", encoding="utf-8") as config_fp:
                    cherry_config_raw = config_fp.read()
            except IOError as ex:
                raise ConfigError(
                    "Failed to read CherryDB configuration at "
                    f"{cherry_root.resolve()}. "
                    "Does a CherryDB configuration directory exist?"
                ) from ex

            try:
                configs = tomlkit.parse(cherry_config_raw)
            except tomlkit.exceptions.TOMLKitError as ex:
                raise ConfigError(
                    "Failed to parse CherryDB configuration at "
                    f"{cherry_root.resolve()}."
                ) from ex

            try:
                config = configs[profile]
            except KeyError:
                raise ConfigError(
                    f'Profile "{profile}" not found in configuration '
                    f"at {cherry_root.resolve()}."
                )

            for field in ("host", "key"):
                if field not in config:
                    raise ConfigError(
                        f'Field "{field}" not in profile "{profile}" '
                        f"in configuration at {cherry_root.resolve()}."
                    )

            try:
                Path(cherry_root / "caches").mkdir(exist_ok=True)
            except IOError as ex:
                raise ConfigError("Failed to create cache directory.") from ex
            self.cache = CherryCache(cherry_root / "caches" / f"{profile}.db")

            host = config["host"]
            key = config["key"]

        self._base_url = (
            f"http://{host}/api/v1"
            if host.startswith("localhost")
            else f"https://{host}/api/v1"
        )
        self._base_headers = {"User-Agent": "cherrydb-client-py", "X-API-Key": key}
        self._transport = httpx.HTTPTransport(retries=1)

        self.client = httpx.Client(
            base_url=self._base_url,
            headers=self._base_headers,
            timeout=timeout,
            transport=self._transport,
        )

    def context(self, notes: str = "") -> "WriteContext":
        """Creates a write context with session-level metadata.

        Args:
            notes: Freeform notes to associate with the write session.

        Returns:
            A context manager for CherryDB writes.
        """
        return WriteContext(db=self, notes=notes)

    @property
    def localities(self):
        """Localities."""
        return LocalityRepo(session=self)


@dataclass
class WriteContext:
    """Context for a CherryDB transaction."""

    db: CherryDB
    notes: str
    meta: ObjectMeta | None = None
    client: httpx.Client | None = None

    def __enter__(self) -> "WriteContext":
        """Creates a write context with metadata."""
        response = self.db.client.post(
            "/meta/", json=ObjectMetaCreate(notes=self.notes).dict()
        )
        response.raise_for_status()  # TODO: refine?

        self.meta = ObjectMeta(**response.json())
        self.client = httpx.Client(
            base_url=self.db._base_url,
            headers={**self.db._base_headers, "X-Cherry-Meta-ID": str(self.meta.uuid)},
            timeout=self.db.timeout,
            transport=self.db._transport,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()

    @property
    def localities(self):
        """Localities."""
        return LocalityRepo(session=self.db, ctx=self)
