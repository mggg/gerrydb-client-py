"""Internal cache operations for CherryDB."""
from pathlib import Path


class CherryCache:
    """CherryDB caching layer."""

    def __init__(self, conn: str | Path):
        """Loads or initializes a cache."""
