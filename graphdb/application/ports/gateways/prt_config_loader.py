from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Protocol
from graphdb.domain.config import GraphDBConfig


class ConfigLoaderPort(Protocol):
    """Abstract contract for loading configuration files from storage."""

    def load_raw(self, path: Path) -> Dict[str, Any]:
        ...

    def load_config(self, path: Path | str | None = None) -> GraphDBConfig:
        ...
