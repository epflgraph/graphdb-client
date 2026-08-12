from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Protocol

from graphdb.domain.models.mdl_config import GraphDBConfig


class ConfigLoaderPort(Protocol):
    """Port interface implemented by matching adapter."""
    def load_raw(self, path: Path) -> Dict[str, Any]: ...
    def load_config(self, path: Path | str | None = None) -> GraphDBConfig: ...
