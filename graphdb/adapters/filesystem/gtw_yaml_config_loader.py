from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from yaml import safe_load

from graphdb.application.ports.gateways.prt_config_loader import ConfigLoaderPort
from graphdb.domain.config import GraphDBConfig


class YAMLConfigLoaderGateway(ConfigLoaderPort):
    """Concrete filesystem adapter for reading YAML configuration files."""

    def load_raw(self, path: Path) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return safe_load(f) or {}

    def load_config(self, path: Path | str | None = None) -> GraphDBConfig:
        if path is None:
            return GraphDBConfig.from_default_file()
        return GraphDBConfig.from_file(path)
