from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from yaml import safe_load

from graphdb.domain.config import GraphDBConfig


class ConfigService:
    """Application service for loading and presenting configuration."""

    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config

    @classmethod
    def from_default_file(cls) -> "ConfigService":
        return cls(GraphDBConfig.from_default_file())

    @classmethod
    def from_file(cls, path: Path | str) -> "ConfigService":
        return cls(GraphDBConfig.from_file(path))

    def load_raw(self, path: Path) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return safe_load(f) or {}

    @staticmethod
    def redact(data: Any) -> Any:
        if isinstance(data, dict):
            return {
                key: ("***REDACTED***" if "password" in key.lower() else ConfigService.redact(value))
                for key, value in data.items()
            }
        if isinstance(data, list):
            return [ConfigService.redact(item) for item in data]
        return data
