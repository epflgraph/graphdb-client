# graphdb/application/config_service.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
from yaml import safe_load
from graphdb.domain.config import GraphDBConfig

# Class ConfigService is responsible for loading and presenting configuration data for the GraphDB application.
# It provides methods to load configuration from default or specified files, as well as a method to redact
# sensitive information from the configuration data.
class ConfigService:
    """Application service for loading and presenting configuration."""

    # The constructor initializes the ConfigService with a GraphDBConfig object, which contains the
    # configuration data.
    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config

    # Method: Creates a ConfigService instance by loading the configuration from the default file.
    # It uses the GraphDBConfig class method from_default_file to load the configuration.
    @classmethod
    def from_default_file(cls) -> "ConfigService":
        return cls(GraphDBConfig.from_default_file())

    # Method: Creates a ConfigService instance by loading the configuration from a specified file path.
    # It uses the GraphDBConfig class method from_file to load the configuration.
    @classmethod
    def from_file(cls, path: Path | str) -> "ConfigService":
        return cls(GraphDBConfig.from_file(path))

    # Method: Loads raw configuration data from a specified file path.
    # It reads the file, parses the YAML content, and returns it as a dictionary.
    def load_raw(self, path: Path) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return safe_load(f) or {}

    # Method: Redacts sensitive information from the provided data structure.
    # It recursively traverses dictionaries and lists, replacing values of keys that contain "password"
    # with "***REDACTED***". This helps to prevent sensitive information from being exposed in logs or outputs.
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
