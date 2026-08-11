# graphdb/application/adapter_registry.py
from __future__ import annotations
from typing import Dict, Optional
from graphdb.domain.config import GraphDBConfig
from graphdb.domain.connection import ConnectionParams
from graphdb.infrastructure.environment_adapter import EnvironmentAdapter

# Class AdapterRegistry is responsible for managing EnvironmentAdapter instances
# for each configured environment in the GraphDB application. It initializes the
# adapters based on the provided configuration and allows retrieval of adapters
# by environment name.
class AdapterRegistry:
    """
    Holds EnvironmentAdapter instances per configured environment.
    Replaces the singleton engine state previously held by GraphDB.
    """

    # The registry is initialized with a GraphDBConfig object, which contains the configuration for all environments.
    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config
        self._adapters: Dict[str, EnvironmentAdapter] = {}
        for env_name, env_config in config.environments.items():
            params = ConnectionParams.from_environment(
                env_name, env_config, {"client_bin": config.client_bin, "dump_bin": config.dump_bin}
            )
            self._adapters[env_name] = EnvironmentAdapter(params, env_name)

    # The get method retrieves the EnvironmentAdapter for a given environment name.
    # If no name is provided, it defaults to the configured default environment.
    # If the requested environment is not found, it raises a ValueError with a list of available environments.
    def get(self, env_name: Optional[str] = None) -> EnvironmentAdapter:
        name = env_name or self.config.default_env
        if name not in self._adapters:
            available = ", ".join(sorted(self._adapters.keys()))
            raise ValueError(
                f"Environment '{name}' not configured. Available: [{available}]"
            )
        return self._adapters[name]

    # The names method returns a list of all configured environment names.
    def names(self):
        return list(self._adapters.keys())
