from __future__ import annotations

from typing import Dict, List, Optional

from graphdb.application.ports.gateways.prt_adapter_registry import AdapterRegistryPort
from graphdb.application.ports.gateways.prt_table_metadata import TableMetadataPort
from graphdb.adapters.gateways.gtw_environment import EnvironmentGateway
from graphdb.domain.mdl_config import GraphDBConfig
from graphdb.domain.mdl_connection import ConnectionParams


class AdapterRegistry(AdapterRegistryPort):
    """Concrete registry managing EnvironmentGateway instances per configured environment."""

    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config
        self._adapters: Dict[str, EnvironmentGateway] = {}
        for env_name, env_config in config.environments.items():
            params = ConnectionParams.from_environment(
                env_name, env_config, {"client_bin": config.client_bin, "dump_bin": config.dump_bin}
            )
            self._adapters[env_name] = EnvironmentGateway(params, env_name)

    def get(self, env_name: Optional[str] = None) -> TableMetadataPort:
        name = env_name or self.config.default_env
        if name not in self._adapters:
            available = ", ".join(sorted(self._adapters.keys()))
            raise ValueError(f"Environment '{name}' not configured. Available: [{available}]")
        return self._adapters[name]

    def names(self) -> List[str]:
        return list(self._adapters.keys())
