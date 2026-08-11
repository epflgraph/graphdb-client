from __future__ import annotations

from typing import Dict, List, Optional

from graphdb.adapters.gateways.gtw_environment import EnvironmentGateway
from graphdb.domain.mdl_config import GraphDBConfig
from graphdb.domain.mdl_connection import ConnectionParams


class Environments:
    """Collection of EnvironmentGateway instances per configured environment.

    This replaces the legacy GraphDB singleton as the top-level adapter
    composition point. CLI commands and application operations hold an
    Environments instance and retrieve EnvironmentGateway objects from it.
    """

    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config
        self._gateways: Dict[str, EnvironmentGateway] = {}
        for env_name, env_config in config.environments.items():
            params = ConnectionParams.from_environment(
                env_name, env_config, {"client_bin": config.client_bin, "dump_bin": config.dump_bin}
            )
            self._gateways[env_name] = EnvironmentGateway(params, env_name)

    def get(self, env_name: Optional[str] = None) -> EnvironmentGateway:
        """Return the EnvironmentGateway for the requested environment."""
        name = env_name or self.config.default_env
        if name not in self._gateways:
            available = ", ".join(sorted(self._gateways.keys()))
            raise ValueError(f"Environment '{name}' not configured. Available: [{available}]")
        return self._gateways[name]

    def names(self) -> List[str]:
        """Return the names of all configured environments."""
        return list(self._gateways.keys())

    def default(self) -> EnvironmentGateway:
        """Return the EnvironmentGateway for the default environment."""
        return self.get(self.config.default_env)


# Backward-compatible alias for code still referencing the old name.
Environments = Environments
