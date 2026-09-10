from __future__ import annotations

from typing import Dict, List, Optional

from graphdb.adapters.gateways.gtw_environment import EnvironmentGateway
from graphdb.domain.models.mdl_config import GraphDBConfig, GraphDBConfigError
from graphdb.domain.models.mdl_connection import ConnectionParams


class Environments:
    """Lazy collection of EnvironmentGateway instances per configured environment.

    This replaces the legacy GraphDB singleton as the top-level adapter
    composition point. CLI commands and application operations hold an
    Environments instance and retrieve EnvironmentGateway objects from it.

    Gateways are built on first access so that constructing Environments does
    not eagerly open database connections for environments that may never be
    used.
    """

    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config
        self._gateways: Dict[str, EnvironmentGateway] = {}

    def _build_gateway(self, env_name: str) -> EnvironmentGateway:
        """Construct the EnvironmentGateway for a configured environment."""
        env_config = self.config.environments[env_name]
        params = ConnectionParams.from_environment(
            env_name, env_config, {"client_bin": self.config.client_bin, "dump_bin": self.config.dump_bin}
        )
        return EnvironmentGateway(params, env_name)

    def get(self, env_name: Optional[str] = None) -> EnvironmentGateway:
        """Return the EnvironmentGateway for the requested environment."""
        name = env_name or self.config.default_env
        if name not in self.config.environments:
            available = ", ".join(sorted(self.config.environments.keys()))
            raise GraphDBConfigError(
                f"Environment '{name}' not configured. Available: [{available}]"
            )

        if name not in self._gateways:
            self._gateways[name] = self._build_gateway(name)
        return self._gateways[name]

    def names(self) -> List[str]:
        """Return the names of all configured environments."""
        return list(self.config.environments.keys())

    def default(self) -> EnvironmentGateway:
        """Return the EnvironmentGateway for the default environment."""
        return self.get(self.config.default_env)

    def __contains__(self, env_name: str) -> bool:
        """Check if an environment name is configured."""
        return env_name in self.config.environments
