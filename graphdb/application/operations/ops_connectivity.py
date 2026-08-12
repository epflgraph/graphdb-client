from __future__ import annotations

from typing import Dict
from graphdb.adapters.environments import Environments


class ConnectivityOperations:
    """Use case orchestrator for testing database environment connectivity."""

    def __init__(self, registry: Environments) -> None:
        self.registry = registry

    def test_one(self, env_name: str) -> bool:
        """Tests database connectivity for a single named environment."""
        adapter = self.registry.get(env_name)
        return adapter.query_executor.test()

    def test_all(self) -> Dict[str, bool]:
        """Tests database connectivity across all configured environments."""
        return {
            name: self.registry.get(name).query_executor.test()
            for name in self.registry.names()
        }
