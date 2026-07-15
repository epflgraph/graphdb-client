from __future__ import annotations

from typing import Dict, List

from graphdb.application.adapter_registry import AdapterRegistry


class ConnectivityService:
    """Application service for testing database connectivity."""

    def __init__(self, registry: AdapterRegistry) -> None:
        self.registry = registry

    def test_one(self, env_name: str) -> bool:
        return self.registry.get(env_name).test()

    def test_all(self) -> Dict[str, bool]:
        return {name: self.registry.get(name).test() for name in self.registry.names()}
