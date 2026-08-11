# graphdb/application/connectivity_service.py
from __future__ import annotations
from typing import Dict, List
from graphdb.application.adapter_registry import AdapterRegistry

# Class ConnectivityService is responsible for testing database connectivity for the GraphDB application.
# It provides methods to test connectivity for a specific environment or for all configured environments.
class ConnectivityService:
    """Application service for testing database connectivity."""

    # The constructor initializes the ConnectivityService with an AdapterRegistry instance, which manages
    # EnvironmentAdapter instances for each configured environment.
    def __init__(self, registry: AdapterRegistry) -> None:
        self.registry = registry

    # Method: Tests the database connectivity for a specific environment by retrieving the corresponding
    # EnvironmentAdapter from the registry and invoking its test method. It returns a boolean indicating
    # whether the connectivity test was successful.
    def test_one(self, env_name: str) -> bool:
        return self.registry.get(env_name).test()

    # Method: Tests the database connectivity for all configured environments by iterating through the
    # list of environment names in the registry and invoking the test method for each EnvironmentAdapter.
    # It returns a dictionary mapping each environment name to a boolean indicating the success of the
    # connectivity test.
    def test_all(self) -> Dict[str, bool]:
        return {name: self.registry.get(name).test() for name in self.registry.names()}
