# graphdb/entrypoints/cli/container.py
# Composition root for the CLI. Wires Environments into application operations.
from __future__ import annotations

from graphdb.adapters.environments import Environments
from graphdb.application.operations.ops_compare import CompareOperations
from graphdb.application.operations.ops_config import ConfigOperations
from graphdb.application.operations.ops_connectivity import ConnectivityOperations
from graphdb.application.operations.ops_copy import CopyOperations
from graphdb.application.operations.ops_export import ExportOperations
from graphdb.application.operations.ops_import import ImportOperations
from graphdb.domain.models.mdl_config import GraphDBConfig


class Container:
    """Composition root wiring concrete adapters to application operations."""

    def __init__(self, config: GraphDBConfig, environments: Environments | None = None) -> None:
        self.config = config
        self.environments = environments or Environments(config)

        # Application operations
        self.config_ops = ConfigOperations(config)
        self.connectivity_ops = ConnectivityOperations(self.environments)
        self.compare_ops = CompareOperations(self.environments)
        self.export_ops = ExportOperations(self.environments)
        self.import_ops = ImportOperations(self.environments)
        self.copy_ops = CopyOperations(
            export_ops=self.export_ops,
            import_ops=self.import_ops,
        )
