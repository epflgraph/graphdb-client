from graphdb.application.operations.ops_copy import CopyOperations
from graphdb.application.operations.ops_export import ExportOperations
from graphdb.application.operations.ops_import import ImportOperations
from graphdb.application.factories.fct_adapter_registry import Environments
from graphdb.domain.models.mdl_config import GraphDBConfig


class Container:
    """Composition Root."""

    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config
        self.registry = Environments(config)

        # Base operations
        self.export_ops = ExportOperations(registry=self.registry)
        self.import_ops = ImportOperations(registry=self.registry)

        # Composite operations
        self.copy_ops = CopyOperations(
            export_ops=self.export_ops,
            import_ops=self.import_ops,
        )
