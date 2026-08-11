from __future__ import annotations

import tempfile
from pathlib import Path
from loguru import logger as sysmsg

from graphdb.application.operations.ops_export import ExportOperations
from graphdb.application.operations.ops_import import ImportOperations
from graphdb.adapters.environments import Environments


class CopyOperations:
    """Use case orchestrator for copying tables and databases across environments."""

    def __init__(
        self,
        export_ops: ExportOperations | None = None,
        import_ops: ImportOperations | None = None,
        registry: Environments | None = None,
    ) -> None:
        if export_ops is None or import_ops is None:
            if registry is None:
                raise ValueError("CopyOperations requires either (export_ops, import_ops) or registry")
            export_ops = export_ops or ExportOperations(registry)
            import_ops = import_ops or ImportOperations(registry)
        self.export_ops = export_ops
        self.import_ops = import_ops

    def copy_table(
        self,
        source_env: str,
        source_schema: str,
        target_env: str,
        target_schema: str,
        table_name: str,
        filter_by: str = "TRUE",
        chunk_size: int = 1_000_000,
        create_keys_after_import: bool = False,
        compress: bool = False,
    ) -> None:
        sysmsg.info(f"Copying table '{source_schema}.{table_name}' ({source_env}) -> '{target_schema}.{table_name}' ({target_env})")

        with tempfile.TemporaryDirectory() as tmpdir:
            self.export_ops.export_table(
                env_name=source_env,
                schema_name=source_schema,
                table_name=table_name,
                output_folder=tmpdir,
                filter_by=filter_by,
                chunk_size=chunk_size,
                include_create_tables=True,
                compress=compress,
            )
            self.import_ops.import_table(
                env_name=target_env,
                schema_name=target_schema,
                input_folder=str(Path(tmpdir) / source_schema / table_name),
                create_keys_after_import=create_keys_after_import,
                compress=compress,
            )

    def copy_database(
        self,
        source_env: str,
        source_schema: str,
        target_env: str,
        target_schema: str,
        filter_by: str = "TRUE",
        chunk_size: int = 1_000_000,
        create_keys_after_import: bool = False,
        compress: bool = False,
    ) -> None:
        sysmsg.info(f"Copying database '{source_schema}' ({source_env}) -> '{target_schema}' ({target_env})")

        with tempfile.TemporaryDirectory() as tmpdir:
            self.export_ops.export_database(
                env_name=source_env,
                schema_name=source_schema,
                output_folder=tmpdir,
                filter_by=filter_by,
                chunk_size=chunk_size,
                include_create_tables=True,
                compress=compress,
            )
            self.import_ops.import_database(
                env_name=target_env,
                schema_name=target_schema,
                input_folder=str(Path(tmpdir) / source_schema),
                create_keys_after_import=create_keys_after_import,
                compress=compress,
            )
