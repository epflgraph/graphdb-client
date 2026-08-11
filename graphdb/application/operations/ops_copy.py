from __future__ import annotations

import tempfile
from pathlib import Path
from loguru import logger as sysmsg

from graphdb.application.operations.ops_export import ExportOperations
from graphdb.application.operations.ops_import import ImportOperations


class CopyOperations:
    """Use case orchestrator for copying tables and databases across environments."""

    def __init__(
        self,
        export_ops: ExportOperations,
        import_ops: ImportOperations,
    ) -> None:
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
                source_env=source_env,
                source_schema=source_schema,
                table_name=table_name,
                output_dir=tmpdir,
                filter_by=filter_by,
                chunk_size=chunk_size,
                include_create_tables=True,
                compress=compress,
            )
            self.import_ops.import_table(
                target_env=target_env,
                target_schema=target_schema,
                dump_path=str(Path(tmpdir) / source_schema / table_name),
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
                source_env=source_env,
                source_schema=source_schema,
                output_dir=tmpdir,
                filter_by=filter_by,
                chunk_size=chunk_size,
                include_create_tables=True,
                compress=compress,
            )
            self.import_ops.import_database(
                target_env=target_env,
                target_schema=target_schema,
                dump_path=str(Path(tmpdir) / source_schema),
                create_keys_after_import=create_keys_after_import,
                compress=compress,
            )
