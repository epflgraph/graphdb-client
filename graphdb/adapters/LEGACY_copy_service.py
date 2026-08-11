from __future__ import annotations

import tempfile
from pathlib import Path

from loguru import logger as sysmsg

from graphdb.application.adapter_registry import AdapterRegistry
from graphdb.application.export_service import ExportService
from graphdb.application.import_service import ImportService


class CopyService:
    """Application service for copying tables and databases across environments."""

    def __init__(self, registry: AdapterRegistry) -> None:
        self.registry = registry
        self.export_service = ExportService(registry)
        self.import_service = ImportService(registry)

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
        with tempfile.TemporaryDirectory() as tmpdir:
            self.export_service.export_table(
                source_env,
                source_schema,
                table_name,
                tmpdir,
                filter_by=filter_by,
                chunk_size=chunk_size,
                include_create_tables=True,
                compress=compress,
            )
            self.import_service.import_table(
                target_env,
                target_schema,
                str(Path(tmpdir) / source_schema / table_name),
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
        with tempfile.TemporaryDirectory() as tmpdir:
            self.export_service.export_database(
                source_env,
                source_schema,
                tmpdir,
                filter_by=filter_by,
                chunk_size=chunk_size,
                include_create_tables=True,
                compress=compress,
            )
            self.import_service.import_database(
                target_env,
                target_schema,
                str(Path(tmpdir) / source_schema),
                create_keys_after_import=create_keys_after_import,
                compress=compress,
            )
