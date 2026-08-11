from __future__ import annotations

import os
from pathlib import Path
from loguru import logger as sysmsg
from tqdm import tqdm

from graphdb.application.policies.pol_ddl_import import DDLImportPolicy
from graphdb.application.ports.gateways.prt_adapter_registry import AdapterRegistryPort
from graphdb.application.ports.gateways.prt_import_restore import ImportRestorePort


class ImportOperations:
    """Use case orchestrator for importing schemas and data from the filesystem."""

    def __init__(self, registry: AdapterRegistryPort) -> None:
        self.registry = registry
        self.ddl_policy = DDLImportPolicy()

    def import_create_table(
        self,
        env_name: str,
        schema_name: str,
        input_folder: str,
        include_keys: bool = True,
        ignore_existing: bool = False,
        verbose: bool = False,
    ) -> None:
        adapter: ImportRestorePort = self.registry.get(env_name)
        table_name = os.path.basename(input_folder)

        if not adapter.database_exists(schema_name):
            adapter.create_database(schema_name)

        if not ignore_existing and adapter.table_exists(schema_name, table_name):
            sysmsg.warning(
                f"Table {schema_name}.{table_name} already exists. "
                f"Flag 'ignore_existing' set to {ignore_existing}."
            )
            sysmsg.warning("Table definition not imported.")
            return

        file_name = "CREATE_TABLE.sql" if include_keys else "CREATE_TABLE_NO_KEYS.sql"
        file_path = Path(input_folder) / file_name

        if ignore_existing:
            content = adapter.filesystem.read_text(file_path)
            updated_content = self.ddl_policy.ensure_if_not_exists(content)
            if updated_content != content:
                adapter.filesystem.write_text(file_path, updated_content)

        adapter.execute_from_file(str(file_path), database=schema_name)

    def import_table_data(
        self,
        env_name: str,
        schema_name: str,
        input_folder: str,
        ignore_existing: bool = False,
        verbose: bool = False,
        compress: bool = False,
    ) -> None:
        adapter: ImportRestorePort = self.registry.get(env_name)
        table_folder = Path(input_folder)

        all_sql_files = adapter.filesystem.list_sql_files(table_folder, compress=compress)
        data_sql_files = self.ddl_policy.filter_data_sql_files(all_sql_files)

        for file_path in tqdm(data_sql_files, unit="offset"):
            table_name = file_path.parent.name
            self._execute_data_file(adapter, schema_name, file_path, table_name, ignore_existing)

    def import_table_keys(
        self,
        env_name: str,
        schema_name: str,
        input_folder: str,
        verbose: bool = False,
    ) -> None:
        adapter: ImportRestorePort = self.registry.get(env_name)
        keys_file = Path(input_folder) / "CREATE_KEYS.sql"
        if not adapter.filesystem.exists(keys_file):
            return

        content = adapter.filesystem.read_text(keys_file)
        parsed_keys = self.ddl_policy.parse_key_statements(content)
        table_name = os.path.basename(input_folder)

        for key_name, key_chunk in parsed_keys:
            if key_name and adapter.key_exists(schema_name, table_name, key_name):
                continue
            adapter.execute_in_shell(
                f"ALTER TABLE `{schema_name}`.`{table_name}` ADD {key_chunk};",
                database=schema_name,
            )

    def import_table(
        self,
        env_name: str,
        schema_name: str,
        input_folder: str,
        create_keys_after_import: bool = False,
        ignore_existing: bool = False,
        verbose: bool = False,
        compress: bool = False,
    ) -> None:
        self.import_create_table(
            env_name,
            schema_name,
            input_folder,
            include_keys=not create_keys_after_import,
            ignore_existing=ignore_existing,
            verbose=verbose,
        )
        self.import_table_data(
            env_name,
            schema_name,
            input_folder,
            ignore_existing=ignore_existing,
            verbose=verbose,
            compress=compress,
        )
        if create_keys_after_import:
            self.import_table_keys(env_name, schema_name, input_folder, verbose=verbose)

    def import_database(
        self,
        env_name: str,
        schema_name: str,
        input_folder: str,
        create_keys_after_import: bool = False,
        ignore_existing: bool = False,
        verbose: bool = False,
        compress: bool = False,
    ) -> None:
        adapter: ImportRestorePort = self.registry.get(env_name)
        if not adapter.database_exists(schema_name):
            adapter.create_database(schema_name)

        table_folders = [
            Path(p)
            for p in sorted(os.listdir(input_folder))
            if (Path(input_folder) / p).is_dir()
        ]
        for table_folder in table_folders:
            self.import_table(
                env_name,
                schema_name,
                str(table_folder),
                create_keys_after_import=create_keys_after_import,
                ignore_existing=ignore_existing,
                verbose=verbose,
                compress=compress,
            )

    def _execute_data_file(
        self,
        adapter: ImportRestorePort,
        schema_name: str,
        file_path: Path,
        table_name: str,
        ignore_existing: bool,
    ) -> None:
        if ignore_existing:
            # Stream through sed to avoid loading large files into memory.
            adapter.execute_file_with_sed(
                str(file_path),
                database=schema_name,
                sed_pattern=r"s/^INSERT INTO /INSERT IGNORE INTO /",
            )
        else:
            adapter.execute_from_file(str(file_path), database=schema_name)
