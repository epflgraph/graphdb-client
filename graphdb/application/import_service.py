from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from loguru import logger as sysmsg
from tqdm import tqdm

from graphdb.application.adapter_registry import AdapterRegistry


class ImportService:
    """Application service for importing schemas and data from the filesystem."""

    def __init__(self, registry: AdapterRegistry) -> None:
        self.registry = registry

    def import_create_table(
        self,
        env_name: str,
        schema_name: str,
        input_folder: str,
        include_keys: bool = True,
        ignore_existing: bool = False,
        verbose: bool = False,
    ) -> None:
        adapter = self.registry.get(env_name)
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
            if "IF NOT EXISTS" not in content:
                content = content.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                adapter.filesystem.write_text(file_path, content)

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
        adapter = self.registry.get(env_name)
        table_folder = Path(input_folder)

        sql_files = adapter.filesystem.list_sql_files(table_folder, compress=compress)
        excluded = {
            "CREATE_KEYS.sql",
            "CREATE_TABLE_NO_KEYS.sql",
            "CREATE_TABLE.sql",
            "CREATE_KEYS.sql.gz",
            "CREATE_TABLE_NO_KEYS.sql.gz",
            "CREATE_TABLE.sql.gz",
        }
        sql_files = [p for p in sql_files if p.name not in excluded]

        for file_path in tqdm(sql_files, unit="offset"):
            table_name = file_path.parent.name
            self._execute_data_file(adapter, schema_name, file_path, table_name, ignore_existing)

    def import_table_keys(
        self,
        env_name: str,
        schema_name: str,
        input_folder: str,
        verbose: bool = False,
    ) -> None:
        adapter = self.registry.get(env_name)
        keys_file = Path(input_folder) / "CREATE_KEYS.sql"
        if not adapter.filesystem.exists(keys_file):
            return

        content = adapter.filesystem.read_text(keys_file)
        statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]

        for statement in statements:
            match = re.search(r"ADD\s+(.*)", statement, re.IGNORECASE)
            if not match:
                continue
            key_chunk = match.group(1).strip()
            key_name_match = re.search(r"`(.*?)`", key_chunk)
            key_name = key_name_match.group(1) if key_name_match else None
            if key_name and adapter.key_exists(schema_name, os.path.basename(input_folder), key_name):
                continue
            table_name = os.path.basename(input_folder)
            adapter.execute_in_shell(f"ALTER TABLE `{schema_name}`.`{table_name}` ADD {key_chunk};", database=schema_name)

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
            env_name, schema_name, input_folder, include_keys=not create_keys_after_import, ignore_existing=ignore_existing, verbose=verbose
        )
        self.import_table_data(env_name, schema_name, input_folder, ignore_existing=ignore_existing, verbose=verbose, compress=compress)
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
        adapter = self.registry.get(env_name)
        if not adapter.database_exists(schema_name):
            adapter.create_database(schema_name)

        table_folders = [Path(p) for p in sorted(os.listdir(input_folder)) if (Path(input_folder) / p).is_dir()]
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

    def _execute_data_file(self, adapter, schema_name: str, file_path: Path, table_name: str, ignore_existing: bool) -> None:
        if ignore_existing:
            # Stream through sed to avoid loading large files into memory.
            adapter.execute_file_with_sed(
                str(file_path),
                database=schema_name,
                sed_pattern=r"s/^INSERT INTO /INSERT IGNORE INTO /",
            )
        else:
            adapter.execute_from_file(str(file_path), database=schema_name)
