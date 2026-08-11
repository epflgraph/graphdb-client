from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from loguru import logger as sysmsg
from tqdm import tqdm

from graphdb.application.adapter_registry import AdapterRegistry
from graphdb.infrastructure.environment_adapter import EnvironmentAdapter


PBWIDTH = 64


class ExportService:
    """Application service for exporting schemas and data to the filesystem."""

    def __init__(self, registry: AdapterRegistry) -> None:
        self.registry = registry

    def export_create_table(
        self,
        env_name: str,
        schema_name: str,
        table_name: str,
        output_folder: str,
    ) -> None:
        adapter = self.registry.get(env_name)
        table_folder = Path(output_folder) / schema_name / table_name
        table_folder.mkdir(parents=True, exist_ok=True)

        create_table_sql = adapter.get_create_table(schema_name, table_name)
        create_table_sql = self._normalize_create_table(create_table_sql)

        keys_chunk = "\n".join(
            re.findall(r"(?m)^\s*(?!PRIMARY KEY)(?:UNIQUE KEY|KEY|INDEX|CONSTRAINT)\b.*$", create_table_sql)
        )
        no_keys_sql = create_table_sql.replace(keys_chunk, "").replace(",\n\n) ENGINE", "\n) ENGINE")

        create_keys_sql = ""
        for line in keys_chunk.split("\n"):
            if "UNIQUE KEY" in line or "KEY" in line or "INDEX" in line or "CONSTRAINT" in line:
                line = line.strip()
                if line.endswith(","):
                    line = line[:-1]
                create_keys_sql += f"ALTER TABLE `{table_name}` ADD {line};\n"

        fs = adapter.filesystem
        fs.write_text(table_folder / "CREATE_TABLE.sql", create_table_sql + ";\n")
        fs.write_text(table_folder / "CREATE_TABLE_NO_KEYS.sql", no_keys_sql + ";\n")
        fs.write_text(table_folder / "CREATE_KEYS.sql", create_keys_sql + "\n")

    def export_table_data(
        self,
        env_name: str,
        schema_name: str,
        table_name: str,
        output_folder: str,
        filter_by: str = "TRUE",
        chunk_size: int = 1_000_000,
        compress: bool = False,
    ) -> None:
        adapter = self.registry.get(env_name)
        table_folder = Path(output_folder) / schema_name / table_name
        table_folder.mkdir(parents=True, exist_ok=True)

        has_row_id = adapter.column_exists(schema_name, table_name, "row_id")

        if has_row_id:
            min_row_id = int(
                adapter.execute(
                    f"SELECT COALESCE(MIN(row_id),0) FROM `{schema_name}`.`{table_name}` WHERE {filter_by}"
                )[0][0]
            )
            max_row_id = int(
                adapter.execute(
                    f"SELECT COALESCE(MAX(row_id),0) FROM `{schema_name}`.`{table_name}` WHERE {filter_by}"
                )[0][0]
            )

            if min_row_id > max_row_id:
                sysmsg.warning(f"No rows found in table {schema_name}.{table_name} with filter '{filter_by}'.")
                return

            n_rows = max_row_id - min_row_id + 1
            for offset in tqdm(
                range(min_row_id - 1, max_row_id + 1, chunk_size),
                unit="offset",
                total=(n_rows // chunk_size) + 1,
            ):
                pb_desc = f"⚙️  Table: {table_name}".ljust(PBWIDTH)[:PBWIDTH]
                # tqdm description update omitted for brevity; set via set_description if needed

                suffix = ".sql.gz" if compress else ".sql"
                output_file = table_folder / f"{table_name}_{str(offset).zfill(10)}{suffix}"
                existing_plain = output_file.with_suffix("") if compress else None

                if output_file.exists() or (existing_plain and existing_plain.exists()):
                    continue

                adapter.dump_table_chunk(
                    schema_name=schema_name,
                    table_name=table_name,
                    output_file=str(output_file),
                    where=filter_by,
                    chunk_column="row_id",
                    chunk_start=offset,
                    chunk_end=offset + chunk_size - 1,
                    compress=compress,
                )
        else:
            suffix = "_FULL.sql.gz" if compress else "_FULL.sql"
            output_file = table_folder / f"{table_name}{suffix}"
            existing_plain = output_file.with_suffix("") if compress else None

            if output_file.exists() or (existing_plain and existing_plain.exists()):
                sysmsg.warning(f"Output file {output_file} already exists. Skipping dump for table '{table_name}'.")
                return

            adapter.dump_table_data(
                schema_name=schema_name,
                table_name=table_name,
                output_file=str(output_file),
                where=filter_by,
                compress=compress,
            )

    def export_table(
        self,
        env_name: str,
        schema_name: str,
        table_name: str,
        output_folder: str,
        filter_by: str = "TRUE",
        chunk_size: int = 1_000_000,
        include_create_tables: bool = False,
        compress: bool = False,
    ) -> None:
        if include_create_tables:
            self.export_create_table(env_name, schema_name, table_name, output_folder)
        self.export_table_data(env_name, schema_name, table_name, output_folder, filter_by, chunk_size, compress)

    def export_database(
        self,
        env_name: str,
        schema_name: str,
        output_folder: str,
        filter_by: str = "TRUE",
        chunk_size: int = 1_000_000,
        include_create_tables: bool = False,
        compress: bool = False,
    ) -> None:
        adapter = self.registry.get(env_name)
        for table_name in sorted(adapter.get_tables(schema_name)):
            self.export_table(
                env_name,
                schema_name,
                table_name,
                output_folder,
                filter_by=filter_by,
                chunk_size=chunk_size,
                include_create_tables=include_create_tables,
                compress=compress,
            )

    @staticmethod
    def _normalize_create_table(sql: str) -> str:
        sql = sql.replace(
            "`row_id` int NOT NULL AUTO_INCREMENT,",
            "`row_id` int NOT NULL AUTO_INCREMENT UNIQUE KEY,",
        )
        sql = sql.replace(
            "`row_id` int unsigned NOT NULL AUTO_INCREMENT,",
            "`row_id` int NOT NULL AUTO_INCREMENT UNIQUE KEY,",
        )
        sql = re.sub(r"AUTO_INCREMENT=\d+", "AUTO_INCREMENT=1", sql)
        return sql
