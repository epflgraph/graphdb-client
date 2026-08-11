from __future__ import annotations

from pathlib import Path
from loguru import logger as sysmsg
from tqdm import tqdm

from graphdb.application.policies.pol_ddl import DDLExportPolicy
from graphdb.adapters.environments import Environments
from graphdb.adapters.gateways.gtw_environment import EnvironmentGateway

PBWIDTH = 64


class ExportOperations:
    """Use case orchestrator for exporting schemas and data to the filesystem."""

    def __init__(self, registry: Environments) -> None:
        self.registry = registry
        self.ddl_policy = DDLExportPolicy()

    def export_create_table(
        self,
        env_name: str,
        schema_name: str,
        table_name: str,
        output_folder: str,
    ) -> None:
        adapter: EnvironmentGateway = self.registry.get(env_name)
        table_folder = Path(output_folder) / schema_name / table_name
        table_folder.mkdir(parents=True, exist_ok=True)

        raw_sql = adapter.get_create_table(schema_name, table_name)
        full_sql, no_keys_sql, create_keys_sql = self.ddl_policy.split_table_keys(raw_sql, table_name)

        fs = adapter.filesystem
        fs.write_text(table_folder / "CREATE_TABLE.sql", full_sql)
        fs.write_text(table_folder / "CREATE_TABLE_NO_KEYS.sql", no_keys_sql)
        fs.write_text(table_folder / "CREATE_KEYS.sql", create_keys_sql)

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
        adapter: EnvironmentGateway = self.registry.get(env_name)
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
        adapter: EnvironmentGateway = self.registry.get(env_name)
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
