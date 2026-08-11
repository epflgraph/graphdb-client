from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple, Union

from graphdb.adapters.gateways.gtw_dbclient import BaseDBClientGateway
from graphdb.adapters.gateways.gtw_filesystem import FilesystemGateway
from graphdb.adapters.gateways.gtw_mysqldump import MySQLDumpBinaryGateway
from graphdb.adapters.gateways.gtw_sqlalchemy import (
    SQLAlchemyQueryExecutorGateway,
    create_sqlalchemy_engine,
)
from graphdb.adapters.schema.sch_introspector import SQLAlchemySchemaIntrospector
from graphdb.domain.mdl_connection import ConnectionParams
from graphdb.domain.models.entities.mdl_table import Column, Key, Table, View


class EnvironmentGateway:
    """
    Per-environment adapter composing SQLAlchemy, MySQL CLI, and filesystem
    capabilities. This replaces the per-environment state previously held by
    the GraphDB singleton.
    """

    _METADATA_COLUMNS = [
        "table_schema", "table_name", "engine", "table_collation",
        "row_format", "table_rows", "data_length", "index_length",
        "total_bytes", "column_count", "nullable_columns",
        "columns_with_default", "index_count", "unique_index_count",
        "avg_row_length",
    ]

    _METADATA_SQL = """
        SELECT
            it.table_schema, it.table_name, it.engine, it.table_collation,
            it.row_format, it.table_rows, it.data_length, it.index_length,
            (it.data_length + it.index_length) AS total_bytes,
            (SELECT COUNT(*) FROM information_schema.columns c
             WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name) AS column_count,
            (SELECT SUM(c.is_nullable = 'YES') FROM information_schema.columns c
             WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name) AS nullable_columns,
            (SELECT SUM(c.column_default IS NOT NULL) FROM information_schema.columns c
             WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name) AS columns_with_default,
            (SELECT COUNT(DISTINCT s.index_name) FROM information_schema.statistics s
             WHERE s.table_schema = it.table_schema AND s.table_name = it.table_name) AS index_count,
            (SELECT COUNT(DISTINCT CASE WHEN s.non_unique = 0 THEN s.index_name END)
             FROM information_schema.statistics s
             WHERE s.table_schema = it.table_schema AND s.table_name = it.table_name) AS unique_index_count,
            it.avg_row_length
        FROM information_schema.tables it
        WHERE it.table_schema = '%s' AND it.table_name = '%s'
    """

    def __init__(
        self,
        params: ConnectionParams,
        env_name: str = "default",
        filesystem: Optional[Any] = None,
    ) -> None:
        self.params = params
        self.env_name = env_name
        self._engine = create_sqlalchemy_engine(params)
        self._query_executor = SQLAlchemyQueryExecutorGateway(self._engine, env_name)
        self._schema = SQLAlchemySchemaIntrospector(self._engine, env_name)
        self._mysql_client = BaseDBClientGateway(params, env_name)
        self._dump_client = MySQLDumpBinaryGateway(params, env_name)
        self._filesystem = filesystem or FilesystemGateway()

    @property
    def engine(self):
        return self._engine

    @property
    def base_command_mysql(self) -> list[str]:
        return self._mysql_client.base_command

    @property
    def base_command_mysqldump(self) -> list[str]:
        return self._dump_client.base_command

    @property
    def subprocess_env(self) -> Dict[str, str]:
        return self._mysql_client.env

    # Database operations
    def test(self) -> bool:
        return self._query_executor.test()

    def execute(
        self,
        query: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        commit: bool = False,
        return_exception: bool = False,
        query_id: Optional[str] = None,
    ) -> Union[List[Any], Tuple[str, str, Any]]:
        return self._query_executor.execute(
            query=query,
            schema_name=schema_name,
            params=params,
            commit=commit,
            return_exception=return_exception,
            query_id=query_id,
        )

    def execute_in_shell(
        self,
        query: str,
        database: Optional[str] = None,
        query_id: Optional[str] = None,
    ) -> None:
        self._mysql_client.execute_query(query, database=database, query_id=query_id)

    def execute_from_file(
        self,
        file_path: str,
        database: Optional[str] = None,
    ) -> None:
        self._mysql_client.execute_from_file(file_path, database=database)

    def execute_file_with_sed(
        self,
        file_path: str,
        database: Optional[str] = None,
        sed_pattern: str = r"s/^INSERT INTO /INSERT IGNORE INTO /",
    ) -> None:
        self._mysql_client.execute_file_with_sed(file_path, database=database, sed_pattern=sed_pattern)

    def execute_stream_to_file(
        self,
        query: str,
        output_file: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        fetch_size: int = 1000,
        query_id: Optional[str] = None,
    ) -> None:
        self._query_executor.execute_stream_to_file(
            query=query,
            output_file=output_file,
            schema_name=schema_name,
            params=params,
            fetch_size=fetch_size,
            query_id=query_id,
        )

    # Schema operations
    def database_exists(self, schema_name: str) -> bool:
        return self._schema.database_exists(schema_name)

    def create_database(self, schema_name: str, drop_existing: bool = False) -> None:
        self._schema.create_database(schema_name, drop_existing=drop_existing)

    def drop_database(self, schema_name: str) -> None:
        self._schema.drop_database(schema_name)

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        return self._schema.table_exists(schema_name, table_name, exclude_views=exclude_views)

    def fetch_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        rows = self.execute(self._METADATA_SQL % (schema_name, table_name), schema_name=schema_name)
        return self._first_row_as_dict(rows, columns=self._METADATA_COLUMNS) or {}

    def get_exact_count(self, schema_name: str, table_name: str) -> int:
        rows = self.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{table_name}`", schema_name=schema_name)
        return int(rows[0][0]) if rows else 0

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        return self._schema.column_exists(schema_name, table_name, column_name)

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        return self._schema.key_exists(schema_name, table_name, key_name)

    def get_tables(self, schema_name: str, include_views: bool = False) -> List[str]:
        return self._schema.get_tables(schema_name, include_views=include_views)

    def get_views(self, schema_name: str) -> List[str]:
        return self._schema.get_views(schema_name)

    def get_create_table(self, schema_name: str, table_name: str) -> str:
        return self._schema.get_create_table(schema_name, table_name)

    def get_create_view(self, schema_name: str, view_name: str) -> str:
        return self._schema.get_create_view(schema_name, view_name)

    def is_view(self, schema_name: str, name: str) -> bool:
        return self._schema.is_view(schema_name, name)

    def get_column_names(self, schema_name: str, table_name: str) -> List[str]:
        return self._schema.get_column_names(schema_name, table_name)

    def get_column_datatypes(self, schema_name: str, table_name: str) -> List[str]:
        return self._schema.get_column_datatypes(schema_name, table_name)

    def has_primary_key(self, schema_name: str, table_name: str) -> bool:
        return self._schema.has_primary_key(schema_name, table_name)

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        return self._schema.get_primary_keys(schema_name, table_name)

    def get_keys(self, schema_name: str, table_name: str) -> Dict[str, List[str]]:
        return self._schema.get_keys(schema_name, table_name)

    def get_columns(self, schema_name: str, table_name: str) -> List[Column]:
        return self._schema.get_columns(schema_name, table_name)

    def get_key_entities(self, schema_name: str, table_name: str) -> List[Key]:
        return self._schema.get_key_entities(schema_name, table_name)

    def describe_table(self, schema_name: str, table_name: str) -> Table:
        return self._schema.describe_table(schema_name, table_name)

    def describe_view(self, schema_name: str, view_name: str) -> View:
        return self._schema.describe_view(schema_name, view_name)

    def create_table_like(
        self,
        source_schema_name: str,
        source_table_name: str,
        target_schema_name: str,
        target_table_name: str,
        drop_table: bool = False,
        drop_keys: bool = False,
    ) -> None:
        self._schema.create_table_like(
            source_schema_name,
            source_table_name,
            target_schema_name,
            target_table_name,
            drop_table=drop_table,
            drop_keys=drop_keys,
        )

    def drop_table(self, schema_name: str, table_name: str) -> None:
        self._schema.drop_table(schema_name, table_name)

    def rename_table(
        self,
        schema_name: str,
        table_name: str,
        rename_to: str,
        replace_existing: bool = False,
        simulation_mode: bool = False,
    ) -> None:
        self._schema.rename_table(
            schema_name,
            table_name,
            rename_to,
            replace_existing=replace_existing,
            simulation_mode=simulation_mode,
        )

    def drop_keys(self, schema_name: str, table_name: str, ignore_keys: Optional[List[str]] = None) -> None:
        self._schema.drop_keys(schema_name, table_name, ignore_keys=ignore_keys)

    # Dump operations
    def dump_table_data(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str = "TRUE",
        compress: bool = False,
    ) -> None:
        self._dump_client.dump_table_data(schema_name, table_name, output_file, where=where, compress=compress)

    def dump_table_chunk(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str = "TRUE",
        chunk_column: str = "row_id",
        chunk_start: int = 0,
        chunk_end: int = 0,
        compress: bool = False,
    ) -> None:
        self._dump_client.dump_table_chunk(
            schema_name,
            table_name,
            output_file,
            where=where,
            chunk_column=chunk_column,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            compress=compress,
        )

    @property
    def filesystem(self):
        return self._filesystem

    @staticmethod
    def _first_row_as_dict(result: Any, *, columns: List[str]) -> Optional[Dict[str, Any]]:
        if not result or not isinstance(result, (list, tuple)):
            return None
        first = result[0]
        if isinstance(first, Mapping):
            return dict(first)
        if isinstance(first, Sequence) and not isinstance(first, (str, bytes, bytearray)):
            vals = list(first)
            return dict(zip(columns, vals)) if columns else {"__row__": vals}
        return None
