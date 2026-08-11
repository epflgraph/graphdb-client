from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from graphdb.domain.connection import ConnectionParams
from graphdb.domain.ports.database_port import DatabasePort
from graphdb.domain.ports.dump_port import DumpPort
from graphdb.domain.ports.filesystem_port import FilesystemPort
from graphdb.domain.ports.schema_port import SchemaPort
from graphdb.infrastructure.filesystem.local_filesystem import LocalFilesystem
from graphdb.infrastructure.mysql_cli.mysql_client import MySQLClient
from graphdb.infrastructure.mysql_cli.mysqldump_client import MySQLDumpClient
from graphdb.infrastructure.sqlalchemy.engine_factory import create_sqlalchemy_engine
from graphdb.infrastructure.sqlalchemy.query_executor import SQLAlchemyQueryExecutor
from graphdb.infrastructure.sqlalchemy.schema_introspector import SQLAlchemySchemaIntrospector


class EnvironmentAdapter(DatabasePort, SchemaPort, DumpPort):
    """
    Per-environment adapter composing SQLAlchemy, MySQL CLI, and filesystem
    capabilities. This replaces the per-environment state previously held by
    the GraphDB singleton.
    """

    def __init__(
        self,
        params: ConnectionParams,
        env_name: str = "default",
        filesystem: Optional[FilesystemPort] = None,
    ) -> None:
        self.params = params
        self.env_name = env_name
        self._engine = create_sqlalchemy_engine(params)
        self._query_executor = SQLAlchemyQueryExecutor(self._engine, env_name)
        self._schema = SQLAlchemySchemaIntrospector(self._engine, env_name)
        self._mysql_client = MySQLClient(params, env_name)
        self._dump_client = MySQLDumpClient(params, env_name)
        self._filesystem = filesystem or LocalFilesystem()

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

    # DatabasePort implementation
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

    # SchemaPort implementation (delegate)
    def database_exists(self, schema_name: str) -> bool:
        return self._schema.database_exists(schema_name)

    def create_database(self, schema_name: str, drop_existing: bool = False) -> None:
        self._schema.create_database(schema_name, drop_existing=drop_existing)

    def drop_database(self, schema_name: str) -> None:
        self._schema.drop_database(schema_name)

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        return self._schema.table_exists(schema_name, table_name, exclude_views=exclude_views)

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

    def get_keys(self, schema_name: str, table_name: str) -> List[dict]:
        return self._schema.get_keys(schema_name, table_name)

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

    # DumpPort implementation
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
    def filesystem(self) -> FilesystemPort:
        return self._filesystem
