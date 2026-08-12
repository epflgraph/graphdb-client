from __future__ import annotations

from typing import Any, Dict, Optional

from graphdb.adapters.gateways.gtw_dbclient import BaseDBClientGateway
from graphdb.adapters.gateways.gtw_filesystem import FilesystemGateway
from graphdb.adapters.gateways.gtw_mysqldump import MySQLDumpBinaryGateway
from graphdb.adapters.gateways.gtw_sqlalchemy import (
    SQLAlchemyQueryExecutorGateway,
    create_sqlalchemy_engine,
)
from graphdb.adapters.schema.sch_column import ColumnSchemaAdapter
from graphdb.adapters.schema.sch_database import DatabaseSchemaAdapter
from graphdb.adapters.schema.sch_key import KeySchemaAdapter
from graphdb.adapters.schema.sch_table import TableSchemaAdapter
from graphdb.adapters.schema.sch_view import ViewSchemaAdapter
from graphdb.domain.models.mdl_connection import ConnectionParams


class EnvironmentGateway:
    """
    Per-environment adapter composing SQLAlchemy, MySQL CLI, and filesystem
    capabilities. This replaces the per-environment state previously held by
    the GraphDB singleton.
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
        self._database = DatabaseSchemaAdapter(self._engine)
        self._key = KeySchemaAdapter(self._engine, graphdb=self)
        self._table = TableSchemaAdapter(self._engine, key_adapter=self._key)
        self._view = ViewSchemaAdapter(self._engine)
        self._column = ColumnSchemaAdapter(self._engine, graphdb=self)
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

    @property
    def filesystem(self):
        return self._filesystem

    @property
    def database(self) -> DatabaseSchemaAdapter:
        return self._database

    @property
    def table(self) -> TableSchemaAdapter:
        return self._table

    @property
    def view(self) -> ViewSchemaAdapter:
        return self._view

    @property
    def column(self) -> ColumnSchemaAdapter:
        return self._column

    @property
    def key(self) -> KeySchemaAdapter:
        return self._key

    @property
    def query_executor(self) -> SQLAlchemyQueryExecutorGateway:
        return self._query_executor

    @property
    def mysql_client(self) -> BaseDBClientGateway:
        return self._mysql_client

    @property
    def dump_client(self) -> MySQLDumpBinaryGateway:
        return self._dump_client
