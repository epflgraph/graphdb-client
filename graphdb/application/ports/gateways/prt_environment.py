from __future__ import annotations

from typing import Protocol

from graphdb.application.ports.gateways.prt_dbclient import DBClientPort
from graphdb.application.ports.gateways.prt_filesystem import FilesystemPort
from graphdb.application.ports.gateways.prt_mysqldump import MySQLDumpBinaryPort
from graphdb.application.ports.gateways.prt_sqlalchemy import SQLAlchemyQueryExecutorPort
from graphdb.application.ports.schema.prt_column import ColumnSchemaPort
from graphdb.application.ports.schema.prt_database import DatabaseSchemaPort
from graphdb.application.ports.schema.prt_key import KeySchemaPort
from graphdb.application.ports.schema.prt_table import TableSchemaPort
from graphdb.application.ports.schema.prt_view import ViewSchemaPort


class EnvironmentPort(Protocol):
    """Container exposing the focused adapter ports for a single environment."""

    @property
    def database(self) -> DatabaseSchemaPort: ...

    @property
    def table(self) -> TableSchemaPort: ...

    @property
    def view(self) -> ViewSchemaPort: ...

    @property
    def column(self) -> ColumnSchemaPort: ...

    @property
    def key(self) -> KeySchemaPort: ...

    @property
    def query_executor(self) -> SQLAlchemyQueryExecutorPort: ...

    @property
    def mysql_client(self) -> DBClientPort: ...

    @property
    def dump_client(self) -> MySQLDumpBinaryPort: ...

    @property
    def filesystem(self) -> FilesystemPort: ...
