from __future__ import annotations

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor


class DatabaseSchemaAdapter:
    """Adapter for database-level schema operations.

    Executes the same SQL as graphdb.application.core.app_graphdb.GraphDB.
    """

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)

    def database_exists(self, schema_name: str) -> bool:
        query = (
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
            f"WHERE SCHEMA_NAME = '{schema_name}'"
        )
        return len(self._exec.execute(query)) > 0

    def create_database(self, schema_name: str, drop_existing: bool = False) -> None:
        if drop_existing:
            self.drop_database(schema_name)
        self._exec.execute_ddl(f"CREATE DATABASE IF NOT EXISTS {schema_name}")

    def drop_database(self, schema_name: str) -> None:
        self._exec.execute_ddl(f"DROP DATABASE IF EXISTS {schema_name}")
