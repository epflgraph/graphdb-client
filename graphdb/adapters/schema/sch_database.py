from __future__ import annotations

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor, _q


class DatabaseSchemaAdapter:
    """Adapter for database-level schema operations."""

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)

    def database_exists(self, schema_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.schemata "
            f"WHERE schema_name = '{schema_name}'"
        )
        return int(self._exec.execute(query)[0][0]) > 0

    def create_database(self, schema_name: str, drop_existing: bool = False) -> None:
        if drop_existing:
            self.drop_database(schema_name)
        self._exec.execute_ddl(f"CREATE DATABASE IF NOT EXISTS {_q(schema_name)}")

    def drop_database(self, schema_name: str) -> None:
        self._exec.execute_ddl(f"DROP DATABASE IF EXISTS {_q(schema_name)}")
