from __future__ import annotations

from typing import List

from sqlalchemy import text
from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor, _q, _qt
from graphdb.domain.err_exceptions import SchemaError


class TableSchemaAdapter:
    """Adapter for table-level schema operations."""

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)
        self.engine = engine

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
        )
        if exclude_views:
            query += " AND table_type = 'BASE TABLE'"
        return int(self._exec.execute(query)[0][0]) > 0

    def get_tables(self, schema_name: str, include_views: bool = False) -> List[str]:
        query = (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}'"
        )
        if not include_views:
            query += " AND table_type = 'BASE TABLE'"
        return [row[0] for row in self._exec.execute(query)]

    def get_create_table(self, schema_name: str, table_name: str) -> str:
        rows = self._exec.execute(f"SHOW CREATE TABLE {_qt(schema_name, table_name)}", schema_name)
        if not rows:
            raise SchemaError(f"Table {schema_name}.{table_name} not found")
        return rows[0][1]

    def drop_table(self, schema_name: str, table_name: str) -> None:
        self._exec.execute_ddl(f"DROP TABLE IF EXISTS {_qt(schema_name, table_name)}")

    def create_table_like(
        self,
        source_schema_name: str,
        source_table_name: str,
        target_schema_name: str,
        target_table_name: str,
        drop_table: bool = False,
    ) -> str:
        """Create target table like source table and return the CREATE SQL."""
        if drop_table:
            self.drop_table(target_schema_name, target_table_name)
        create_sql = self.get_create_table(source_schema_name, source_table_name)
        create_sql = create_sql.replace(
            f"`{source_table_name}`", f"`{target_schema_name}`.`{target_table_name}`"
        )
        self._exec.execute_ddl(create_sql)
        return create_sql

    def rename_table(
        self,
        schema_name: str,
        table_name: str,
        rename_to: str,
        replace_existing: bool = False,
        simulation_mode: bool = False,
    ) -> None:
        if simulation_mode:
            return
        with self.engine.connect() as connection:
            if replace_existing:
                connection.execute(text(f"DROP TABLE IF EXISTS {_qt(schema_name, rename_to)}"))
            connection.execute(
                text(f"RENAME TABLE {_qt(schema_name, table_name)} TO {_qt(schema_name, rename_to)}")
            )
            connection.commit()
