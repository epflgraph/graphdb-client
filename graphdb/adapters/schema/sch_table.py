from __future__ import annotations

import re
from typing import List, Optional

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor, _q, _qt
from graphdb.domain.exceptions import SchemaError


class TableSchemaAdapter:
    """Adapter for table-level schema operations.

    Executes the same SQL as graphdb.application.core.app_graphdb.GraphDB.
    """

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)
        self.engine = engine

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        query = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'"
        )
        if exclude_views:
            query += " AND TABLE_TYPE = 'BASE TABLE'"
        return len(self._exec.execute(query)) > 0

    def count_rows_in_table(
        self, schema_name: str, table_name: str, where_clause: Optional[str] = None
    ) -> int:
        query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        return int(self._exec.execute(query, schema_name=schema_name)[0][0])

    def get_table_size(self, schema_name: str, table_name: str) -> int:
        return int(self._exec.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}", schema_name=schema_name)[0][0])

    def get_tables(
        self,
        schema_name: str,
        include_views: bool = False,
        filter_by: Optional[List[str]] = None,
        use_regex: Optional[List[str]] = None,
    ) -> List[str]:
        query = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema_name}'"
        )
        if not include_views:
            query += " AND TABLE_TYPE = 'BASE TABLE'"

        tables = [row[0] for row in self._exec.execute(query)]

        if filter_by and not use_regex:
            tables = [t for t in tables if any(f in t for f in filter_by)]

        if use_regex:
            tables = [t for t in tables if any(re.search(f, t) for f in use_regex)]

        return sorted(tables)

    def get_create_table(self, schema_name: str, table_name: str) -> str:
        query = f"SHOW CREATE TABLE {_q(schema_name)}.{_q(table_name)}"
        rows = self._exec.execute(query, schema_name=schema_name)
        if not rows:
            raise SchemaError(f"Table {schema_name}.{table_name} not found")
        return rows[0][1]

    def drop_table(self, schema_name: str, table_name: str) -> None:
        self._exec.execute_ddl(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")

    def create_table_like(
        self,
        source_schema_name: str,
        source_table_name: str,
        target_schema_name: str,
        target_table_name: str,
        drop_table: bool = False,
    ) -> None:
        if drop_table:
            self.drop_table(target_schema_name, target_table_name)
        self._exec.execute_ddl(
            f"CREATE TABLE IF NOT EXISTS {target_schema_name}.{target_table_name} "
            f"LIKE {source_schema_name}.{source_table_name}"
        )

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
        if replace_existing:
            self.drop_table(schema_name, rename_to)
        self._exec.execute_ddl(
            f"ALTER TABLE {schema_name}.{table_name} RENAME {schema_name}.{rename_to}"
        )
