from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor, _q
from graphdb.domain.exceptions import SchemaError
from graphdb.domain.models.mdl_table import Column, Key, Table


class TableSchemaAdapter:
    """Adapter for table-level schema operations against a SQLAlchemy engine."""

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

    def __init__(self, engine: Engine, key_adapter: Optional[Any] = None) -> None:
        self._exec = SchemaExecutor(engine)
        self.engine = engine
        self._key = key_adapter

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
        drop_keys: bool = False,
    ) -> None:
        if drop_table:
            self.drop_table(target_schema_name, target_table_name)
        self._exec.execute_ddl(
            f"CREATE TABLE IF NOT EXISTS {target_schema_name}.{target_table_name} "
            f"LIKE {source_schema_name}.{source_table_name}"
        )
        if drop_keys:
            if self._key is None:
                raise SchemaError("drop_keys requires a key adapter")
            self._key.drop_keys(target_schema_name, target_table_name)

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

    def fetch_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        rows = self._exec.execute(self._METADATA_SQL % (schema_name, table_name), schema_name=schema_name)
        return self._first_row_as_dict(rows, columns=self._METADATA_COLUMNS) or {}

    def get_exact_count(self, schema_name: str, table_name: str) -> int:
        rows = self._exec.execute(
            f"SELECT COUNT(*) FROM `{schema_name}`.`{table_name}`", schema_name=schema_name
        )
        return int(rows[0][0]) if rows else 0

    def describe_table(
        self,
        schema_name: str,
        table_name: str,
        *,
        columns: Optional[List[Column]] = None,
        keys: Optional[List[Key]] = None,
    ) -> Table:
        create_sql = self.get_create_table(schema_name, table_name)
        with self.engine.connect() as connection:
            result = connection.execute(
                text(
                    "SELECT engine, table_collation, row_format FROM information_schema.tables "
                    f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
                )
            )
            row = result.fetchone()
        engine, collation, row_format = (row[0], row[1], row[2]) if row else (None, None, None)
        return Table(
            name=table_name,
            engine=engine,
            collation=collation,
            row_format=row_format,
            columns=columns or [],
            keys=keys or [],
            create_sql=create_sql,
        )

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
