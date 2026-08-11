from __future__ import annotations

from typing import Dict, List

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor
from graphdb.domain.models.entities.mdl_table import Column


class ColumnSchemaAdapter:
    """Adapter for column-level schema introspection.

    Executes the same SQL as graphdb.application.core.app_graphdb.GraphDB.
    """

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        query = (
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema_name}' "
            f"AND TABLE_NAME   = '{table_name}' "
            f"AND COLUMN_NAME  = '{column_name}'"
        )
        return len(self._exec.execute(query)) > 0

    def has_column(self, schema_name: str, table_name: str, column_name: str) -> bool:
        """Alias for column_exists; matches legacy GraphDB naming."""
        return self.column_exists(schema_name, table_name, column_name)

    def get_column_names(self, schema_name: str, table_name: str) -> List[str]:
        query = f"SHOW COLUMNS FROM {schema_name}.{table_name}"
        return [row[0] for row in self._exec.execute(query) if row is not None]

    def get_column_datatypes(self, schema_name: str, table_name: str) -> Dict[str, str]:
        query = f"SHOW COLUMNS FROM {schema_name}.{table_name}"
        datatypes: Dict[str, str] = {}
        for row in self._exec.execute(query):
            if row is None:
                continue
            datatypes[row[0]] = row[1]
        return datatypes

    def get_columns(self, schema_name: str, table_name: str) -> List[Column]:
        """Return column definitions as domain entities (extension)."""
        query = (
            "SELECT column_name, column_type, is_nullable, column_default "
            "FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        )
        columns: List[Column] = []
        for row in self._exec.execute(query):
            name, datatype, nullable_str, default = row[0], row[1], row[2], row[3]
            nullable = {"YES": True, "NO": False}.get(nullable_str) if nullable_str else None
            columns.append(Column(name=name, datatype=datatype, nullable=nullable, default=default))
        return columns
