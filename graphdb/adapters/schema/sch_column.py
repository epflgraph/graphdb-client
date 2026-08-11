from __future__ import annotations

from typing import List

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor
from graphdb.domain.models.entities.mdl_table import Column


class ColumnSchemaAdapter:
    """Adapter for column-level schema introspection."""

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            f"AND column_name = '{column_name}'"
        )
        return int(self._exec.execute(query)[0][0]) > 0

    def get_column_names(self, schema_name: str, table_name: str) -> List[str]:
        query = (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        )
        return [row[0] for row in self._exec.execute(query)]

    def get_column_datatypes(self, schema_name: str, table_name: str) -> List[str]:
        query = (
            "SELECT column_type FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        )
        return [row[0] for row in self._exec.execute(query)]

    def get_columns(self, schema_name: str, table_name: str) -> List[Column]:
        """Return column definitions as domain entities."""
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
