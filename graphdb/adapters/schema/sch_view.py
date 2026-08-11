from __future__ import annotations

from typing import List

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor, _qt
from graphdb.domain.err_exceptions import SchemaError


class ViewSchemaAdapter:
    """Adapter for view-level schema operations."""

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)

    def get_views(self, schema_name: str) -> List[str]:
        query = (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_type = 'VIEW'"
        )
        return [row[0] for row in self._exec.execute(query)]

    def get_create_view(self, schema_name: str, view_name: str) -> str:
        rows = self._exec.execute(f"SHOW CREATE VIEW {_qt(schema_name, view_name)}", schema_name)
        if not rows:
            raise SchemaError(f"View {schema_name}.{view_name} not found")
        return rows[0][1]

    def is_view(self, schema_name: str, name: str) -> bool:
        query = (
            "SELECT table_type FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{name}'"
        )
        rows = self._exec.execute(query)
        return bool(rows and rows[0][0] == "VIEW")
