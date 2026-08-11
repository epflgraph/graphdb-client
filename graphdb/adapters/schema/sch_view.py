from __future__ import annotations

import json
from typing import List

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor, _q
from graphdb.domain.err_exceptions import SchemaError


class ViewSchemaAdapter:
    """Adapter for view-level schema operations.

    Executes the same SQL as graphdb.application.core.app_graphdb.GraphDB.
    """

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)

    def get_views(self, schema_name: str) -> List[str]:
        query = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_TYPE = 'VIEW'"
        )
        return [row[0] for row in self._exec.execute(query)]

    def get_create_view(self, schema_name: str, view_name: str) -> str:
        query = f"SHOW CREATE VIEW {_q(schema_name)}.{_q(view_name)}"
        rows = self._exec.execute(query, schema_name=schema_name)
        if not rows:
            raise SchemaError(f"View {schema_name}.{view_name} not found")
        return rows[0][1]

    def is_view(self, schema_name: str, name: str) -> bool:
        query = (
            "SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = {json.dumps(schema_name)} "
            f"AND TABLE_NAME = {json.dumps(name)} "
            "LIMIT 1"
        )
        rows = self._exec.execute(query)
        return bool(rows) and rows[0][0] == "VIEW"
