from __future__ import annotations

from typing import Dict, List, Optional

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor
from graphdb.domain.models.entities.mdl_table import Key


class KeySchemaAdapter:
    """Adapter for key/index-level schema operations.

    Executes the same SQL as graphdb.application.core.app_graphdb.GraphDB.
    """

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)
        self.engine = engine

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        query = (
            "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
            f"WHERE TABLE_SCHEMA = '{schema_name}' "
            f"AND TABLE_NAME   = '{table_name}' "
            f"AND INDEX_NAME   = '{key_name}'"
        )
        return len(self._exec.execute(query)) > 0

    def has_primary_key(self, schema_name: str, table_name: str) -> bool:
        query = f"SHOW KEYS FROM {schema_name}.{table_name} WHERE Key_name = 'PRIMARY'"
        return len(self._exec.execute(query)) > 0

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        query = f"SHOW KEYS FROM {schema_name}.{table_name} WHERE Key_name = 'PRIMARY'"
        return [row[4] for row in self._exec.execute(query) if row is not None]

    def get_keys(self, schema_name: str, table_name: str) -> Dict[str, List[str]]:
        query = f"SHOW KEYS FROM {schema_name}.{table_name}"
        keys: Dict[str, List[str]] = {}
        for row in self._exec.execute(query):
            if row is None:
                continue
            key_name = row[2]
            keys.setdefault(key_name, []).append(row[4])
        return keys

    def get_key_entities(self, schema_name: str, table_name: str) -> List[Key]:
        """Return keys/indexes as domain entities (extension)."""
        query = (
            "SELECT index_name, column_name, non_unique "
            "FROM information_schema.statistics "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY index_name, seq_in_index"
        )
        key_columns: Dict[str, List[str]] = {}
        key_unique: Dict[str, bool] = {}
        for row in self._exec.execute(query):
            name, column, non_unique = row[0], row[1], row[2]
            key_columns.setdefault(name, []).append(column)
            key_unique[name] = non_unique == 0
        return [
            Key(name=name, columns=columns, unique=key_unique.get(name, False), primary=name == "PRIMARY")
            for name, columns in key_columns.items()
        ]

    def drop_keys(self, schema_name: str, table_name: str, ignore_keys: Optional[List[str]] = None) -> None:
        ignore_keys = ignore_keys or ["row_id"]
        keys = self.get_keys(schema_name, table_name)
        if len(keys) == 0:
            return

        query = f"ALTER TABLE {schema_name}.{table_name}"
        for key_name in keys:
            if key_name in ignore_keys:
                continue
            if key_name == "PRIMARY":
                query += " DROP PRIMARY KEY,"
            else:
                query += f" DROP KEY {key_name},"

        if query.endswith(","):
            query = query[:-1]

        self._exec.execute_ddl(query)
