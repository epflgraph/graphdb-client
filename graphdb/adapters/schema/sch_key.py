from __future__ import annotations

from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor, _qt
from graphdb.domain.models.entities.mdl_table import Key


class KeySchemaAdapter:
    """Adapter for key/index-level schema operations."""

    def __init__(self, engine: Engine) -> None:
        self._exec = SchemaExecutor(engine)
        self.engine = engine

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.statistics "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            f"AND index_name = '{key_name}'"
        )
        return int(self._exec.execute(query)[0][0]) > 0

    def has_primary_key(self, schema_name: str, table_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.table_constraints "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "AND constraint_type = 'PRIMARY KEY'"
        )
        return int(self._exec.execute(query)[0][0]) > 0

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        query = (
            "SELECT column_name FROM information_schema.key_column_usage "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "AND constraint_name = 'PRIMARY' "
            "ORDER BY ordinal_position"
        )
        return [row[0] for row in self._exec.execute(query)]

    def get_keys(self, schema_name: str, table_name: str) -> Dict[str, List[str]]:
        """Return key name -> ordered list of column names."""
        query = (
            "SELECT index_name, column_name FROM information_schema.statistics "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY index_name, seq_in_index"
        )
        keys: Dict[str, List[str]] = {}
        for row in self._exec.execute(query):
            name, column = row[0], row[1]
            keys.setdefault(name, []).append(column)
        return keys

    def get_key_entities(self, schema_name: str, table_name: str) -> List[Key]:
        """Return keys/indexes as domain entities."""
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
        with self.engine.connect() as connection:
            for key_name in keys:
                if key_name in ignore_keys:
                    continue
                if key_name == "PRIMARY":
                    connection.execute(
                        text(f"ALTER TABLE {_qt(schema_name, table_name)} DROP PRIMARY KEY")
                    )
                else:
                    connection.execute(
                        text(f"ALTER TABLE {_qt(schema_name, table_name)} DROP INDEX `{key_name}`")
                    )
            connection.commit()
