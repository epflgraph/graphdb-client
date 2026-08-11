from __future__ import annotations

from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from graphdb.domain.err_exceptions import SchemaError


def _q(name: str) -> str:
    return f"`{name}`"


def _qt(schema_name: str, table_name: str) -> str:
    return f"`{schema_name}`.`{table_name}`"


class SQLAlchemySchemaIntrospector:
    """Adapter for schema introspection and DDL via SQLAlchemy."""

    def __init__(self, engine: Engine, env_name: str = "default") -> None:
        self.engine = engine
        self.env_name = env_name

    def _execute(self, query: str, schema_name: Optional[str] = None) -> List:
        with self.engine.connect() as connection:
            if schema_name:
                connection.execute(text(f"USE {_q(schema_name)}"))
            result = connection.execute(text(query))
            return result.fetchall() if result.returns_rows else []

    def database_exists(self, schema_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.schemata "
            f"WHERE schema_name = '{schema_name}'"
        )
        return int(self._execute(query)[0][0]) > 0

    def create_database(self, schema_name: str, drop_existing: bool = False) -> None:
        if drop_existing:
            self.drop_database(schema_name)
        with self.engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {_q(schema_name)}"))
            connection.commit()

    def drop_database(self, schema_name: str) -> None:
        with self.engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS {_q(schema_name)}"))
            connection.commit()

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
        )
        if exclude_views:
            query += " AND table_type = 'BASE TABLE'"
        return int(self._execute(query)[0][0]) > 0

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            f"AND column_name = '{column_name}'"
        )
        return int(self._execute(query)[0][0]) > 0

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.statistics "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            f"AND index_name = '{key_name}'"
        )
        return int(self._execute(query)[0][0]) > 0

    def get_tables(self, schema_name: str, include_views: bool = False) -> List[str]:
        query = (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}'"
        )
        if not include_views:
            query += " AND table_type = 'BASE TABLE'"
        return [row[0] for row in self._execute(query)]

    def get_views(self, schema_name: str) -> List[str]:
        query = (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_type = 'VIEW'"
        )
        return [row[0] for row in self._execute(query)]

    def get_create_table(self, schema_name: str, table_name: str) -> str:
        rows = self._execute(f"SHOW CREATE TABLE {_qt(schema_name, table_name)}", schema_name)
        if not rows:
            raise SchemaError(f"Table {schema_name}.{table_name} not found")
        return rows[0][1]

    def get_create_view(self, schema_name: str, view_name: str) -> str:
        rows = self._execute(f"SHOW CREATE VIEW {_qt(schema_name, view_name)}", schema_name)
        if not rows:
            raise SchemaError(f"View {schema_name}.{view_name} not found")
        return rows[0][1]

    def is_view(self, schema_name: str, name: str) -> bool:
        query = (
            "SELECT table_type FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{name}'"
        )
        rows = self._execute(query)
        return bool(rows and rows[0][0] == "VIEW")

    def get_column_names(self, schema_name: str, table_name: str) -> List[str]:
        query = (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        )
        return [row[0] for row in self._execute(query)]

    def get_column_datatypes(self, schema_name: str, table_name: str) -> List[str]:
        query = (
            "SELECT column_type FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        )
        return [row[0] for row in self._execute(query)]

    def has_primary_key(self, schema_name: str, table_name: str) -> bool:
        query = (
            "SELECT COUNT(*) FROM information_schema.table_constraints "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "AND constraint_type = 'PRIMARY KEY'"
        )
        return int(self._execute(query)[0][0]) > 0

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        query = (
            "SELECT column_name FROM information_schema.key_column_usage "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "AND constraint_name = 'PRIMARY' "
            "ORDER BY ordinal_position"
        )
        return [row[0] for row in self._execute(query)]

    def get_keys(self, schema_name: str, table_name: str) -> List[dict]:
        query = (
            "SELECT index_name, column_name, non_unique FROM information_schema.statistics "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY index_name, seq_in_index"
        )
        keys: dict = {}
        for row in self._execute(query):
            name, column, non_unique = row[0], row[1], row[2]
            keys.setdefault(name, {"name": name, "columns": [], "unique": non_unique == 0})
            keys[name]["columns"].append(column)
        return list(keys.values())

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
        create_sql = self.get_create_table(source_schema_name, source_table_name)
        create_sql = create_sql.replace(
            f"`{source_table_name}`", f"`{target_schema_name}`.`{target_table_name}`"
        )
        with self.engine.connect() as connection:
            connection.execute(text(create_sql))
            connection.commit()
        if drop_keys:
            self.drop_keys(target_schema_name, target_table_name)

    def drop_table(self, schema_name: str, table_name: str) -> None:
        with self.engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {_qt(schema_name, table_name)}"))
            connection.commit()

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

    def drop_keys(self, schema_name: str, table_name: str, ignore_keys: Optional[List[str]] = None) -> None:
        ignore_keys = ignore_keys or ["row_id"]
        keys = self.get_keys(schema_name, table_name)
        with self.engine.connect() as connection:
            for key in keys:
                if key["name"] in ignore_keys:
                    continue
                if key["name"] == "PRIMARY":
                    connection.execute(
                        text(f"ALTER TABLE {_qt(schema_name, table_name)} DROP PRIMARY KEY")
                    )
                else:
                    connection.execute(
                        text(
                            f"ALTER TABLE {_qt(schema_name, table_name)} DROP INDEX `{key['name']}`"
                        )
                    )
            connection.commit()
