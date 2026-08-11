from __future__ import annotations

from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _q(name: str) -> str:
    """Backtick-quote a single SQL identifier, escaping embedded backticks."""
    return f"`{name.replace('`', '``')}`"


def _qt(schema_name: str, table_name: str) -> str:
    return f"`{schema_name}`.`{table_name}`"


class SchemaExecutor:
    """Lightweight SQLAlchemy execution helper used by schema adapters."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def execute(self, query: str, schema_name: Optional[str] = None) -> List:
        with self.engine.connect() as connection:
            if schema_name:
                connection.execute(text(f"USE {_q(schema_name)}"))
            result = connection.execute(text(query))
            return result.fetchall() if result.returns_rows else []

    def execute_ddl(self, query: str) -> None:
        with self.engine.connect() as connection:
            connection.execute(text(query))
            connection.commit()

    @property
    def q(self):
        return _q

    @property
    def qt(self):
        return _qt
