"""Pure SQL identifier-quoting helpers.

These have no framework dependencies and live in the domain layer so that both
application services and infrastructure adapters can depend on them without
violating the inward dependency direction of hexagonal architecture.
"""
from __future__ import annotations


def _q(name: str) -> str:
    """Backtick-quote a single SQL identifier, escaping embedded backticks."""
    return f"`{name.replace('`', '``')}`"


def _qt(schema_name: str, table_name: str) -> str:
    """Backtick-quote a schema-qualified table name."""
    return f"{_q(schema_name)}.{_q(table_name)}"
