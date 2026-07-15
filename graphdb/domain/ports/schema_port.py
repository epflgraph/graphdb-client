from __future__ import annotations

from typing import List, Optional, Protocol

from graphdb.domain.schema import Table, View


class SchemaPort(Protocol):
    """Port for schema introspection and DDL operations."""

    def database_exists(self, schema_name: str) -> bool:
        ...

    def create_database(self, schema_name: str, drop_existing: bool = False) -> None:
        ...

    def drop_database(self, schema_name: str) -> None:
        ...

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        ...

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        ...

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        ...

    def get_tables(self, schema_name: str, include_views: bool = False) -> List[str]:
        ...

    def get_views(self, schema_name: str) -> List[str]:
        ...

    def get_create_table(self, schema_name: str, table_name: str) -> str:
        ...

    def get_create_view(self, schema_name: str, view_name: str) -> str:
        ...

    def is_view(self, schema_name: str, name: str) -> bool:
        ...

    def get_column_names(self, schema_name: str, table_name: str) -> List[str]:
        ...

    def get_column_datatypes(self, schema_name: str, table_name: str) -> List[str]:
        ...

    def has_primary_key(self, schema_name: str, table_name: str) -> bool:
        ...

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        ...

    def get_keys(self, schema_name: str, table_name: str) -> List[dict]:
        ...

    def create_table_like(
        self,
        source_schema_name: str,
        source_table_name: str,
        target_schema_name: str,
        target_table_name: str,
        drop_table: bool = False,
        drop_keys: bool = False,
    ) -> None:
        ...

    def drop_table(self, schema_name: str, table_name: str) -> None:
        ...

    def rename_table(
        self,
        schema_name: str,
        table_name: str,
        rename_to: str,
        replace_existing: bool = False,
        simulation_mode: bool = False,
    ) -> None:
        ...

    def drop_keys(self, schema_name: str, table_name: str, ignore_keys: Optional[List[str]] = None) -> None:
        ...
