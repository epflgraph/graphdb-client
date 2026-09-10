from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from graphdb.domain.models.mdl_table import Column, Key, Table


@runtime_checkable
class TableSchemaPort(Protocol):
    """Port interface for TableSchemaAdapter."""

    def count_rows_in_table(
        self, schema_name: str, table_name: str, where_clause: Optional[str] = None
    ) -> int: ...

    def create_table_like(
        self,
        source_schema_name: str,
        source_table_name: str,
        target_schema_name: str,
        target_table_name: str,
        drop_table: bool = False,
        drop_keys: bool = False,
    ) -> None: ...

    def drop_table(self, schema_name: str, table_name: str) -> None: ...

    def fetch_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]: ...

    def get_create_table(self, schema_name: str, table_name: str) -> str: ...

    def get_exact_count(self, schema_name: str, table_name: str) -> int: ...

    def get_table_size(self, schema_name: str, table_name: str) -> int: ...

    def get_tables(
        self,
        schema_name: str,
        include_views: bool = False,
        filter_by: Optional[List[str]] = None,
        use_regex: Optional[List[str]] = None,
    ) -> List[str]: ...

    def rename_table(
        self,
        schema_name: str,
        table_name: str,
        rename_to: str,
        replace_existing: bool = False,
        simulation_mode: bool = False,
    ) -> None: ...

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool: ...

    def describe_table(
        self,
        schema_name: str,
        table_name: str,
        *,
        columns: Optional[List[Column]] = None,
        keys: Optional[List[Key]] = None,
    ) -> Table: ...
