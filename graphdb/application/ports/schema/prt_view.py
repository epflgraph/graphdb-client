from __future__ import annotations

from typing import List, Protocol

from graphdb.domain.models.mdl_table import View


class ViewSchemaPort(Protocol):
    """Port interface for ViewSchemaAdapter."""

    def create_view(self, schema_name: str, view_name: str, query: str) -> None: ...

    def describe_view(self, schema_name: str, view_name: str) -> View: ...

    def drop_view(self, schema_name: str, view_name: str) -> None: ...

    def get_create_view(self, schema_name: str, view_name: str) -> str: ...

    def get_views(self, schema_name: str) -> List[str]: ...

    def is_view(self, schema_name: str, name: str) -> bool: ...
