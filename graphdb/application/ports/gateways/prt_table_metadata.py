from __future__ import annotations
from typing import Any, Dict, List, Protocol

class TableMetadataPort(Protocol):
    """Abstract contract for querying database schema metadata and metrics."""

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        ...

    def get_tables(self, schema_name: str) -> List[str]:
        ...

    def fetch_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        ...

    def get_exact_count(self, schema_name: str, table_name: str) -> int:
        ...
