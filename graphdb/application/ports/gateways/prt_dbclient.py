from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

@runtime_checkable
class DBClientPort(Protocol):
    """Port interface implemented by matching adapter."""
    def execute_file_with_sed(self, file_path: str, database: Optional[str] = None, sed_pattern: str = 's/^INSERT INTO /INSERT IGNORE INTO /') -> None: ...
    def execute_from_file(self, file_path: str, database: Optional[str] = None) -> None: ...
    def execute_query(self, query: str, database: Optional[str] = None, query_id: Optional[str] = None) -> None: ...
