from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Tuple, Union


class DatabasePort(Protocol):
    """Port for executing queries against a database environment."""

    def test(self) -> bool:
        """Return True if the environment is reachable."""
        ...

    def execute(
        self,
        query: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        commit: bool = False,
        return_exception: bool = False,
        query_id: Optional[str] = None,
    ) -> Union[List[Any], Tuple[str, str, Any]]:
        """Execute a query and return rows or exception details."""
        ...

    def execute_in_shell(
        self,
        query: str,
        database: Optional[str] = None,
        query_id: Optional[str] = None,
    ) -> None:
        """Execute a query via the native mysql shell client."""
        ...

    def execute_from_file(
        self,
        file_path: str,
        database: Optional[str] = None,
    ) -> None:
        """Execute SQL statements streamed from a file (plain or gzipped)."""
        ...

    def execute_stream_to_file(
        self,
        query: str,
        output_file: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        fetch_size: int = 1000,
        query_id: Optional[str] = None,
    ) -> None:
        """Stream SELECT results as newline-delimited JSON."""
        ...
