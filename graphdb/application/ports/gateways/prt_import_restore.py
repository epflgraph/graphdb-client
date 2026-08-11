from __future__ import annotations

from pathlib import Path
from typing import Any, List, Protocol


class FilesystemImportPort(Protocol):
    """Abstract contract for reading, writing, and scanning filesystem artifacts."""

    def read_text(self, path: Any) -> str:
        ...

    def write_text(self, path: Any, content: str) -> None:
        ...

    def exists(self, path: Any) -> bool:
        ...

    def list_sql_files(self, folder: Any, compress: bool = False) -> List[Path]:
        ...


class ImportRestorePort(Protocol):
    """Abstract contract for executing database schema/data restorations from dump files."""

    filesystem: FilesystemImportPort

    def database_exists(self, schema_name: str) -> bool:
        ...

    def create_database(self, schema_name: str) -> None:
        ...

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        ...

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        ...

    def execute_from_file(self, file_path: str, database: str) -> None:
        ...

    def execute_in_shell(self, sql: str, database: str) -> None:
        ...

    def execute_file_with_sed(self, file_path: str, database: str, sed_pattern: str) -> None:
        ...
