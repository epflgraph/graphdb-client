from __future__ import annotations

from pathlib import Path
from typing import Any, List, Protocol

from graphdb.application.ports.gateways.prt_filesystem import FilesystemPort


class ImportRestorePort(Protocol):
    """Abstract contract for executing database schema/data restorations from dump files."""

    filesystem: FilesystemPort

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
