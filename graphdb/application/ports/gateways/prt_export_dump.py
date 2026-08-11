from __future__ import annotations

from typing import Any, List, Protocol, Tuple

from graphdb.application.ports.gateways.prt_filesystem import FilesystemPort


class ExportDumpPort(Protocol):
    """Abstract contract for exporting database schemas and table data chunks."""

    filesystem: FilesystemPort

    def get_create_table(self, schema_name: str, table_name: str) -> str:
        ...

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        ...

    def execute(self, sql: str, schema_name: str | None = None) -> List[Tuple[Any, ...]]:
        ...

    def get_tables(self, schema_name: str) -> List[str]:
        ...

    def dump_table_chunk(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str,
        chunk_column: str,
        chunk_start: int,
        chunk_end: int,
        compress: bool = False,
    ) -> None:
        ...

    def dump_table_data(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str,
        compress: bool = False,
    ) -> None:
        ...
