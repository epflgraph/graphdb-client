from __future__ import annotations

from typing import Protocol


class DumpPort(Protocol):
    """Port for bulk data export/import using native mysqldump/mysql tools."""

    def dump_table_data(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str = "TRUE",
        compress: bool = False,
    ) -> None:
        """Dump table data to a file (plain SQL or gzipped)."""
        ...

    def dump_table_chunk(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str = "TRUE",
        chunk_column: str = "row_id",
        chunk_start: int = 0,
        chunk_end: int = 0,
        compress: bool = False,
    ) -> None:
        """Dump a chunked slice of table data."""
        ...
