from __future__ import annotations

from typing import Any, Protocol

class MySQLDumpBinaryPort(Protocol):
    """Port interface implemented by matching adapter."""
    def dump_table_chunk(self, schema_name: str, table_name: str, output_file: str, where: str = 'TRUE', chunk_column: str = 'row_id', chunk_start: int = 0, chunk_end: int = 0, compress: bool = False) -> None: ...
    def dump_table_data(self, schema_name: str, table_name: str, output_file: str, where: str = 'TRUE', compress: bool = False) -> None: ...
