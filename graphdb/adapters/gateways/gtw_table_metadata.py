from __future__ import annotations
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional
from graphdb.application.ports.gateways.prt_table_metadata import TableMetadataPort

class MySQLTableMetadataGateway(TableMetadataPort):
    """Concrete MySQL adapter that executes metadata queries against information_schema."""

    _EXPECTED_COLS = [
        "table_schema", "table_name", "engine", "table_collation",
        "row_format", "table_rows", "data_length", "index_length",
        "total_bytes", "column_count", "nullable_columns",
        "columns_with_default", "index_count", "unique_index_count",
        "avg_row_length",
    ]

    _COMPARISON_SQL = """
        SELECT
            it.table_schema, it.table_name, it.engine, it.table_collation,
            it.row_format, it.table_rows, it.data_length, it.index_length,
            (it.data_length + it.index_length) AS total_bytes,
            (SELECT COUNT(*) FROM information_schema.columns c
             WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name) AS column_count,
            (SELECT SUM(c.is_nullable = 'YES') FROM information_schema.columns c
             WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name) AS nullable_columns,
            (SELECT SUM(c.column_default IS NOT NULL) FROM information_schema.columns c
             WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name) AS columns_with_default,
            (SELECT COUNT(DISTINCT s.index_name) FROM information_schema.statistics s
             WHERE s.table_schema = it.table_schema AND s.table_name = it.table_name) AS index_count,
            (SELECT COUNT(DISTINCT CASE WHEN s.non_unique = 0 THEN s.index_name END)
             FROM information_schema.statistics s
             WHERE s.table_schema = it.table_schema AND s.table_name = it.table_name) AS unique_index_count,
            it.avg_row_length
        FROM information_schema.tables it
        WHERE it.table_schema = '%s' AND it.table_name = '%s'
    """

    def __init__(self, raw_adapter: Any) -> None:
        self.adapter = raw_adapter

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        return self.adapter.table_exists(schema_name, table_name)

    def get_tables(self, schema_name: str) -> List[str]:
        return self.adapter.get_tables(schema_name)

    def fetch_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        rows = self.adapter.execute(self._COMPARISON_SQL % (schema_name, table_name), schema_name=schema_name)
        return self._first_row_as_dict(rows, columns=self._EXPECTED_COLS) or {}

    def get_exact_count(self, schema_name: str, table_name: str) -> int:
        rows = self.adapter.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{table_name}`", schema_name=schema_name)
        return int(rows[0][0]) if rows else 0

    @staticmethod
    def _first_row_as_dict(result: Any, *, columns: List[str]) -> Optional[Dict[str, Any]]:
        if not result or not isinstance(result, (list, tuple)):
            return None
        first = result[0]
        if isinstance(first, Mapping):
            return dict(first)
        if isinstance(first, Sequence) and not isinstance(first, (str, bytes, bytearray)):
            vals = list(first)
            return dict(zip(columns, vals)) if columns else {"__row__": vals}
        return None
