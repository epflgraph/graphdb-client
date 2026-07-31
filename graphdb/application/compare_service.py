from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple

from graphdb.application.adapter_registry import AdapterRegistry
from graphdb.core.graphdb import GraphDB


class CompareService:
    """Application service for comparing tables and databases across environments."""

    def __init__(self, registry: AdapterRegistry) -> None:
        self.registry = registry

    _EXPECTED_COLS = [
        "table_schema",
        "table_name",
        "engine",
        "table_collation",
        "row_format",
        "table_rows",
        "data_length",
        "index_length",
        "total_bytes",
        "column_count",
        "nullable_columns",
        "columns_with_default",
        "index_count",
        "unique_index_count",
        "avg_row_length",
    ]

    _COMPARISON_SQL = """
        SELECT
            it.table_schema,
            it.table_name,
            it.engine,
            it.table_collation,
            it.row_format,
            it.table_rows,
            it.data_length,
            it.index_length,
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
        WHERE it.table_schema = '%s'
        AND it.table_name = '%s'
    """

    def compare_tables(
        self,
        source_env: str,
        source_schema: str,
        target_env: str,
        target_schema: str,
        table_name: str,
        row_count_tolerance: float = 0.10,
        ignore_warnings: bool = False,
    ) -> Dict[str, Any]:
        source_adapter = self.registry.get(source_env)
        target_adapter = self.registry.get(target_env)

        if not source_adapter.table_exists(source_schema, table_name):
            return {"error": f"Table {source_schema}.{table_name} does not exist in '{source_env}'"}
        if not target_adapter.table_exists(target_schema, table_name):
            return {"error": f"Table {target_schema}.{table_name} does not exist in '{target_env}'"}

        source_row = self._fetch_metadata(source_adapter, source_schema, table_name)
        target_row = self._fetch_metadata(target_adapter, target_schema, table_name)

        source_count = self._exact_count(source_adapter, source_schema, table_name)
        target_count = self._exact_count(target_adapter, target_schema, table_name)
        source_row["table_rows"] = source_count
        target_row["table_rows"] = target_count

        rows: List[Dict[str, Any]] = []
        has_error = False
        has_warning = False

        for col in self._EXPECTED_COLS:
            if col in ("table_schema", "table_name"):
                continue
            a, b = source_row.get(col), target_row.get(col)
            if col == "table_rows":
                status = self._row_count_status(a, b, row_count_tolerance)
            else:
                status = "OK" if a == b else "ERR"
            rows.append({"metric": col, "source": a, "target": b, "status": status})
            if status == "ERR":
                has_error = True
            elif status == "WARN":
                has_warning = True

        return {
            "table": table_name,
            "source": {"env": source_env, "schema": source_schema, "rows": source_count},
            "target": {"env": target_env, "schema": target_schema, "rows": target_count},
            "rows": rows,
            "has_error": has_error,
            "has_warning": has_warning,
            "ignore_warnings": ignore_warnings,
        }

    def compare_databases(
        self,
        source_env: str,
        source_schema: str,
        target_env: str,
        target_schema: str,
        row_count_tolerance: float = 0.10,
        ignore_warnings: bool = False,
    ) -> List[Dict[str, Any]]:
        source_adapter = self.registry.get(source_env)
        target_adapter = self.registry.get(target_env)

        source_tables = set(source_adapter.get_tables(source_schema))
        target_tables = set(target_adapter.get_tables(target_schema))
        all_tables = sorted(source_tables | target_tables)

        results = []
        for table_name in all_tables:
            result = self.compare_tables(
                source_env,
                source_schema,
                target_env,
                target_schema,
                table_name,
                row_count_tolerance=row_count_tolerance,
                ignore_warnings=ignore_warnings,
            )
            results.append(result)
        return results

    def compare_tables_by_random_sampling(
        self,
        source_env: str,
        source_schema: str,
        target_env: str,
        target_schema: str,
        table_name: str,
        sample_size: int = 1024,
    ) -> Dict[str, Any]:
        """Compare two tables using the legacy GraphDB random-sampling method.

        The underlying implementation prints results directly, so this wrapper
        only validates table existence and returns a lightweight status dict.
        """
        source_adapter = self.registry.get(source_env)
        target_adapter = self.registry.get(target_env)

        if not source_adapter.table_exists(source_schema, table_name):
            return {"error": f"Table {source_schema}.{table_name} does not exist in '{source_env}'"}
        if not target_adapter.table_exists(target_schema, table_name):
            return {"error": f"Table {target_schema}.{table_name} does not exist in '{target_env}'"}

        graphdb = GraphDB(config=self.registry.config)
        graphdb.compare_tables_by_random_sampling(
            source_engine_name=source_env,
            source_schema_name=source_schema,
            source_table_name=table_name,
            target_engine_name=target_env,
            target_schema_name=target_schema,
            target_table_name=table_name,
            sample_size=sample_size,
        )
        return {"ok": True}

    def compare_databases_by_random_sampling(
        self,
        source_env: str,
        source_schema: str,
        target_env: str,
        target_schema: str,
        sample_size: int = 1024,
    ) -> List[Dict[str, Any]]:
        """Compare all tables present in both schemas using random sampling."""
        source_adapter = self.registry.get(source_env)
        target_adapter = self.registry.get(target_env)

        source_tables = set(source_adapter.get_tables(source_schema))
        target_tables = set(target_adapter.get_tables(target_schema))
        common_tables = sorted(source_tables & target_tables)

        results = []
        for table_name in common_tables:
            result = self.compare_tables_by_random_sampling(
                source_env,
                source_schema,
                target_env,
                target_schema,
                table_name,
                sample_size=sample_size,
            )
            results.append(result)
        return results

    def _fetch_metadata(self, adapter, schema_name: str, table_name: str) -> Dict[str, Any]:
        rows = adapter.execute(self._COMPARISON_SQL % (schema_name, table_name), schema_name=schema_name)
        return self._first_row_as_dict(rows, columns=self._EXPECTED_COLS) or {}

    def _exact_count(self, adapter, schema_name: str, table_name: str) -> int:
        rows = adapter.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{table_name}`", schema_name=schema_name)
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
            if not columns:
                return {"__row__": vals}
            return dict(zip(columns, vals))
        return None

    @staticmethod
    def _row_count_status(a: Any, b: Any, tol: float) -> str:
        try:
            a = None if a is None else int(a)
            b = None if b is None else int(b)
        except Exception:
            return "ERR" if a != b else "OK"
        if a == b:
            return "OK"
        if a is None or b is None:
            return "ERR"
        if a == 0 and b == 0:
            return "OK"
        if a == 0 or b == 0:
            return "ERR"
        rel_diff = abs(a - b) / max(a, b)
        return "WARN" if rel_diff <= tol else "ERR"
