from __future__ import annotations
from typing import Any, Dict, List

from graphdb.application.policies.pol_table import TableComparisonPolicy
from graphdb.application.ports.gateways.prt_adapter_registry import AdapterRegistryPort, EnvironmentPort


class CompareOperations:
    """Use case orchestrator for table and database metadata comparison."""

    def __init__(self, registry: AdapterRegistryPort) -> None:
        self.registry = registry
        self.policy = TableComparisonPolicy()

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
        source_meta: EnvironmentPort = self.registry.get(source_env)
        target_meta: EnvironmentPort = self.registry.get(target_env)

        if not source_meta.table_exists(source_schema, table_name):
            return {"error": f"Table {source_schema}.{table_name} does not exist in '{source_env}'"}
        if not target_meta.table_exists(target_schema, table_name):
            return {"error": f"Table {target_schema}.{table_name} does not exist in '{target_env}'"}

        source_row = source_meta.fetch_table_metadata(source_schema, table_name)
        target_row = target_meta.fetch_table_metadata(target_schema, table_name)

        source_count = source_meta.get_exact_count(source_schema, table_name)
        target_count = target_meta.get_exact_count(target_schema, table_name)
        source_row["table_rows"] = source_count
        target_row["table_rows"] = target_count

        rows: List[Dict[str, Any]] = []
        has_error = False
        has_warning = False

        for col, a_val, b_val in self._extract_metrics(source_row, target_row):
            status = self.policy.evaluate_metric_status(col, a_val, b_val, row_count_tolerance)
            rows.append({"metric": col, "source": a_val, "target": b_val, "status": status})
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
        source_meta: EnvironmentPort = self.registry.get(source_env)
        target_meta: EnvironmentPort = self.registry.get(target_env)

        source_tables = set(source_meta.get_tables(source_schema))
        target_tables = set(target_meta.get_tables(target_schema))
        all_tables = sorted(source_tables | target_tables)

        return [
            self.compare_tables(
                source_env, source_schema, target_env, target_schema,
                tbl, row_count_tolerance, ignore_warnings
            )
            for tbl in all_tables
        ]

    @staticmethod
    def _extract_metrics(source_row: Dict[str, Any], target_row: Dict[str, Any]):
        for col in source_row.keys():
            if col not in ("table_schema", "table_name"):
                yield col, source_row.get(col), target_row.get(col)
