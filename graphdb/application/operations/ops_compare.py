from __future__ import annotations
import random
from typing import Any, Dict, List, Tuple

from graphdb.application.policies.pol_table import TableComparisonPolicy
from graphdb.adapters.environments import Environments
from graphdb.adapters.gateways.gtw_environment import EnvironmentGateway


class CompareOperations:
    """Use case orchestrator for table and database comparison."""

    def __init__(self, registry: Environments) -> None:
        self.registry = registry
        self.policy = TableComparisonPolicy()

    @staticmethod
    def _q(name: str) -> str:
        return f"`{name.replace('`', '``')}`"

    @staticmethod
    def _sql_literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _uid_columns(self, env: EnvironmentGateway, schema_name: str, table_name: str) -> List[str]:
        return env.key.get_keys(schema_name, table_name).get("uid", [])

    def _sample_uid_tuples(
        self,
        env: EnvironmentGateway,
        schema_name: str,
        table_name: str,
        sample_size: int,
    ) -> List[Tuple[Any, ...]]:
        uid_columns = self._uid_columns(env, schema_name, table_name)
        if not uid_columns:
            return []

        q_schema = self._q(schema_name)
        q_table = self._q(table_name)
        q_uid_cols = ", ".join(self._q(c) for c in uid_columns)
        query = (
            f"SELECT {q_uid_cols} FROM {q_schema}.{q_table} "
            f"ORDER BY RAND() LIMIT {sample_size}"
        )
        rows = env.query_executor.execute(query, schema_name=schema_name)
        return [tuple(row) for row in rows]

    def _fetch_rows_by_uid(
        self,
        env: EnvironmentGateway,
        schema_name: str,
        table_name: str,
        uid_tuples: List[Tuple[Any, ...]],
    ) -> Dict[Tuple[Any, ...], Dict[str, Any]]:
        uid_columns = self._uid_columns(env, schema_name, table_name)
        if not uid_columns or not uid_tuples:
            return {}

        column_names = env.column.get_column_names(schema_name, table_name)
        q_schema = self._q(schema_name)
        q_table = self._q(table_name)
        q_columns = ", ".join(self._q(c) for c in column_names)
        q_uid_cols = ", ".join(self._q(c) for c in uid_columns)

        def _format_uid(t: Tuple[Any, ...]) -> str:
            if len(t) == 1:
                return self._sql_literal(t[0])
            return f"({', '.join(self._sql_literal(v) for v in t)})"

        in_clause = ", ".join(_format_uid(t) for t in uid_tuples)
        query = (
            f"SELECT {q_columns} FROM {q_schema}.{q_table} "
            f"WHERE ({q_uid_cols}) IN ({in_clause})"
        )
        rows = env.query_executor.execute(query, schema_name=schema_name)

        result: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for row in rows:
            uid = tuple(row[: len(uid_columns)])
            result[uid] = dict(zip(column_names, row))
        return result

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
        source_meta: EnvironmentGateway = self.registry.get(source_env)
        target_meta: EnvironmentGateway = self.registry.get(target_env)

        if not source_meta.table.table_exists(source_schema, table_name):
            return {"error": f"Table {source_schema}.{table_name} does not exist in '{source_env}'"}
        if not target_meta.table.table_exists(target_schema, table_name):
            return {"error": f"Table {target_schema}.{table_name} does not exist in '{target_env}'"}

        source_row = source_meta.table.fetch_table_metadata(source_schema, table_name)
        target_row = target_meta.table.fetch_table_metadata(target_schema, table_name)

        source_count = source_meta.table.get_exact_count(source_schema, table_name)
        target_count = target_meta.table.get_exact_count(target_schema, table_name)
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
            "source": {"env": source_env, "schema": source_schema},
            "target": {"env": target_env, "schema": target_schema},
            "rows": rows,
            "has_error": has_error,
            "has_warning": has_warning,
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
        source_meta: EnvironmentGateway = self.registry.get(source_env)
        target_meta: EnvironmentGateway = self.registry.get(target_env)

        source_tables = set(source_meta.table.get_tables(source_schema))
        target_tables = set(target_meta.table.get_tables(target_schema))

        results: List[Dict[str, Any]] = []
        for table_name in sorted(source_tables | target_tables):
            result = self.compare_tables(
                source_env,
                source_schema,
                target_env,
                target_schema,
                table_name,
                row_count_tolerance=row_count_tolerance,
                ignore_warnings=ignore_warnings,
            )
            if ignore_warnings and result.get("has_warning") and not result.get("has_error"):
                continue
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
        source_meta: EnvironmentGateway = self.registry.get(source_env)
        target_meta: EnvironmentGateway = self.registry.get(target_env)

        if not source_meta.table.table_exists(source_schema, table_name):
            return {"error": f"Table {source_schema}.{table_name} does not exist in '{source_env}'"}
        if not target_meta.table.table_exists(target_schema, table_name):
            return {"error": f"Table {target_schema}.{table_name} does not exist in '{target_env}'"}

        source_uid_cols = self._uid_columns(source_meta, source_schema, table_name)
        if not source_uid_cols:
            return {"error": f"Table {source_schema}.{table_name} has no 'uid' unique key"}

        half = sample_size // 2
        source_uids = self._sample_uid_tuples(source_meta, source_schema, table_name, half)
        target_uids = self._sample_uid_tuples(target_meta, target_schema, table_name, half)
        all_uids = list(set(source_uids) | set(target_uids))

        if not all_uids:
            return {
                "table": table_name,
                "source": {"env": source_env, "schema": source_schema},
                "target": {"env": target_env, "schema": target_schema},
                "error": "No rows sampled from either table",
            }

        source_rows = self._fetch_rows_by_uid(source_meta, source_schema, table_name, all_uids)
        target_rows = self._fetch_rows_by_uid(target_meta, target_schema, table_name, all_uids)

        new_rows = 0
        deleted_rows = 0
        matched = 0
        mismatched = 0
        mismatch_examples: List[Dict[str, Any]] = []

        for uid in all_uids:
            in_source = uid in source_rows
            in_target = uid in target_rows

            if in_source and not in_target:
                new_rows += 1
            elif in_target and not in_source:
                deleted_rows += 1
            elif in_source and in_target:
                if source_rows[uid] == target_rows[uid]:
                    matched += 1
                else:
                    mismatched += 1
                    if len(mismatch_examples) < 5:
                        mismatch_examples.append(
                            {
                                "uid": uid,
                                "source": source_rows[uid],
                                "target": target_rows[uid],
                            }
                        )

        total = len(all_uids)
        return {
            "table": table_name,
            "source": {"env": source_env, "schema": source_schema},
            "target": {"env": target_env, "schema": target_schema},
            "sample_size": total,
            "new_rows": new_rows,
            "deleted_rows": deleted_rows,
            "matched": matched,
            "mismatched": mismatched,
            "mismatch_examples": mismatch_examples,
        }

    def compare_databases_by_random_sampling(
        self,
        source_env: str,
        source_schema: str,
        target_env: str,
        target_schema: str,
        sample_size: int = 1024,
    ) -> List[Dict[str, Any]]:
        source_meta: EnvironmentGateway = self.registry.get(source_env)
        target_meta: EnvironmentGateway = self.registry.get(target_env)

        source_tables = set(source_meta.table.get_tables(source_schema))
        target_tables = set(target_meta.table.get_tables(target_schema))

        results: List[Dict[str, Any]] = []
        for table_name in sorted(source_tables | target_tables):
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

    @staticmethod
    def _extract_metrics(source_row: Dict[str, Any], target_row: Dict[str, Any]):
        for col in source_row.keys():
            if col not in ("table_schema", "table_name"):
                yield col, source_row.get(col), target_row.get(col)
