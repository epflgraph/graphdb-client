# graphdb/application/operations/ops_compare.py
from __future__ import annotations
import json
import random
import time

import numpy as np

# Allow running this file directly from the repo root
if __name__ == "__main__":
    import sys
    from pathlib import Path
    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root))

from graphdb.domain.models.mdl_config import GraphDBConfig
from graphdb.application.policies.pol_table import TableComparisonPolicy
from graphdb.adapters.environments import Environments
from graphdb.adapters.rendering.rdr_statusmsg import StatusMessageAdapter

#==================#
# Class definition #
#==================#
class CompareOperations:
    """Use case orchestrator for table and database comparison."""

    def __init__(self, envs: Environments) -> None:
        self.envs = envs
        self.status = StatusMessageAdapter()

    # Helper method to safely quote identifiers (e.g. database, table, column names) with backticks
    def _q(self, name: str) -> str:
        """Backtick-quote a single SQL identifier safely."""
        return f"`{name.replace('`', '``')}`"

    def _qt(self, schema_name: str, table_name: str) -> str:
        """Backtick-quote a schema-qualified table name safely."""
        return f"{self._q(schema_name)}.{self._q(table_name)}"

    #=========================================================#
    #                                                         #
    #   METHOD GROUP: Compare tables across engines/servers   #
    #                                                         #
    #=========================================================#

    #---------------------------------------#
    # Method: Compare tables across engines #
    #---------------------------------------#
    def compare_tables(self, source_engine_name, source_schema_name,
                    target_engine_name, target_schema_name,
                    table_name: str, *, row_count_tolerance: float = 0.10,
                    ignore_warnings: bool = False):
        """
        Compare a table across two MySQL servers/schemas and print results as:

                            source   target   diff    result
        metric
        engine              ...      ...      ...     🟢/🟡/🔴
        ...

        Always computes exact row counts (COUNT(*)).
        Row counts that differ by <= row_count_tolerance (default 10%) are
        reported as warnings, not errors. Missing tables or schema differences
        remain errors.

        If ignore_warnings=True, no output is produced for tables that have
        only warnings and no errors.

        Returns a dict with source/target rows + comparison table rows + df.
        """

        from collections.abc import Mapping, Sequence
        import pandas as pd

        OK, WARN, ERR = "🟢", "🟡", "🔴"

        source_path = f"{source_engine_name}.{source_schema_name}"
        target_path = f"{target_engine_name}.{target_schema_name}"

        def _print_header():
            header_width = max(
                len(table_name),
                len(f"Source: {source_path}"),
                len(f"Target: {target_path}"),
            ) + 4
            bar = "─" * (header_width + 1)
            table_name_colored = f"\033[1;36m{table_name}\033[0m"
            print("\n")
            print(f"┌{bar}┐")
            print(f"│ Table : {table_name_colored}{' ' * (header_width - 9 - len(table_name))} │")
            print(f"│ Source: {source_path:<{header_width - 9}} │")
            print(f"│ Target: {target_path:<{header_width - 9}} │")
            print(f"└{bar}┘")

        # -------------------------
        # Formatting helpers
        # -------------------------
        def _fmt(v):
            return "NULL" if v is None else str(v)

        def _fmt_dt(v):
            return "NULL" if v is None else str(v)

        def _fmt_bytes(n):
            if n is None:
                return "NULL"
            try:
                n = int(n)
            except Exception:
                return str(n)
            units = ["B", "KiB", "MiB", "GiB", "TiB"]
            v = float(n)
            for u in units:
                if v < 1024 or u == units[-1]:
                    return f"{v:.2f} {u}"
                v /= 1024.0

        def _row_count_status(a, b, tol):
            """
            Decide status for numeric counts (exact or estimate):
            - equal -> OK
            - one is zero and the other is not -> ERR
            - relative difference <= tol -> WARN
            - otherwise -> ERR
            """
            try:
                a = None if a is None else int(a)
                b = None if b is None else int(b)
            except Exception:
                return ERR if a != b else OK

            if a == b:
                return OK
            if a is None or b is None:
                return ERR
            if a == 0 and b == 0:
                return OK
            if a == 0 or b == 0:
                return ERR

            rel_diff = abs(a - b) / max(a, b)
            return WARN if rel_diff <= tol else ERR

        def _count_diff(a, b):
            """Human-readable diff for counts, e.g. '+2 (0.07%)'."""
            try:
                a = None if a is None else int(a)
                b = None if b is None else int(b)
            except Exception:
                return ""
            if a is None or b is None:
                return ""
            if a == b:
                return ""
            delta = b - a
            denom = max(a, b)
            pct = abs(delta) / denom * 100 if denom else 0.0
            sign = "+" if delta >= 0 else ""
            return f"{sign}{delta} ({pct:.1f}%)"

        # -------------------------
        # Result normalizer
        # -------------------------
        def _first_row_as_dict(result, *, columns):
            if result is None:
                return None
            if not isinstance(result, (list, tuple)):
                return None
            if len(result) == 0:
                return None

            first = result[0]

            # dict-like row
            if isinstance(first, Mapping):
                return dict(first)

            # sequence-like row (tuple/list/Row/RowProxy/etc.), but avoid str/bytes
            if isinstance(first, Sequence) and not isinstance(first, (str, bytes, bytearray)):
                vals = list(first)
                if not columns:
                    return {"__row__": vals}
                return dict(zip(columns, vals))

            # last resort
            try:
                vals = list(first)
                if vals and columns:
                    return dict(zip(columns, vals))
                if vals:
                    return {"__row__": vals}
            except Exception:
                pass

            return None

        expected_cols = [
            "table_schema", "table_name", "engine", "table_collation", "row_format",
            "table_rows", "data_length", "index_length", "total_bytes",
            "column_count", "nullable_columns", "columns_with_default",
            "index_count", "unique_index_count", "avg_row_length"
        ]

        comparison_sql_template = """
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

                (SELECT COUNT(*)
                FROM information_schema.columns c
                WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name
                ) AS column_count,

                (SELECT SUM(c.is_nullable = 'YES')
                FROM information_schema.columns c
                WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name
                ) AS nullable_columns,

                (SELECT SUM(c.column_default IS NOT NULL)
                FROM information_schema.columns c
                WHERE c.table_schema = it.table_schema AND c.table_name = it.table_name
                ) AS columns_with_default,

                (SELECT COUNT(DISTINCT s.index_name)
                FROM information_schema.statistics s
                WHERE s.table_schema = it.table_schema AND s.table_name = it.table_name
                ) AS index_count,

                (SELECT COUNT(DISTINCT CASE WHEN s.non_unique = 0 THEN s.index_name END)
                FROM information_schema.statistics s
                WHERE s.table_schema = it.table_schema AND s.table_name = it.table_name
                ) AS unique_index_count,

                it.avg_row_length

            FROM information_schema.tables it
            WHERE it.table_schema = '%s'
            AND it.table_name = '%s'
            LIMIT 1;
        """

        def _secondary_exists_check(engine_name, schema_name, tname):
            sql = f"SHOW FULL TABLES FROM `{schema_name}` LIKE '{tname}';"
            try:
                env = self.envs.get(engine_name)
                raw = env.query_executor.execute(sql, schema_name=schema_name)
                if raw is None:
                    return False
                if isinstance(raw, dict) and raw.get("rows"):
                    return len(raw["rows"]) > 0
                if isinstance(raw, (list, tuple)):
                    return len(raw) > 0
                return False
            except Exception:
                return False

        def _fetch_side(engine_name, schema_name, tname):
            sql = comparison_sql_template % (schema_name, tname)
            env = self.envs.get(engine_name)
            raw = env.query_executor.execute(sql, schema_name=schema_name)
            row = _first_row_as_dict(raw, columns=expected_cols)

            if not raw:
                if _secondary_exists_check(engine_name, schema_name, tname):
                    print(f"\n⚠️ `{schema_name}`.`{tname}` exists on {engine_name}, but information_schema returned 0 rows.")
                else:
                    print(f"\n❌ Table not found: `{schema_name}`.`{tname}` on {engine_name}")
            elif row is None:
                print(f"\n⚠️ `{schema_name}`.`{tname}` found on {engine_name}, but could not map result row to dict.")
                print(f"Raw row type={type(raw[0])} value={raw[0]!r}")

            return raw, row

        def _exact_count(engine_name, schema_name, tname):
            sql = f"SELECT COUNT(*) AS cnt FROM `{schema_name}`.`{tname}`;"
            env = self.envs.get(engine_name)
            raw = env.query_executor.execute(sql, schema_name=schema_name)
            if not raw:
                return None
            first = raw[0]
            if isinstance(first, Mapping):
                return first.get("cnt")
            try:
                return list(first)[0]
            except Exception:
                return None

        # -------------------------
        # Execute both sides
        # -------------------------
        try:
            src_raw, src = _fetch_side(source_engine_name, source_schema_name, table_name)
        except Exception as e:
            self.status.error(f"❌ Source query failed on {source_engine_name}: {e}")
            return {"source": None, "target": None, "table": None, "df": None, "diffs": {"fatal": ["source_query_failed"]}}

        try:
            tgt_raw, tgt = _fetch_side(target_engine_name, target_schema_name, table_name)
        except Exception as e:
            self.status.error(f"❌ Target query failed on {target_engine_name}: {e}")
            return {"source": None, "target": None, "table": None, "df": None, "diffs": {"fatal": ["target_query_failed"]}}

        if not src or not tgt:
            return {"source": src, "target": tgt, "table": None, "df": None, "diffs": {"fatal": ["missing_or_unreadable_table_metadata"]}}

        # -------------------------
        # Exact row count (default)
        # -------------------------
        src["exact_row_count"] = _exact_count(source_engine_name, source_schema_name, table_name)
        tgt["exact_row_count"] = _exact_count(target_engine_name, target_schema_name, table_name)

        # -------------------------
        # Build comparison table rows
        # -------------------------
        def _row(metric, a, b, formatter, sev_on_diff, diff_formatter=None):
            diff = ""
            if a != b and diff_formatter is not None:
                diff = diff_formatter(a, b)
            return {
                "metric": metric,
                "source": formatter(a),
                "target": formatter(b),
                "diff": diff,
                "result": OK if a == b else sev_on_diff,
            }

        rows = []

        # Schema-ish (critical)
        rows.append(_row("engine", src.get("engine"), tgt.get("engine"), _fmt, ERR))
        rows.append(_row("collation", src.get("table_collation"), tgt.get("table_collation"), _fmt, ERR))
        rows.append(_row("row_format", src.get("row_format"), tgt.get("row_format"), _fmt, WARN))

        rows.append(_row("column_count", src.get("column_count"), tgt.get("column_count"), _fmt, ERR))
        rows.append(_row("nullable_columns", src.get("nullable_columns"), tgt.get("nullable_columns"), _fmt, ERR))
        rows.append(_row("columns_with_default", src.get("columns_with_default"), tgt.get("columns_with_default"), _fmt, WARN))

        # Indexes (usually warning)
        rows.append(_row("index_count", src.get("index_count"), tgt.get("index_count"), _fmt, WARN))
        rows.append(_row("unique_index_count", src.get("unique_index_count"), tgt.get("unique_index_count"), _fmt, WARN))

        # Exact count - tolerate small differences
        exact_a = src.get("exact_row_count")
        exact_b = tgt.get("exact_row_count")
        exact_status = _row_count_status(exact_a, exact_b, row_count_tolerance)
        rows.append({
            "metric": "row_count (exact)",
            "source": _fmt(exact_a),
            "target": _fmt(exact_b),
            "diff": _count_diff(exact_a, exact_b),
            "result": exact_status,
        })

        # Footprint (warning)
        rows.append(_row("data_length", src.get("data_length"), tgt.get("data_length"), _fmt_bytes, WARN))
        rows.append(_row("index_length", src.get("index_length"), tgt.get("index_length"), _fmt_bytes, WARN))
        rows.append(_row("total_bytes", src.get("total_bytes"), tgt.get("total_bytes"), _fmt_bytes, WARN))
        rows.append(_row("avg_row_length", src.get("avg_row_length"), tgt.get("avg_row_length"), _fmt, WARN))

        # -------------------------
        # Display as dataframe
        # -------------------------
        df = pd.DataFrame(rows).set_index("metric")[["source", "target", "diff", "result"]]

        # -------------------------
        # Emit soft self.status summary
        # -------------------------
        n_warn = int((df["result"] == WARN).sum())
        n_err  = int((df["result"] == ERR).sum())

        diffs = {"warning": [], "error": [], "fatal": []}
        for metric, r in df.iterrows():
            if r["result"] == WARN:
                diffs["warning"].append(f"{metric}: {r['source']} != {r['target']}")
            elif r["result"] == ERR:
                diffs["error"  ].append(f"{metric}: {r['source']} != {r['target']}")

        # Optionally suppress tables that only have warnings
        skip_output = ignore_warnings and n_err == 0
        if not skip_output:
            _print_header()
            with pd.option_context(
                "display.max_rows", 200,
                "display.max_colwidth", 120,
                "display.width", 200
            ):
                print(df.to_string())

            if n_err:
                print(f"\n🔴 {n_err} critical differences found. ({n_warn} warnings)")
            elif n_warn:
                print(f"\n🟡 {n_warn} warnings found.")
            else:
                print("\n🟢 No differences detected in metadata metrics.")

        return {"source": src, "target": tgt, "table": rows, "df": df, "diffs": diffs}

    #-----------------------------------------#
    # Method: Compare database across engines #
    #-----------------------------------------#
    def compare_databases(self, source_engine_name, source_schema_name,
                    target_engine_name, target_schema_name,
                    *, row_count_tolerance: float = 0.10,
                    ignore_warnings: bool = False):
        """
        Compare all tables in a database across two MySQL servers/schemas.
        Calls compare_tables() for each table and aggregates results.
        """
        self.status.info("🔎 Compare database across MySQL servers.")
        self.status.trace(f"Source ........... {source_engine_name} / {source_schema_name}")
        self.status.trace(f"Target ........... {target_engine_name} / {target_schema_name}")
        self.status.trace(f"'row_count_tolerance' is set to {row_count_tolerance * 100:.0f}%")

        source_env = self.envs.get(source_engine_name)
        target_env = self.envs.get(target_engine_name)
        source_tables = set(source_env.table.get_tables(source_schema_name))
        target_tables = set(target_env.table.get_tables(target_schema_name))
        all_tables = sorted(source_tables.union(target_tables))
        self.status.info(f"🔢 Found {len(source_tables)} tables in source, {len(target_tables)} tables in target, {len(all_tables)} total unique tables.")

        results = {}
        for table_name in all_tables:
            result = self.compare_tables(
                source_engine_name, source_schema_name,
                target_engine_name, target_schema_name,
                table_name,
                row_count_tolerance=row_count_tolerance,
                ignore_warnings=ignore_warnings,
            )
            results[table_name] = result

        self.status.success("✅ Done comparing database.")
        return results

    #-----------------------------------------------#
    # Method: Compare two tables by random sampling #
    #-----------------------------------------------#
    def get_random_primary_key_set(self, engine_name, schema_name, table_name, sample_size=100, partition_by=None, use_row_id=False):

        # Get the primary keys
        primary_keys = self.envs.get(engine_name).key.get_primary_keys(schema_name, table_name)

        # Using row_id?
        # Yes.
        if use_row_id:

            # Get maximum row_id -> FIX: add min row_id
            # print(f"SELECT MAX(row_id) FROM {schema_name}.{table_name}")
            env = self.envs.get(engine_name)
            max_row_id = env.query_executor.execute(f"SELECT COALESCE(MAX(row_id), 0) FROM {schema_name}.{table_name}", schema_name=schema_name)

            # Extract (and fix) the max_row_id value
            if type(max_row_id) == list and len(max_row_id) > 0:
                max_row_id = max_row_id[0][0]
            else:
                max_row_id = 0

            # Return empty set if no rows in the table
            if max_row_id == 0:
                return []

            # Generate random row_id set
            random_primary_key_set = sorted([random.randint(1, max_row_id) for _ in range(sample_size)])

            # Return empty set if no rows in the table
            if len(random_primary_key_set) == 0:
                return []

            # Fetch respective primary keys set
            random_primary_key_set = self.envs.get(engine_name).query_executor.execute(f"SELECT {', '.join(primary_keys)} FROM {schema_name}.{table_name} WHERE row_id IN ({', '.join([str(r) for r in random_primary_key_set])});", schema_name=schema_name)

        # No.
        else:

            # Generate the SQL query for sample tuples
            sql_query = f"SELECT {', '.join(primary_keys)} FROM {schema_name}.{table_name} ORDER BY RAND() LIMIT {sample_size};"

            # Generate the SQL query for sample tuples with partitioning
            if partition_by in primary_keys:

                # Fetch all object types
                partition_column_possible_vals = [r[0] for r in self.envs.get(engine_name).query_executor.execute(f"SELECT DISTINCT {partition_by} FROM {schema_name}.{table_name};", schema_name=schema_name)]

                # Loop over the object types
                sql_query_stack = []
                for colval in partition_column_possible_vals:
                    sql_query_stack += [
                        f"(SELECT {', '.join(primary_keys)} FROM {schema_name}.{table_name} WHERE object_type = '{colval}' ORDER BY RAND() LIMIT {round(sample_size/len(partition_column_possible_vals))})",
                    ]
                sql_query = ' UNION ALL '.join(sql_query_stack)

            # Execute the query
            random_primary_key_set = self.envs.get(engine_name).query_executor.execute(sql_query, schema_name=schema_name)

        # Return the random sample tuples
        return random_primary_key_set

    def get_random_uid_set(self, engine_name, schema_name, table_name, sample_size=100, partition_by=None, use_row_id=False):
        """
        Return a random sample of tuples for the unique key named 'uid'.
        Similar to get_random_primary_key_set, but uses the columns of the
        'uid' index instead of the PRIMARY KEY columns.
        """

        # Get the columns that make up the 'uid' unique key
        keys = self.envs.get(engine_name).key.get_keys(schema_name, table_name)
        uid_columns = keys.get('uid', [])
        if not uid_columns:
            return []

        # Quote identifiers safely
        q_schema = self._q(schema_name)
        q_table = self._q(table_name)
        q_uid_columns = [self._q(c) for c in uid_columns]

        # Using row_id?
        # Yes.
        if use_row_id:

            # Get maximum row_id
            env = self.envs.get(engine_name)
            max_row_id = env.query_executor.execute(f"SELECT COALESCE(MAX(row_id), 0) FROM {q_schema}.{q_table}", schema_name=schema_name)

            # Extract (and fix) the max_row_id value
            if type(max_row_id) == list and len(max_row_id) > 0:
                max_row_id = max_row_id[0][0]
            else:
                max_row_id = 0

            # Return empty set if no rows in the table
            if max_row_id == 0:
                return []

            # Generate random row_id set
            random_row_id_set = sorted([random.randint(1, max_row_id) for _ in range(sample_size)])

            # Return empty set if no rows in the table
            if len(random_row_id_set) == 0:
                return []

            # Fetch respective uid tuples set
            random_uid_set = self.envs.get(engine_name).query_executor.execute(f"SELECT {', '.join(q_uid_columns)} FROM {q_schema}.{q_table} WHERE row_id IN ({', '.join([str(r) for r in random_row_id_set])});", schema_name=schema_name)

        # No.
        else:

            # Generate the SQL query for sample tuples
            sql_query = f"SELECT {', '.join(q_uid_columns)} FROM {q_schema}.{q_table} ORDER BY RAND() LIMIT {sample_size};"

            # Generate the SQL query for sample tuples with partitioning
            if partition_by in uid_columns:

                # Fetch all partition values
                partition_column_possible_vals = [r[0] for r in self.envs.get(engine_name).query_executor.execute(f"SELECT DISTINCT {self._q(partition_by)} FROM {q_schema}.{q_table};", schema_name=schema_name)]

                # Loop over the partition values
                n_partitions = len(partition_column_possible_vals)
                if n_partitions > 0:
                    per_partition = round(sample_size / n_partitions)
                    sql_query_stack = []
                    for colval in partition_column_possible_vals:
                        sql_query_stack.append(
                            f"(SELECT {', '.join(q_uid_columns)} FROM {q_schema}.{q_table} WHERE {self._q(partition_by)} = '{colval}' ORDER BY RAND() LIMIT {per_partition})"
                        )
                    sql_query = ' UNION ALL '.join(sql_query_stack)

            # Execute the query
            random_uid_set = self.envs.get(engine_name).query_executor.execute(sql_query, schema_name=schema_name)

        # Return the random sample tuples
        return random_uid_set

    #-----------------------------------------------#
    # Method: Compare two tables by random sampling #
    #-----------------------------------------------#
    def get_rows_by_primary_key_set(self, engine_name, schema_name, table_name, primary_key_set, return_as_dict=False):

        # Get the primary keys
        env = self.envs.get(engine_name)
        primary_keys = env.key.get_primary_keys(schema_name, table_name)

        # Get the column names
        all_columns = env.column.get_column_names(schema_name, table_name)

        # Columns used for data comparison: exclude row_id and primary keys
        data_columns = [c for c in all_columns if c != 'row_id' and c not in primary_keys]

        # SELECT primary keys first, then data columns, so row offsets align
        select_columns = list(dict.fromkeys(primary_keys + data_columns))

        # Generate the SQL query for sample tuples
        def _format_pk(pk_values):
            if len(pk_values) == 1:
                return str(pk_values[0])
            return f"({', '.join(str(v) for v in pk_values)})"

        pk_placeholders = ', '.join(_format_pk(pk) for pk in primary_key_set)
        sql_query = f"SELECT {', '.join(select_columns)} FROM {schema_name}.{table_name} WHERE ({', '.join(primary_keys)}) IN ({pk_placeholders});"

        # Execute the query
        row_set = self.envs.get(engine_name).query_executor.execute(sql_query, schema_name=schema_name)

        # Return as list of tuples
        if not return_as_dict:
            return row_set

        # Convert to dictionary in format {primary_key: {column_name: value}}
        row_set_dict = {tuple(r[0:len(primary_keys)]): dict(zip(data_columns, r[len(primary_keys):])) for r in row_set}

        # Execute the query
        return row_set_dict

    #-----------------------------------------------#
    # Method: Compare two tables by random sampling #
    #-----------------------------------------------#
    def get_rows_by_uid_set(self, engine_name, schema_name, table_name, uid_set, return_as_dict=False):
        """
        Fetch rows from a table using tuples from the 'uid' unique key.
        Similar to get_rows_by_primary_key_set, but uses the columns of the
        'uid' index instead of the PRIMARY KEY columns.
        """

        # Get the columns that make up the 'uid' unique key
        env = self.envs.get(engine_name)
        keys = env.key.get_keys(schema_name, table_name)
        uid_columns = keys.get('uid', [])
        if not uid_columns:
            return [] if not return_as_dict else {}

        # Get all column names
        all_columns = env.column.get_column_names(schema_name, table_name)

        # Columns used for data comparison: exclude row_id and uid columns
        data_columns = [c for c in all_columns if c != 'row_id' and c not in uid_columns]

        # SELECT uid columns first, then data columns, so row offsets align
        select_columns = list(dict.fromkeys(uid_columns + data_columns))

        # Quote identifiers safely
        q_schema = self._q(schema_name)
        q_table = self._q(table_name)
        q_uid_columns = [self._q(c) for c in uid_columns]
        q_select_columns = [self._q(c) for c in select_columns]

        # Helper to format a Python value as a SQL literal
        def _sql_literal(v):
            if v is None:
                return 'NULL'
            if isinstance(v, bool):
                return '1' if v else '0'
            if isinstance(v, (int, float)):
                return str(v)
            return "'" + str(v).replace("'", "''") + "'"

        # Generate the SQL query for sample tuples
        def _format_uid(uid_values):
            if len(uid_values) == 1:
                return _sql_literal(uid_values[0])
            return f"({', '.join(_sql_literal(v) for v in uid_values)})"

        if not uid_set:
            return [] if not return_as_dict else {}

        uid_placeholders = ', '.join(_format_uid(uid) for uid in uid_set)
        sql_query = f"SELECT {', '.join(q_select_columns)} FROM {q_schema}.{q_table} WHERE ({', '.join(q_uid_columns)}) IN ({uid_placeholders});"

        # Execute the query
        row_set = self.envs.get(engine_name).query_executor.execute(sql_query, schema_name=schema_name)

        # Return as list of tuples
        if not return_as_dict:
            return row_set

        # Convert to dictionary in format {uid_tuple: {column_name: value}}
        row_set_dict = {tuple(r[0:len(uid_columns)]): dict(zip(data_columns, r[len(uid_columns):])) for r in row_set}

        return row_set_dict

    #-----------------------------------------------#
    # Method: Compare two tables by random sampling #
    #-----------------------------------------------#
    def compare_tables_by_random_sampling(self, source_engine_name, source_schema_name, source_table_name, target_engine_name, target_schema_name, target_table_name, sample_size=1024):

        # Check if the source table exists
        if not self.envs.get(source_engine_name).table.table_exists(source_schema_name, source_table_name):
            self.status.error(f"🚨 Table {source_schema_name}.{source_table_name} does not exist in '{source_engine_name}'.")
            return

        # Check if the target table exists
        if not self.envs.get(target_engine_name).table.table_exists(target_schema_name, target_table_name):
            self.status.error(f"🚨 Table {target_schema_name}.{target_table_name} does not exist in '{target_engine_name}'.")
            return

        # # Detect table type
        # table_type = _get_table_type_from_name(source_table_name)
        # if table_type == 'doc_profile':
        #     pass

        # Print a clear header for this table comparison
        header_label = f" {target_table_name} "
        print('')
        print('╔' + '═' * 78 + '╗')
        print('║' + header_label.center(78) + '║')
        print('╚' + '═' * 78 + '╝')

        #------------------------------------------#
        # Generate the SQL query for sample tuples #
        #------------------------------------------#

        # This comparison requires a 'uid' unique key to reliably match rows
        # across schemas, because row_id-based matching is not reliable when
        # auto-increment values differ between environments.
        source_keys = self.envs.get(source_engine_name).key.get_keys(source_schema_name, source_table_name)
        if 'uid' not in source_keys:
            self.status.error(f"🚨 Table {source_schema_name}.{source_table_name} does not have a 'uid' unique key. Cannot compare by uid.")
            return

        target_keys = self.envs.get(target_engine_name).key.get_keys(target_schema_name, target_table_name)
        if 'uid' not in target_keys:
            self.status.error(f"🚨 Table {target_schema_name}.{target_table_name} does not have a 'uid' unique key. Cannot compare by uid.")
            return

        source_uid_columns = source_keys.get('uid', [])
        target_uid_columns = target_keys.get('uid', [])
        if source_uid_columns != target_uid_columns:
            self.status.error(
                f"🚨 UID key mismatch: source uses {source_uid_columns}, target uses {target_uid_columns}. "
                "Cannot compare tables with different uid keys."
            )
            return

        # Get random uid tuple set
        random_key_set  = self.get_random_uid_set(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name, sample_size=round(sample_size/2), partition_by=None, use_row_id=False)
        random_key_set += self.get_random_uid_set(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name, sample_size=round(sample_size/2), partition_by=None, use_row_id=False)

        # Defensive: every uid tuple must have the same arity as the uid key
        expected_uid_len = len(source_uid_columns)
        bad_uid_count = sum(1 for uid in random_key_set if len(uid) != expected_uid_len)
        if bad_uid_count:
            self.status.error(
                f"🚨 Found {bad_uid_count} uid tuple(s) with unexpected arity (expected {expected_uid_len} columns). "
                "Skipping mismatched tuples."
            )
            random_key_set = [uid for uid in random_key_set if len(uid) == expected_uid_len]

        # Return if no rows found
        if len(random_key_set) == 0:
            print(f"⚠️  No rows found in either source or target table for comparison.")
            return

        # Get the rows by uid set (source and target)
        source_row_set_dict = self.get_rows_by_uid_set(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name, uid_set=random_key_set, return_as_dict=True)
        target_row_set_dict = self.get_rows_by_uid_set(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name, uid_set=random_key_set, return_as_dict=True)

        # Get unique set of tuples
        unique_tuples  = list(set(source_row_set_dict.keys()).union(set(target_row_set_dict.keys())))

        # Update the sample size
        sample_size = len(unique_tuples)

        # Initialise stats dictionary
        stats = {
            'new_rows': 0,
            'deleted_rows': 0,
            'existing_rows': 0,
            'mismatch': 0,
            'custom_column_mismatch': 0,
            'match': 0,
            'set_to_null': 0,
            'percent_new_rows': 0,
            'percent_deleted_rows': 0,
            'percent_existing_rows': 0,
            'percent_mismatch': 0,
            'percent_custom_column_mismatch': 0,
            'percent_match': 0,
            'percent_set_to_null': 0,
            'mismatch_by_column': {}
        }

        # Initialise stacks
        mismatch_changes_stack = []

        # Initialise score and rank differences
        score_rank_diffs = {
            'semantic_score': [],
            'degree_score': [],
            'row_rank': []
        }

        #----------------------------#
        # Analyse comparison results #
        #----------------------------#

        # Initialise test results
        test_results = {
            'flawless_match_test' : False,
            'deleted_rows_test' : True,
            'column_missing_or_renamed_test' : True,
            'custom_column_mismatch_test' : True,
            'set_to_null_test' : True,
            'median_score_diff_test' : True,
            'warning_flag' : False
        }

        # Initialise column missing or renamed list
        column_missing_or_renamed_list = []

        # Loop over the unique tuples
        for t in unique_tuples:

            # Check if the tuple is new
            if t in source_row_set_dict and t not in target_row_set_dict:
                stats['new_rows'] += 1

            # Check if the tuple is deleted
            elif t not in source_row_set_dict and t in target_row_set_dict:
                stats['deleted_rows'] += 1

            # Check if the tuple is in both source and target (existing row)
            if t in source_row_set_dict and t in target_row_set_dict:

                # Add to existing rows
                stats['existing_rows'] += 1

                # Check if the values fully match
                if source_row_set_dict[t] == target_row_set_dict[t]:
                    stats['match'] += 1

                # Else, analyse the differences
                else:

                    # Initialise flags
                    exact_row_mismatch_detected = False
                    custom_column_mismatch_detected = False
                    set_to_null_detected = False

                    # Loop over all columns from both source and target
                    for k in set(source_row_set_dict[t]) | set(target_row_set_dict[t]):

                        # Check if the key is in both source and target
                        if k not in source_row_set_dict[t] or k not in target_row_set_dict[t]:

                            # Add column existance mismatch to list
                            column_missing_or_renamed_list.append(k)
                            column_missing_or_renamed_list = sorted(list(set(column_missing_or_renamed_list)))

                        # Else, analyse values in matching columns
                        else:

                            # Check if column exists in stats dictionary
                            if k not in stats['mismatch_by_column']:
                                stats['mismatch_by_column'][k] = 0

                            # Check if the values are different in matching columns
                            if source_row_set_dict[t][k] != target_row_set_dict[t][k]:

                                # Flag mismatch detected
                                exact_row_mismatch_detected = True

                                # Increment the mismatch counter
                                stats['mismatch_by_column'][k] += 1

                                # Check if custom column mismatch detected
                                if k not in ['row_rank', 'row_score', 'semantic_score', 'degree_score', 'object_created', 'object_updated']:

                                    # Flag custom column mismatch detected
                                    custom_column_mismatch_detected = True

                                    # Append the mismatch changes stack
                                    mismatch_changes_stack += [(f'{k}: [S] {str(source_row_set_dict[t][k])[0:64]} ... [T] {str(target_row_set_dict[t][k])[0:64]}')]

                                # Check if the value is set to NULL from source to target
                                if source_row_set_dict[t][k] is None:
                                    set_to_null_detected = True

                            # Append score and rank differences to list
                            if k in score_rank_diffs:
                                score_rank_diffs[k] += [source_row_set_dict[t][k] - target_row_set_dict[t][k]]

                    # Increment the mismatch counters based on flags
                    if exact_row_mismatch_detected:
                        stats['mismatch'] += 1
                    if custom_column_mismatch_detected:
                        stats['custom_column_mismatch'] += 1
                    if set_to_null_detected:
                        stats['set_to_null'] += 1

        # Calculate the percentages
        try:
            stats['percent_existing_rows'] = stats['existing_rows'] / sample_size * 100
            stats['percent_new_rows']      = stats['new_rows'     ] / sample_size * 100
            stats['percent_deleted_rows']  = stats['deleted_rows' ] / sample_size * 100

            if stats['existing_rows'] > 0:
                stats['percent_mismatch']      = stats['mismatch'     ] / stats['existing_rows'] * 100
                stats['percent_match']         = stats['match'        ] / stats['existing_rows'] * 100
                # stats['percent_set_to_null']   = stats['set_to_null'  ] / stats['existing_rows'] * 100
            else:
                stats['percent_mismatch']    = 0
                stats['percent_match']       = 0
                # stats['percent_set_to_null'] = 0

            if stats['mismatch'] > 0:
                stats['percent_custom_column_mismatch'] = stats['custom_column_mismatch'] / stats['mismatch'] * 100
                stats['percent_set_to_null'] = stats['set_to_null'] / stats['mismatch'] * 100
            else:
                stats['percent_custom_column_mismatch'] = 0
                stats['percent_set_to_null'] = 0
        except ZeroDivisionError:
            print('ZeroDivisionError')
            print('sample_size:', sample_size)
            print('stats dict:')
            print(json.dumps(stats, indent=2, default=str))
            raise

        # print("\033[31mThis is red text\033[0m")
        # print("\033[32mThis is green text\033[0m")
        # print("\033[34mThis is blue text\033[0m")
        # print("\033[33mThis is yellow text\033[0m")
        # print("\033[35mThis is purple text\033[0m")
        # print("\033[36mThis is cyan text\033[0m")
        # print("\033[37mThis is white text\033[0m")
        # print("\033[1;31mThis is bold red text\033[0m")

        # Flawless match test
        if stats['percent_match'] == 100:
            test_results['flawless_match_test'] = True
            print(f"🚀 \033[32mFlawless match test passed for {target_table_name}.\033[0m")
            return

        # Generate print colours
        if stats['percent_deleted_rows'] >= 25:
            percent_deleted_rows_colour = '\033[31m'
            test_results['deleted_rows_test'] = False
        elif stats['percent_deleted_rows'] >= 10:
            percent_deleted_rows_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_deleted_rows_colour = '\033[37m'

        if stats['percent_mismatch'] >= 10:
            percent_mismatch_colour = '\033[33m'
        elif stats['percent_mismatch'] >= 5:
            percent_mismatch_colour = '\033[33m'
        else:
            percent_mismatch_colour = '\033[37m'

        if stats['percent_custom_column_mismatch'] >= 10:
            percent_custom_column_mismatch_colour = '\033[31m'
            test_results['custom_column_mismatch_test'] = False
        elif stats['percent_custom_column_mismatch'] >= 5:
            percent_custom_column_mismatch_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_custom_column_mismatch_colour = '\033[37m'

        if stats['percent_set_to_null'] >= 10:
            percent_set_to_null_colour = '\033[31m'
            test_results['set_to_null_test'] = False
        elif stats['percent_set_to_null'] >= 5:
            percent_set_to_null_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_set_to_null_colour = '\033[37m'

        # Print the stats
        print('')
        print(f" - Sample size ....... {sample_size}")
        print(f" - Existing rows ..... {stats['existing_rows']} {' '*(8-len(str(stats['existing_rows'])))} {stats['percent_existing_rows']:.1f}%")
        print(f" - New rows .......... {stats['new_rows']     } {' '*(8-len(str(stats['new_rows'])))     } {stats['percent_new_rows'     ]:.1f}%")
        print(f"{percent_deleted_rows_colour} - Deleted rows ...... {stats['deleted_rows'] } {' '*(8-len(str(stats['deleted_rows']))) } {stats['percent_deleted_rows' ]:.1f}% \033[0m")
        print('')
        print(f" - Match ............. {stats['match']        } {' '*(8-len(str(stats['match'])))        } {stats['percent_match'        ]:.1f}%")
        print(f"{percent_mismatch_colour} - Mismatch .......... {stats['mismatch']     } {' '*(8-len(str(stats['mismatch'])))     } {stats['percent_mismatch'     ]:.1f}% \033[0m")
        print(f"{percent_custom_column_mismatch_colour} - (custom columns) .. {stats['custom_column_mismatch']  } {' '*(8-len(str(stats['custom_column_mismatch']))  )} {stats['percent_custom_column_mismatch'  ]:.1f}% \033[0m")
        print(f"{percent_set_to_null_colour} - Set to NULL ....... {stats['set_to_null']  } {' '*(8-len(str(stats['set_to_null'])))  } {stats['percent_set_to_null'  ]:.1f}% \033[0m")
        print('')
        if len(stats['mismatch_by_column']) > 0:
            print('Mismatch(s) by column:')
            for column in stats['mismatch_by_column']:
                if stats['mismatch_by_column'][column] == 0:
                    print(f"\t- {column} {'.'*(64-len(column))} {stats['mismatch_by_column'][column]}")
                else:
                    if column in ['row_rank', 'row_score', 'semantic_score', 'degree_score', 'object_created', 'object_updated']:
                        print(f"\033[33m\t- {column} {'.'*(64-len(column))} {stats['mismatch_by_column'][column]}\033[0m")
                    else:
                        print(f"\033[31m\t- {column} {'.'*(64-len(column))} {stats['mismatch_by_column'][column]}\033[0m")
            print('')

        # Print score and rank average differences
        if len(score_rank_diffs['semantic_score'])>0 or len(score_rank_diffs['degree_score'])>0 or len(score_rank_diffs['row_rank'])>0:
            print('Median score and rank differences:')
            for k in score_rank_diffs:
                if score_rank_diffs[k]:
                    # avg_val = sum(score_rank_diffs[k])/len(score_rank_diffs[k])
                    med_val = np.median(score_rank_diffs[k])
                    if   k in ['semantic_score', 'degree_score'] and abs(med_val)>=0.2:
                        test_results['median_score_diff_test'] = False
                        print(f"\033[31m\t- {k}: {med_val:.2f}\033[0m")
                    elif k in ['semantic_score', 'degree_score'] and abs(med_val)>=0.1:
                        test_results['warning_flag'] = True
                        print(f"\033[33m\t- {k}: {med_val:.2f}\033[0m")
                    else:
                        print(f"\t- {k}: {med_val:.2f}")
            print('')

        if len(column_missing_or_renamed_list) > 0:
            test_results['column_missing_or_renamed_test'] = False
            print(f"\033[31mColumn mismatch(s) detected:\033[0m {column_missing_or_renamed_list}")
            print('')

        # Print the first 3 mismatch changes
        if len(mismatch_changes_stack) > 0:
            mismatch_changes_stack = list(set(mismatch_changes_stack))
            # randomize
            mismatch_changes_stack = random.sample(mismatch_changes_stack, len(mismatch_changes_stack))
            print('Example mismatch changes:')
            for n,r in enumerate(mismatch_changes_stack):
                print('\t-', r)
                if n==32:
                    break
            print('')

        #----------------------------------------------------#
        # Calculate conditions for passing the test (or not) #
        #----------------------------------------------------#

        print('')
        if stats['existing_rows'] == 0 and stats['new_rows'] > 0 and stats['deleted_rows'] == 0:
            print("Test result: \033[33mTarget table is empty. All rows are new.\033[0m")
        elif stats['existing_rows'] == 0 and stats['deleted_rows'] > 0 and stats['new_rows'] == 0:
            print("Test result: \033[31mSource table is empty. All rows risk being deleted in target.\033[0m")
        elif test_results['deleted_rows_test'] and test_results['column_missing_or_renamed_test'] and test_results['custom_column_mismatch_test'] and test_results['set_to_null_test'] and test_results['median_score_diff_test']:
            if test_results['warning_flag']:
                print("Test result: \033[33mMinor changes detected.\033[0m")
            else:
                print("Test result: \033[32mNo significant changes detected.\033[0m")
        else:
            print("Test result: \033[31mMajor changes detected!\033[0m")
        print('')

        time.sleep(1)

#======================================#
# Run as standalone script for testing #
#======================================#
if __name__ == "__main__":

    # Load the configuration from the default file
    config = GraphDBConfig.from_default_file()

    # Initialize the Environments class with the configuration
    envs = Environments(config)

    # Initialize the CompareOperations class
    ops = CompareOperations(envs)

    # Run with example parameters
    ops.compare_tables_by_random_sampling(
        source_engine_name = 'xaas_coresrv',
        source_schema_name = 'graphsearch_test',
        source_table_name  = 'Data_N_Object_T_PageProfile',
        target_engine_name = 'xaas_prod',
        target_schema_name = 'graphsearch_prod_2025_11_05',
        target_table_name  = 'Data_N_Object_T_PageProfile',
        sample_size        = 1024
    )
