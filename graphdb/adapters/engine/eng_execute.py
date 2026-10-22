# graphdb-client/graphdb/adapters/engine/eng_execute.py
"""Full engine-name-addressed query execution adapter for a single environment.

Implements the complete ``EngineExecutePort`` surface (plus the thin
``SQLAlchemyQueryExecutorPort`` methods) by composing two capabilities of one
environment: the SQLAlchemy engine for in-process SQL and the MySQL CLI
gateway for shell/file transport. The chunking, safe-insert and upsert
SQL-building logic is carried over verbatim from the pre-hexagonal adapter
so bulk-data behavior cannot drift between consumers.

``engine_name`` arguments are kept for port compatibility with the legacy
multi-environment facade; the adapter enforces that they match the
environment it was built for, turning wrong-environment bugs into loud
errors instead of silent cross-environment writes.
"""
from __future__ import annotations

# Heavy data/rendering imports are intentional: verbose mode renders SQL and
# evaluation DataFrames through the shared renderers, and the CLI gateway
# owns all shell/file transport.
import json
import os
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd
from loguru import logger as sysmsg
from sqlalchemy import text
from sqlalchemy.dialects.mysql import dialect as MySQLDialect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from tqdm import tqdm
from graphdb.adapters.gateways.gtw_dbclient import BaseDBClientGateway
from graphdb.adapters.rendering.rdr_dataframe import print_dataframe
from graphdb.adapters.rendering.rdr_print import print_colour
from graphdb.adapters.rendering.rdr_sqlquery import print_sql

#==================#
# Class Definition #
#==================#
class EngineExecuteAdapter:
    """Engine-name-addressed executor composing the SQLAlchemy engine and the MySQL CLI gateway."""

    # Internal Method: Store the environment's engine and CLI gateway for later use.
    def __init__(self, engine: Engine, mysql_client: BaseDBClientGateway, env_name: str = "default") -> None:
        self.engine = engine
        self._mysql_client = mysql_client
        self.env_name = env_name

    # Internal Method: Validate the engine-name argument against this adapter's environment.
    def _ensure_environment(self, engine_name: Optional[str]) -> None:
        # A per-environment adapter must never silently execute against a
        # different environment than the one it was constructed for.
        if engine_name is not None and engine_name != self.env_name:
            raise ValueError(
                f"EngineExecuteAdapter for environment '{self.env_name}' "
                f"received mismatched engine_name '{engine_name}'."
            )

    # Internal Method: Backtick-quote a single SQL identifier, escaping embedded backticks.
    @staticmethod
    def _q(name: str) -> str:
        return f"`{name.replace('`', '``')}`"

    # Internal Method: Backtick-quote a schema-qualified table name.
    @staticmethod
    def _qt(schema_name: str, table_name: str) -> str:
        return f"`{schema_name}`.`{table_name}`"

    # Internal Method: Normalize NULL/NaN-like strings to real NULLs before binding.
    @staticmethod
    def _normalize_sql_value(value):
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if stripped in {"NULL", "null", "NaN", "nan"}:
                return None
            return value
        return value

    # Public Method: Execute a single query through the SQLAlchemy engine.
    def execute_query(self, engine_name, query, schema_name=None, params=None, commit=False, return_exception=False, verbose=False, query_id=None) -> Union[List[Any], Tuple[str, str, Any]]:

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # If verbose is enabled, print the command being executed
        if verbose:
            print_sql(query, title=f"Executing query{f' [{query_id}]' if query_id else ''}")

        # Open a transaction-scoped connection when committing; read-only otherwise.
        connection_ctx = self.engine.begin() if commit else self.engine.connect()
        try:
            with connection_ctx as connection:
                if schema_name:
                    connection.execute(text(f"USE {self._q(schema_name)}"))
                result = connection.execute(text(query), parameters=params or {})
                if result.returns_rows:
                    rows = list(result.fetchall())
                else:
                    rows = []
        except (DataError, IntegrityError, SQLAlchemyError) as e:
            if return_exception:
                # Return a (type, message, dbapi_code) tuple for callers that
                # inspect failures instead of unwinding (e.g. duplicate keys).
                error_type = type(e).__name__      # e.g. "DataError"
                error_message = str(e)             # human-readable
                dbapi_code = getattr(e.orig, "args", [None])[0] if hasattr(e, "orig") else None
                return error_type, error_message, dbapi_code
            else:
                print(f"\033[91mError executing query{f' [{query_id}]' if query_id else ''}.\033[0m")
                print(e)
                raise
        return rows

    # Public Method: Stream SELECT results row by row into a JSON-lines file.
    def execute_query_stream_to_file(self, engine_name, query, schema_name=None, params=None, *, fetch_size=1000, output_file=None, verbose=False, query_id=None):

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # Fail fast when the caller forgot the destination file.
        if not output_file:
            raise ValueError("output_file must be provided")

        # If verbose is enabled, print the command being executed
        if verbose:
            print_sql(query, title=f"Executing query in streaming mode{f' [{query_id}]' if query_id else ''}")

        # Create the destination directory so streaming can open the file directly.
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)

        # Hold one connection for the whole stream and release it in finally.
        connection = self.engine.connect()
        try:
            if schema_name:
                connection.execute(text(f"USE {self._q(schema_name)}"))

            # Server-side cursors keep memory flat for multi-million-row results.
            exec_conn = connection.execution_options(stream_results=True)
            result = exec_conn.execute(text(query), parameters=params)

            # Only SELECT-style results produce rows to stream.
            if not result.returns_rows:
                raise ValueError("execute_query_stream_to_file only supports SELECT queries")

            # Write each row as a JSON line so downstream readers can consume incrementally.
            with open(output_file, "w", encoding="utf-8") as f:
                while True:
                    chunk = result.fetchmany(fetch_size)
                    if not chunk:
                        break
                    for row in chunk:
                        f.write(json.dumps(dict(row._mapping), ensure_ascii=False, default=str) + "\n")

        # Invalidate the pooled connection on memory blowups so poisoned state is never reused.
        except MemoryError:
            try:
                connection.invalidate()
            except Exception:
                pass
            raise

        # Surface driver errors with context, then re-raise for caller handling.
        except (DataError, IntegrityError, SQLAlchemyError) as e:
            print(f"\033[91mError executing query{f' [{query_id}]' if query_id else ''}.\033[0m")
            print(e)
            raise

        # Always return the connection to the pool, even after failures.
        finally:
            try:
                connection.close()
            except Exception:
                pass

    # Public Method: Execute a single-row upsert safely with optional eval output.
    def execute_upsert_row(self, engine_name, schema_name, table_name, key_column_names, key_column_values, upd_column_names, upd_column_values, actions=()):
        """
        Possible actions: 'print', 'eval', 'commit'
        """

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # Generate the full table name
        t = f'{schema_name}.{table_name}'

        # Get the number of columns to update and create the dictionary with normalized values
        num_upd_columns = len(upd_column_names)
        num_key_columns = len(key_column_names)

        # Bind normalized key and update values as named SQL parameters.
        sql_params = {
            key_column_names[k]: self._normalize_sql_value(key_column_values[k])
            for k in range(num_key_columns)
        }
        sql_params.update({
            upd_column_names[u]: self._normalize_sql_value(upd_column_values[u])
            for u in range(num_upd_columns)
        })

        # Initialise test results dictionary
        eval_results = None

        # Evaluate changes to be made
        if 'eval' in actions:

            # Define the colour map
            colour_map = {
                'no change'  : 'green',
                'new value'  : 'cyan',
                'set to null': 'red',
                'key exists' : 'green',
                'key is new' : 'cyan'
            }

            # Generate SELECT statement
            if num_upd_columns > 0:
                if_statements = []
                for k in range(num_upd_columns):
                    if isinstance(upd_column_values[k], float):
                        if_statements.append(
                            f'IF('
                                f'ABS({upd_column_names[k]} - :{upd_column_names[k]})<1e-6 '
                                    f'OR (:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NULL), '
                                f'"no change", '
                                f'IF(:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NOT NULL, "set to null", "new value")'
                            f') AS TEST_{upd_column_names[k]}'
                        )
                    else:
                        if_statements.append(
                            f'IF('
                                f'({upd_column_names[k]} = :{upd_column_names[k]}) '
                                    f'OR (:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NULL), '
                                f'"no change", '
                                f'IF(:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NOT NULL, "set to null", "new value")'
                            f') AS TEST_{upd_column_names[k]}'
                        )
                sql_select_statement = ', '.join(if_statements)
            else:
                sql_select_statement = '*'

            # Generate the SQL query for evaluation
            sql_query_eval = f"""
                SELECT {sql_select_statement}
                  FROM {t}
                 WHERE ({', '.join(key_column_names)}) = (:{', :'.join(key_column_names)});
            """

            # Print the SQL query
            if 'print' in actions:
                print(sql_query_eval)

            # Execute the query
            out = self.execute_query(engine_name=engine_name, query=sql_query_eval, params=sql_params)

            # Build up the test results dictionary
            print_colour(f'\nChanges on table {t}:', style='bold')
            eval_result = 'key exists' if len(out) > 0 else 'key is new'
            eval_results = [{'column': 'primary_key', 'result': eval_result}]
            print(f"primary_key {'.'*(48-len('primary_key'))} ", end="", flush=True)
            print_colour(eval_result, colour=colour_map[eval_result])
            if len(out) > 0:
                for k in range(num_upd_columns):
                    eval_result = out[0][k]
                    eval_results.append({'column': upd_column_names[k], 'result': eval_result})
                    print(f"{upd_column_names[k]} {'.'*(48-len(upd_column_names[k]))} ", end="", flush=True)
                    print_colour(eval_result, colour=colour_map[eval_result])

        # Generate the SQL query for commit
        if num_upd_columns > 0:
            sql_query_commit = f"""
                INSERT INTO {t}
                    ({', '.join(key_column_names)}, {', '.join(upd_column_names)})
                SELECT {', '.join(key_column_names)}, {', '.join(upd_column_names)}
                FROM (
                    SELECT
                        {', '.join([f':{key_column_names[k]} AS {key_column_names[k]}' for k in range(num_key_columns)])},
                        {', '.join([f':{upd_column_names[u]} AS {upd_column_names[u]}' for u in range(num_upd_columns)])}
                ) AS d
                ON DUPLICATE KEY UPDATE
                    record_updated_date = IF(
                        {' OR '.join([f"COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__')" for c in upd_column_names])},
                        CURRENT_TIMESTAMP,
                        {t}.record_updated_date
                    ),
                    {', '.join(
                        [f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})" for c in upd_column_names]
                    )};"""
        else:
            sql_query_commit = f"""
                INSERT IGNORE INTO {t}
                    ({', '.join(key_column_names)})
                SELECT
                    {', '.join([f':{key_column_names[k]} AS {key_column_names[k]}' for k in range(num_key_columns)])};"""

        # Print the SQL query
        if 'print' in actions:
            stmt = text(sql_query_commit).bindparams(**sql_params)
            print(stmt.compile(
                dialect        = MySQLDialect(),
                compile_kwargs = {"literal_binds": True}
            ))

        # Execute commit
        if 'commit' in actions:
            out = self.execute_query(engine_name=engine_name, query=sql_query_commit, params=sql_params, commit=True, return_exception=True)
            if not type(out) is list:
                error_type, error_msg, dbapi_code = out
                if dbapi_code==1062: # Duplicate entry
                    sysmsg.warning(f'Duplicate entry error when inserting into {t} with keys {sql_params}. Continuing ...')
                else:
                    sysmsg.critical(f'Error when inserting into {t} with keys {sql_params}. Exiting ...')
                    print('Error details:')
                    print(f'{error_type}: {error_msg} (DBAPI code: {dbapi_code})')
                    exit()

        # Return the test results
        return eval_results

    # Public Method: Evaluate and patch a query's rows via ON DUPLICATE KEY UPDATE.
    def execute_query_as_safe_inserts(self, engine_name, schema_name, table_name, query, key_column_names, upd_column_names, eval_column_names=None, actions=(), verbose=False, query_id=None):

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # Target table path
        t = target_table_path = f'{schema_name}.{table_name}'

        # Evaluate the patch operation
        if 'eval' in actions:

            # Generate evaluation query
            query_eval = f"""
                SELECT t.{', t.'.join(eval_column_names)},
                       COUNT(*) AS n_to_process,
                       SUM({' OR '.join([f"COALESCE(t.{c}, '__null__') != COALESCE(j.{c}, '__null__')" for c in upd_column_names])}) AS n_to_patch
                  FROM (
                        {query}
                       ) t
             LEFT JOIN {target_table_path} j
                    ON {' AND '.join([f"t.{c} = j.{c}" for c in key_column_names])}
              GROUP BY t.{', t.'.join(eval_column_names)}
            """

            # If verbose is enabled, print the command being executed
            if verbose or 'print' in actions:
                print_sql(query_eval, title=f"Executing query as safe inserts{f' [{query_id}][eval]' if query_id else ''}")

            # Execute the evaluation query and print the results
            out = self.execute_query(engine_name=engine_name, query=query_eval)
            if len(out) > 0:
                df = pd.DataFrame(out, columns=eval_column_names+['rows to process', 'rows to patch'])
                print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')

        # Generate the SQL commit query
        query_commit = f"""
                 INSERT INTO {target_table_path}
                             ({', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)})
                      SELECT  {', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)}
                        FROM (
                              {query}
                             ) AS d
            ON DUPLICATE KEY
                      UPDATE {', '.join([f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})" for c in upd_column_names])};
        """

        # Execute the commit query
        if 'commit' in actions:

            # If verbose is enabled, print the command being executed
            if verbose or 'print' in actions:
                print_sql(query_commit, title=f"Executing query as safe inserts{f' [{query_id}][commit]' if query_id else ''}")

            # Execute the commit query in the shell
            self.execute_query_in_shell(engine_name=engine_name, query=query_commit)

    # Public Method: Evaluate and patch a query's rows via ON DUPLICATE KEY UPDATE, optionally chunked.
    def execute_query_as_safe_inserts_in_chunks(self, engine_name, schema_name, table_name, query, key_column_names, upd_column_names, eval_column_names=None, actions=(), table_to_chunk=None, chunk_filter=None, chunk_size=None, row_id_name=None, show_progress=False, verbose=False, query_id=None):

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # Target table path
        t = target_table_path = f'{schema_name}.{table_name}'

        # Check if chunk_size and row_id_name are provided
        if 'commit' in actions and chunk_size is not None and row_id_name is not None:

            # Strip semicolon from inner query if needed
            base_query = query.strip().rstrip(';')

            # Internal Function: Build the chunked commit query template for one row_id window.
            def build_chunked_commit_query(chunk_condition):
                return f"""
                    INSERT INTO {target_table_path}
                               ({', '.join(key_column_names)}{', ' if upd_column_names else ''}{', '.join(upd_column_names)})
                          SELECT {', '.join(key_column_names)}{', ' if upd_column_names else ''}{', '.join(upd_column_names)}
                            FROM (
                                 {base_query} {chunk_condition}
                                 ) AS d
                ON DUPLICATE KEY UPDATE
                    {', '.join([
                        f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})"
                        for c in upd_column_names
                    ])}
                """

            # Determine chunking source and strategy
            row_id_field = row_id_name.split('.')[-1]  # handle aliases
            chunk_source = table_to_chunk or f"{schema_name}.{table_name}"
            use_dense_boundaries = table_to_chunk is not None or chunk_filter is not None
            filter_clause = f"WHERE {chunk_filter}" if chunk_filter else ""

            # Dense discovery for filtered/aliased sources; fixed-width ranges otherwise.
            if use_dense_boundaries:
                # Discover dense chunk boundaries over the actual filtered rows.
                # Each boundary returned is the last row_id of a chunk_size block.
                boundaries_query = f"""
                    WITH ranked AS (
                        SELECT {row_id_field},
                               ROW_NUMBER() OVER (ORDER BY {row_id_field}) AS rn
                          FROM {chunk_source}
                         {filter_clause}
                    )
                    SELECT {row_id_field}
                      FROM ranked
                     WHERE rn % {chunk_size} = 0
                     ORDER BY {row_id_field}
                """
                boundaries = [r[0] for r in self.execute_query(engine_name, boundaries_query)]

                # Absolute min/max to bracket the first and last chunk
                row_num_min = self.execute_query(engine_name, f"SELECT COALESCE(MIN({row_id_field}), 0) FROM {chunk_source} {filter_clause}")[0][0]
                row_num_max = self.execute_query(engine_name, f"SELECT COALESCE(MAX({row_id_field}), 0) FROM {chunk_source} {filter_clause}")[0][0]

                # Bail out cleanly when the chunk source has no rows.
                if row_num_min is None or row_num_max is None or (row_num_min == 0 and row_num_max == 0):
                    print("⚠️ No rows found to process.")
                    return

                # Close the final chunk up to the absolute maximum row_id.
                if not boundaries or boundaries[-1] != row_num_max:
                    boundaries.append(row_num_max)
                starts = [row_num_min] + [b + 1 for b in boundaries[:-1]]
                ends = boundaries
            else:
                # Legacy fixed-width row_id range chunking
                row_num_min = self.execute_query(engine_name, f"SELECT MIN({row_id_field}) FROM {chunk_source}")[0][0]
                row_num_max = self.execute_query(engine_name, f"SELECT MAX({row_id_field}) FROM {chunk_source}")[0][0]

                # Bail out cleanly when the chunk source has no rows.
                if row_num_min is None or row_num_max is None:
                    print("⚠️ No rows found to process.")
                    return

                # Widen the range by one so BETWEEN covers every row inclusive of boundaries.
                row_num_min -= 1
                row_num_max += 1
                starts = list(range(row_num_min, row_num_max, chunk_size))
                ends = [start + chunk_size - 1 for start in starts]

            # Execute each chunk with progress bar
            for start, end in (tqdm(zip(starts, ends), desc='Executing in chunks', unit='chunk', total=len(starts)) if show_progress else zip(starts, ends)):
                chunk_condition = f"{'WHERE' if 'WHERE' not in base_query.upper() else 'AND'} {row_id_name} BETWEEN {start} AND {end}"
                chunked_query = build_chunked_commit_query(chunk_condition)

                # Optionally print each chunked query before execution.
                if 'print' in actions:
                    print(chunked_query)

                # Execute the chunk in the shell so huge statements skip Python entirely.
                self.execute_query_in_shell(engine_name=engine_name, query=chunked_query)

            # Chunked path handled; skip the non-chunked commit below.
            return

        # Evaluate the patch operation
        if 'eval' in actions:

            # Generate evaluation query
            query_eval = f"""
                       SELECT {', '.join(eval_column_names)}, COUNT(*) AS n_to_process
                         FROM ({query}) t
                     GROUP BY {', '.join(eval_column_names)}
            """

            # If verbose is enabled, print the command being executed
            if verbose or 'print' in actions:
                print_sql(query_eval, title=f"Executing query as safe inserts in chunks{f' [{query_id}][eval]' if query_id else ''}")

            # Execute the evaluation query and print the results
            out = self.execute_query(engine_name=engine_name, query=query_eval)
            if len(out) > 0:
                df = pd.DataFrame(out, columns=eval_column_names+['# to process'])
                print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')

        # Build the commit query (non-chunked)
        query_commit = f"""
             INSERT INTO {target_table_path}
                         ({', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)})
                  SELECT  {', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)}
                    FROM (
                         {query}
                         ) AS d
        ON DUPLICATE KEY
                  UPDATE {', '.join([
                         f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})"
                         for c in upd_column_names
                         ])};
        """

        # If 'commit' is in actions, execute the commit query
        if 'commit' in actions:

            # If verbose is enabled, print the command being executed
            if verbose or 'print' in actions:
                print_sql(query_commit, title=f"Executing query as safe inserts in chunks{f' [{query_id}][commit]' if query_id else ''}")

            # Execute the commit query in the shell
            self.execute_query_in_shell(engine_name=engine_name, query=query_commit)

    # Public Method: Execute a query sequentially by row_id chunks in the shell.
    def execute_query_in_chunks(self, engine_name, schema_name, table_name, query, has_filters=None, table_to_chunk=None, chunk_filter=None, chunk_size=1000000, row_id_name='row_id', show_progress=False, verbose=False, query_id=None):

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # If verbose is enabled, print the command being executed
        if verbose:
            print_sql(query, title=f"Executing query in chunks{f' [{query_id}]' if query_id else ''}")

        # Remove trailing semicolon from the query
        if query.strip()[-1] == ';':
            query = query.strip()[:-1]

        # Which filter command to use?
        if has_filters is None:
            if 'WHERE' in query.upper():
                filter_command = 'AND'
            else:
                filter_command = 'WHERE'
        else:
            filter_command = 'AND' if has_filters else 'WHERE'

        # Row_id name contains alias?
        if '.' in row_id_name:
            row_id_name_no_alias = row_id_name.split('.')[1]
        else:
            row_id_name_no_alias = row_id_name

        # Determine chunking source and strategy
        chunk_source = table_to_chunk or f"{schema_name}.{table_name}"
        use_dense_boundaries = table_to_chunk is not None or chunk_filter is not None
        filter_clause = f"WHERE {chunk_filter}" if chunk_filter else ""

        # Dense discovery for filtered/aliased sources; fixed-width ranges otherwise.
        if use_dense_boundaries:
            # Discover dense chunk boundaries over the actual filtered rows.
            # Each boundary returned is the last row_id of a chunk_size block.
            boundaries_query = f"""
                WITH ranked AS (
                    SELECT {row_id_name_no_alias},
                           ROW_NUMBER() OVER (ORDER BY {row_id_name_no_alias}) AS rn
                      FROM {chunk_source}
                     {filter_clause}
                )
                SELECT {row_id_name_no_alias}
                  FROM ranked
                 WHERE rn % {chunk_size} = 0
                 ORDER BY {row_id_name_no_alias}
            """
            boundaries = [r[0] for r in self.execute_query(engine_name=engine_name, query=boundaries_query, query_id=query_id)]

            # Absolute min/max to bracket the first and last chunk
            row_num_min = self.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MIN({row_id_name_no_alias}), 0) FROM {chunk_source} {filter_clause}", query_id=query_id)[0][0]
            row_num_max = self.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MAX({row_id_name_no_alias}), 0) FROM {chunk_source} {filter_clause}", query_id=query_id)[0][0]

            # Bail out cleanly when the chunk source has no rows.
            if row_num_min is None or row_num_max is None or (row_num_min == 0 and row_num_max == 0):
                print("⚠️ No rows found to process.")
                return

            # Close the final chunk up to the absolute maximum row_id.
            if not boundaries or boundaries[-1] != row_num_max:
                boundaries.append(row_num_max)
            starts = [row_num_min] + [b + 1 for b in boundaries[:-1]]
            ends = boundaries
        else:
            # Legacy fixed-width row_id range chunking
            row_num_min = self.execute_query(engine_name=engine_name, query=f"SELECT MIN({row_id_name_no_alias}) FROM {chunk_source}", query_id=query_id)[0][0]
            row_num_max = self.execute_query(engine_name=engine_name, query=f"SELECT MAX({row_id_name_no_alias}) FROM {chunk_source}", query_id=query_id)[0][0]

            # Bail out cleanly when the chunk source has no rows.
            if row_num_min is None or row_num_max is None:
                print("⚠️ No rows found to process.")
                return

            # Widen the range by one so BETWEEN covers every row inclusive of boundaries.
            row_num_min -= 1
            row_num_max += 1
            starts = list(range(row_num_min, row_num_max, chunk_size))
            ends = [start + chunk_size - 1 for start in starts]

        # Process table in chunks
        for start, end in (tqdm(zip(starts, ends), total=len(starts)) if show_progress else zip(starts, ends)):

            # Generate SQL query
            sql_query = f"{query} {filter_command} {row_id_name} BETWEEN {start} AND {end};"

            # Execute the query
            self.execute_query_in_shell(engine_name=engine_name, query=sql_query, query_id=query_id)

    # Public Method: Execute a query through the environment's MySQL CLI in the shell.
    def execute_query_in_shell(self, engine_name, query, verbose=False, query_id=None):

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # If verbose is enabled, print the command being executed
        if verbose:
            print_sql(query, title=f"Executing query in shell{f' [{query_id}]' if query_id else ''}")

        # Delegate the shell transport to the environment's CLI gateway: it owns
        # the connection flags, SSL options, MYSQL_PWD env and stdin streaming.
        self._mysql_client.execute_query(query, query_id=query_id)

        # If verbose is enabled, confirm success
        if verbose:
            print("\033[92m✅ Query executed successfully.\033[0m\n")
        return True

    # Public Method: Execute an SQL file through the environment's MySQL CLI.
    def execute_query_from_file(self, engine_name, file_path, database=None, verbose=False):

        # Validate the environment name before touching the database.
        self._ensure_environment(engine_name)

        # Get absolute file path
        abs_file_path = os.path.abspath(file_path)

        # Check if the file exists
        if not os.path.isfile(abs_file_path):
            print(f"SQL file does not exist: {abs_file_path}")
            return False

        # Check if the file is empty before streaming
        if os.path.getsize(abs_file_path) == 0:
            print(f"⚠️  SQL file is empty: {abs_file_path}")
            return False

        # If verbose is enabled, print what is being streamed
        if verbose:
            print('\n')
            print(f"Streaming SQL from file: {abs_file_path}")
            print('\n')

        # Delegate the shell transport to the environment's CLI gateway: it owns
        # gzip-aware streaming, pipe draining and error reporting.
        self._mysql_client.execute_from_file(abs_file_path, database=database)

        # If verbose is enabled, confirm success
        if verbose:
            print("\033[92m✅ SQL file executed successfully.\033[0m\n")
        return True

    # Public Method: Thin-port alias executing a query without engine addressing.
    def execute(self, query: str, schema_name: Optional[str] = None, params: Optional[Dict[str, Any]] = None, commit: bool = False, return_exception: bool = False, query_id: Optional[str] = None) -> Union[List[Any], Tuple[str, str, Any]]:

        # Delegate to the engine-addressed path using this adapter's own environment.
        return self.execute_query(
            self.env_name,
            query,
            schema_name      = schema_name,
            params           = params,
            commit           = commit,
            return_exception = return_exception,
            query_id         = query_id,
        )

    # Public Method: Thin-port alias streaming SELECT results to a JSON-lines file.
    def execute_stream_to_file(self, query: str, output_file: str, schema_name: Optional[str] = None, params: Optional[Dict[str, Any]] = None, fetch_size: int = 1000, query_id: Optional[str] = None) -> None:

        # Delegate to the engine-addressed path using this adapter's own environment.
        self.execute_query_stream_to_file(
            self.env_name,
            query,
            schema_name = schema_name,
            params      = params,
            fetch_size  = fetch_size,
            output_file = output_file,
            query_id    = query_id,
        )

    # Public Method: Verify connectivity with a trivial SELECT through the engine.
    def test(self) -> bool:
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                return True
        except Exception:
            return False
