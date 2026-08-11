from __future__ import annotations

import json
import os
import subprocess
import tempfile
import types
from typing import Any, Dict, Optional

import pandas as pd
from loguru import logger as sysmsg
from sqlalchemy import text
from sqlalchemy.dialects.mysql import dialect as MySQLDialect
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from tqdm import tqdm

from graphdb.adapters.display.dis_print import print_colour
from graphdb.utils.mdl_sqlquery import print_dataframe, print_sql


class EngineExecuteAdapter:
    """Adapter for SQL query execution operations."""

    def __init__(self, graphdb: Any) -> None:
        self._graphdb = graphdb

    @staticmethod
    def _q(name: str) -> str:
        """Backtick-quote a single SQL identifier, escaping embedded backticks."""
        return f"`{name.replace('`', '``')}`"

    @staticmethod
    def _qt(schema_name: str, table_name: str) -> str:
        """Backtick-quote a schema-qualified table name."""
        return f"`{schema_name}`.`{table_name}`"

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

    def execute_query(self, engine_name, query, schema_name=None, params=None, commit=False, return_exception=False, verbose=False, query_id=None):

        # If verbose is enabled, print the command being executed
        if verbose:
            print_sql(query, title=f"Executing query{f' [{query_id}]' if query_id else ''}")

        connection_ctx = self._graphdb.engine[engine_name].begin() if commit else self._graphdb.engine[engine_name].connect()
        try:
            with connection_ctx as connection:
                if schema_name:
                    connection.execute(text(f"USE {self._q(schema_name)}"))
                result = connection.execute(text(query), parameters=params or {})
                if result.returns_rows:
                    rows = result.fetchall()
                else:
                    rows = []
        except (DataError, IntegrityError, SQLAlchemyError) as e:
            if return_exception:
                # You can return different levels of detail here
                error_type = type(e).__name__      # e.g. "DataError"
                error_message = str(e)             # human-readable
                # if you want the underlying DBAPI code, it's in e.orig (if available)
                dbapi_code = getattr(e.orig, "args", [None])[0] if hasattr(e, "orig") else None
                return error_type, error_message, dbapi_code
            else:
                print(f"\033[91mError executing query{f' [{query_id}]' if query_id else ''}.\033[0m")
                print(e)
                raise
        return rows

    #----------------------------------------------------#
    # Method: Executes query into a file using streaming #
    #----------------------------------------------------#
    def execute_query_stream_to_file(self, engine_name, query, schema_name=None, params=None, *, fetch_size=1000, output_file=None, verbose=False, query_id=None):

        if not output_file:
            raise ValueError("output_file must be provided")

        # If verbose is enabled, print the command being executed
        if verbose:
            print_sql(query, title=f"Executing query in streaming mode{f' [{query_id}]' if query_id else ''}")

        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)

        connection = self._graphdb.engine[engine_name].connect()
        try:
            if schema_name:
                connection.execute(text(f"USE {self._q(schema_name)}"))

            exec_conn = connection.execution_options(stream_results=True)
            result = exec_conn.execute(text(query), parameters=params)

            if not result.returns_rows:
                raise ValueError("execute_query_stream_to_file only supports SELECT queries")

            with open(output_file, "w", encoding="utf-8") as f:
                while True:
                    chunk = result.fetchmany(fetch_size)
                    if not chunk:
                        break
                    for row in chunk:
                        f.write(json.dumps(dict(row._mapping), ensure_ascii=False, default=str) + "\n")

        except MemoryError:
            try:
                connection.invalidate()
            except Exception:
                pass
            raise

        except (DataError, IntegrityError, SQLAlchemyError) as e:
            print(f"\033[91mError executing query{f' [{query_id}]' if query_id else ''}.\033[0m")
            print(e)
            raise

        finally:
            try:
                connection.close()
            except Exception:
                pass

    #----------------------------------------------------#
    # Method: Execute a single-row upsert safely         #
    #----------------------------------------------------#
    def execute_upsert_row(self, engine_name, schema_name, table_name, key_column_names, key_column_values, upd_column_names, upd_column_values, actions=()):
        """
        Possible actions: 'print', 'eval', 'commit'
        """

        # Generate the full table name
        t = f'{schema_name}.{table_name}'

        # Get the number of columns to update and create the dictionary with normalized values
        num_upd_columns = len(upd_column_names)
        num_key_columns = len(key_column_names)

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
                'no change'     : 'green',
                'new value'     : 'cyan',
                'set to null'   : 'red',
                'key exists'    : 'green',
                'key is new'    : 'cyan'
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
                dialect=MySQLDialect(),
                compile_kwargs={"literal_binds": True}
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

    #------------------------------------------------------------------#
    # Method: Executes/Evaluates a query using ON DUPLICATE KEY UPDATE #
    #------------------------------------------------------------------#
    def execute_query_as_safe_inserts(self, engine_name, schema_name, table_name, query, key_column_names, upd_column_names, eval_column_names=None, actions=(), verbose=False, query_id=None):

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

    #------------------------------------------------------------------------------#
    # Method: Executes/Evaluates a query using ON DUPLICATE KEY UPDATE (in chunks) #
    #------------------------------------------------------------------------------#
    def execute_query_as_safe_inserts_in_chunks(self, engine_name, schema_name, table_name, query, key_column_names, upd_column_names, eval_column_names=None, actions=(), table_to_chunk=None, chunk_filter=None, chunk_size=None, row_id_name=None, show_progress=False, verbose=False, query_id=None):

        # Target table path
        t = target_table_path = f'{schema_name}.{table_name}'

        # Check if chunk_size and row_id_name are provided
        if 'commit' in actions and chunk_size is not None and row_id_name is not None:

            # Strip semicolon from inner query if needed
            base_query = query.strip().rstrip(';')

            # Build base commit query (template, to be filled with chunk conditions)
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

                if row_num_min is None or row_num_max is None or (row_num_min == 0 and row_num_max == 0):
                    print("⚠️ No rows found to process.")
                    return

                if not boundaries or boundaries[-1] != row_num_max:
                    boundaries.append(row_num_max)
                starts = [row_num_min] + [b + 1 for b in boundaries[:-1]]
                ends = boundaries
            else:
                # Legacy fixed-width row_id range chunking
                row_num_min = self.execute_query(engine_name, f"SELECT MIN({row_id_field}) FROM {chunk_source}")[0][0]
                row_num_max = self.execute_query(engine_name, f"SELECT MAX({row_id_field}) FROM {chunk_source}")[0][0]

                if row_num_min is None or row_num_max is None:
                    print("⚠️ No rows found to process.")
                    return

                row_num_min -= 1
                row_num_max += 1
                starts = list(range(row_num_min, row_num_max, chunk_size))
                ends = [start + chunk_size - 1 for start in starts]

            # Execute each chunk with progress bar
            for start, end in (tqdm(zip(starts, ends), desc='Executing in chunks', unit='chunk', total=len(starts)) if show_progress else zip(starts, ends)):
                chunk_condition = f"{'WHERE' if 'WHERE' not in base_query.upper() else 'AND'} {row_id_name} BETWEEN {start} AND {end}"
                chunked_query = build_chunked_commit_query(chunk_condition)

                if 'print' in actions:
                    print(chunked_query)

                self.execute_query_in_shell(engine_name=engine_name, query=chunked_query)

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

    #-------------------------------------------------#
    # Method: Executes a query sequentially by chunks #
    #-------------------------------------------------#
    def execute_query_in_chunks(self, engine_name, schema_name, table_name, query, has_filters=None, table_to_chunk=None, chunk_filter=None, chunk_size=1000000, row_id_name='row_id', show_progress=False, verbose=False, query_id=None):

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

            if row_num_min is None or row_num_max is None or (row_num_min == 0 and row_num_max == 0):
                print("⚠️ No rows found to process.")
                return

            if not boundaries or boundaries[-1] != row_num_max:
                boundaries.append(row_num_max)
            starts = [row_num_min] + [b + 1 for b in boundaries[:-1]]
            ends = boundaries
        else:
            # Legacy fixed-width row_id range chunking
            row_num_min = self.execute_query(engine_name=engine_name, query=f"SELECT MIN({row_id_name_no_alias}) FROM {chunk_source}", query_id=query_id)[0][0]
            row_num_max = self.execute_query(engine_name=engine_name, query=f"SELECT MAX({row_id_name_no_alias}) FROM {chunk_source}", query_id=query_id)[0][0]

            if row_num_min is None or row_num_max is None:
                print("⚠️ No rows found to process.")
                return

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

    #---------------------------------------------#
    # Method: Executes a query in the MySQL shell #
    #---------------------------------------------#
    def execute_query_in_shell(self, engine_name, query, verbose=False, query_id=None):

        # Define the shell command for execution
        shell_command = self._graphdb.base_command_mysql[engine_name] + ["-e", query]

        # If verbose is enabled, print the command being executed
        if verbose:
            print_sql(query, title=f"Executing query in shell{f' [{query_id}]' if query_id else ''}")

        # Execute the command using subprocess and capture the output and errors
        result = subprocess.run(
            shell_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self._graphdb.subprocess_env.get(engine_name),
        )

        # Strip the stderr to remove any leading/trailing whitespace (including newlines)
        stderr = result.stderr.strip()

        # Ignore harmless mysql password warning
        password_warning = "mysql: [Warning] Using a password on the command line interface can be insecure."
        clean_stderr = "" if stderr == password_warning else stderr

        # Verify the return code to determine if the command executed successfully
        if result.returncode == 0:
            if verbose:
                print("\033[92m✅ Query executed successfully.\033[0m\n")
            return True

        # Generate a detailed error message including the command, return code, stderr, and stdout for diagnostics
        message = (
            f"Failed to execute query{f' [{query_id}]' if query_id else ''}.\n"
            f"Return code: {result.returncode}\n"
            f"STDERR:\n{clean_stderr or '<empty>'}\n"
            f"STDOUT:\n{result.stdout.strip() or '<empty>'}"
        )

        # Log the error message and raise an exception with the details
        sysmsg.critical(message)
        raise RuntimeError(message)

    #--------------------------------------------------------------#
    # Method: Executes a query in the MySQL shell from an SQL file #
    #--------------------------------------------------------------#
    def execute_query_from_file(self, engine_name, file_path, database=None, verbose=False):

        # Get absolute file path
        abs_file_path = os.path.abspath(file_path)

        # Check if the file exists
        if not os.path.isfile(abs_file_path):
            print(f"SQL file does not exist: {abs_file_path}")
            return False

        # Define the shell command for execution, ensuring we don't include -e/--execute to allow for stdin execution
        shell_command = list(self._graphdb.base_command_mysql[engine_name])

        # Warn if command already contains -e/--execute, as this would conflict with stdin execution
        if '-e' in shell_command or '--execute' in shell_command:
            print("Configured mysql command already contains -e/--execute, so stdin SQL will be ignored.")
            print(f"Command: {shell_command}")
            return False

        # If a database is specified, add it to the command (after connection parameters but before execution parameters)
        if database:
            shell_command.append(database)

        # Check if the file is empty before streaming
        if os.path.getsize(abs_file_path) == 0:
            print(f"⚠️  SQL file is empty: {abs_file_path}")
            return False

        # Print command if verbose
        if verbose:
            print('\n')
            print("Executing command:")
            print(shell_command)
            print('\n')
            print(f"Streaming SQL from file: {abs_file_path}")
            print('\n')

        # If verbose is enabled, add the flag to show warnings in the mysql command output
        if verbose:
            shell_command += ["--show-warnings"]

        # Stream the SQL file into mysql via a native pipe (gzip -dc ... | mysql ...).
        # This avoids Python in the data path and is much faster than Python streaming.
        # stdout/stderr are redirected to temp files so we can't deadlock on full pipes.
        try:
            with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
                if abs_file_path.endswith('.gz'):
                    reader_cmd = ['gzip', '-dc', abs_file_path]
                else:
                    reader_cmd = ['cat', abs_file_path]

                reader_proc = subprocess.Popen(
                    reader_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                mysql_proc = subprocess.Popen(
                    shell_command,
                    stdin=reader_proc.stdout,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    text=False,
                    env=self._graphdb.subprocess_env.get(engine_name),
                )
                reader_proc.stdout.close()

                mysql_returncode = mysql_proc.wait()
                reader_stdout, reader_stderr = reader_proc.communicate()

                stdout_file.seek(0)
                stdout_bytes = stdout_file.read()
                stderr_file.seek(0)
                stderr_bytes = stderr_file.read() + reader_stderr

                # SIGPIPE (-13) from the reader is expected when mysql closes stdin early.
                # Only report a reader failure if it's something other than SIGPIPE.
                if reader_proc.returncode not in (0, -13):
                    raise RuntimeError(
                        f"Failed to read SQL file: {abs_file_path} "
                        f"(reader return code: {reader_proc.returncode})"
                    )

                returncode = mysql_returncode

        # Handle file reading errors and subprocess execution errors separately for clearer diagnostics
        except OSError as exc:
            print(f"Failed to open SQL file: {abs_file_path}")
            print(str(exc))
            raise RuntimeError(f"Failed to open SQL file: {abs_file_path}") from exc

        except Exception as exc:
            print(f"Failed to execute mysql command for file: {abs_file_path}")
            print(str(exc))
            raise RuntimeError(f"Failed to execute mysql command for file: {abs_file_path}") from exc

        # Process the result and handle stderr, ignoring the common password warning
        warn = "mysql: [Warning] Using a password on the command line interface can be insecure."
        stderr_text = stderr_bytes.decode('utf-8', errors='replace')
        stdout_text = stdout_bytes.decode('utf-8', errors='replace')
        stderr_lines = [
            line for line in stderr_text.splitlines()
            if line.strip() and line.strip() != warn
        ]

        # If verbose, print the command, return code, stderr, and stdout. If not verbose but there are stderr lines (other than the ignored warning), print them with the file path for context.
        if verbose:

            # Print the executed command and return code
            print(f"\nmysql command: {' '.join(shell_command)}")
            print(f"return code: {returncode}")

            # Print stderr if there are any lines to show
            if stderr_lines:
                print("\nstderr:")
                print("\n".join(stderr_lines))

            # Print stdout if there is any output
            if stdout_text.strip():
                print("\nstdout:")
                print(stdout_text)

        else:
            # If not verbose but there are stderr lines (other than the ignored warning), print them with the file path for context
            if stderr_lines:
                print(f"stderr for file: {abs_file_path}")
                print("\n".join(stderr_lines))

        # Check the return code to determine success or failure of the command execution
        if returncode != 0:
            raise RuntimeError(f"mysql command failed for file: {abs_file_path}")

        # If we reach this point, the command executed successfully (return code 0), so we return a result object
        return types.SimpleNamespace(returncode=returncode, stdout=stdout_text, stderr=stderr_text)

