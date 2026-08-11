#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from sqlalchemy import create_engine as SQLEngine, text, event
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from sqlalchemy.dialects.mysql import dialect as MySQLDialect
from typing import Any, Dict, Optional
from loguru import logger as sysmsg
from tqdm import tqdm
from pathlib import Path
import numpy as np
import pandas as pd
from tabulate import tabulate
import sys, os, re, subprocess, json, datetime, hashlib, random, glob, time, rich, ssl, shlex, shutil, gzip, tempfile, types
from graphdb.application.core.cfg_config import GraphDBConfig, GraphDBConfigError
from graphdb.common.mdl_sqlquery import print_sql

# New architecture imports (incremental migration)
from graphdb.application.factories.fct_adapter_registry import AdapterRegistry
from graphdb.domain.mdl_connection import ConnectionParams
from graphdb.common.cmn_ssl_options import (
    normalize_ssl_options,
    parse_bool,
    build_ssl_connect_args,
    detect_cli_option_names,
    build_ssl_cli_flags,
)
from graphdb.adapters.gateways.gtw_sqlalchemy import create_sqlalchemy_engine
from graphdb.adapters.engine.eng_execute import EngineExecuteAdapter
from graphdb.adapters.engine.eng_initiate import EngineInitiateAdapter
from graphdb.adapters.engine.eng_test import EngineTestAdapter
from graphdb.adapters.data.dta_compare import DataCompareAdapter
from graphdb.adapters.data.dta_copy import DataCopyAdapter
from graphdb.adapters.data.dta_export import DataExportAdapter
from graphdb.adapters.data.dta_import import DataImportAdapter
from graphdb.adapters.display.dis_print import DisplayAdapter


# Find the repository root directory
REPO_ROOT = Path(__file__).resolve().parents[2]

#------------------------------------------------#
# Progress bar and system messages configuration #
#------------------------------------------------#

# Width of the progress bar
PBWIDTH = 64

# Set up system message handler to display TRACE messages
sysmsg.remove()
sysmsg.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line:06d}</cyan> - "
           "<level>{message}</level>",
    level="TRACE"
)

#----------------------------------------#

# Function to get the table type from the table name
def get_table_type_from_name(table_name):

    match_gen_from_to_edges    = re.findall(r"Edges_N_[^_]*_[^_]*_N_[^_]*_[^_]*_T_(GBC|AS)$", table_name)
    match_obj_to_obj_edges     = re.findall(r"Edges_N_[^_]*_N_(?!Concept)[^_]*_T_[^_]*$", table_name)
    match_obj_to_concept_edges = re.findall(r"Edges_N_[^_]*_N_Concept_T_[^_]*$", table_name)
    match_data_object          = re.findall(r"Data_N_Object_T_[^_]*(_COPY)?$", table_name)
    match_data_obj_to_obj      = re.findall(r"Data_N_Object_N_Object_T_[^_]*$", table_name)
    match_doc_index            = re.findall(r"Index_D_[^_]*(_COPY)?$", table_name)
    match_link_index           = re.findall(r"Index_D_[^_]*_L_[^_]*_T_[^_]*(_Search)?(_COPY)?$", table_name)
    match_stats_object         = re.findall(r"Stats_N_Object_T_[^_]*$", table_name)
    match_stats_obj_to_obj     = re.findall(r"Stats_N_Object_N_Object_T_[^_]*$", table_name)
    match_buildup_docs         = re.findall(r'^IndexBuildup_Fields_Docs_[^_]*', table_name)
    match_buildup_links        = re.findall(r'^IndexBuildup_Fields_Links_ParentChild_[^_]*_[^_]*', table_name)
    match_scores_matrix        = re.findall(r"Edges_N_Object_N_Object_T_ScoresMatrix_AS$", table_name)

    if match_gen_from_to_edges:
        return 'from_to_edges'
    elif match_obj_to_obj_edges:
        return 'object_to_object'
    elif match_obj_to_concept_edges:
        return 'object_to_concept'
    elif match_data_object:
        if 'PageProfile' in table_name:
            return 'doc_profile'
        else:
            return 'object'
    elif match_data_obj_to_obj:
        return 'object_to_object'
    elif match_doc_index:
        return 'doc_index'
    elif match_link_index:
        return 'link_index'
    elif match_stats_object:
        return 'object'
    elif match_stats_obj_to_obj:
        return 'object_to_object'
    elif match_buildup_docs:
        return 'doc_index'
    elif match_buildup_links:
        return 'link_index'
    elif match_scores_matrix:
        return 'object_to_object'
    else:
        return None

# Print in colour
def print_colour(msg, colour='white', background='black', style='normal', display_method=False):
    colour_codes = {
        'black'  : 30,
        'red'    : 31,
        'green'  : 32,
        'yellow' : 33,
        'blue'   : 34,
        'purple' : 35,
        'magenta': 35,
        'cyan'   : 36,
        'white'  : 37
    }
    background_codes = {
        'black'  : 40,
        'red'    : 41,
        'green'  : 42,
        'yellow' : 43,
        'blue'   : 44,
        'purple' : 45,
        'magenta': 45,
        'cyan'   : 46,
        'white'  : 47
    }
    style_codes = {
        'normal'  : 0,
        'bold'    : 1,
        'underline': 4,
        'blink'   : 5,
        'reverse' : 7,
        'hidden'  : 8
    }

    if display_method:
        import inspect
        frame = inspect.currentframe().f_back
        method = frame.f_code.co_name

        # Attempt to get class name from 'self' or 'cls'
        class_name = None
        if 'self' in frame.f_locals:
            class_name = type(frame.f_locals['self']).__name__
        elif 'cls' in frame.f_locals:
            class_name = frame.f_locals['cls'].__name__

        if class_name:
            msg = f"{class_name}.{method}(): {msg}"
        else:
            msg = f"{method}(): {msg}"

    print(f"\033[{style_codes[style]};{colour_codes[colour]};{background_codes[background]}m{msg}\033[0m")

# Pretty-print dataframe
def print_dataframe(df, title):
    print('')
    print_colour(title, colour='white', background='black', style='bold')
    print(tabulate(df, headers=df.columns, tablefmt='fancy_grid', showindex=False))
    print('')

#-----------------------------------------#
# Class definition for Graph MySQL engine #
#-----------------------------------------#
class GraphDB():

    # Class variable to hold the single instance
    _instance = None
    _cli_option_cache: Dict[str, set] = {}

    # Create new instance of class before __init__ is called
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)  # Use `object.__new__()` explicitly
            cls._instance._initialized = False  # Flag for initialization check
        return cls._instance

    # Class constructor
    def __init__(self, name="GraphDB", config: Optional[GraphDBConfig] = None):

        # Check if the instance is already initialized
        if not self._initialized:  # Prevent reinitialization
            self.name = name
            self._initialized = True

        self.config = config or GraphDBConfig.from_default_file()
        self.default_engine_name = self.config.default_env

        # Build new architecture adapters; mirror their state for backward compatibility.
        self._adapter_registry = AdapterRegistry(self.config)

        self.params = {}
        self.engine = {}
        self.base_command_mysql = {}
        self.base_command_mysqldump = {}
        self.subprocess_env = {}

        for env_name, env_config in self.config.environments.items():
            adapter = self._adapter_registry.get(env_name)
            self.params[env_name] = env_config.as_dict()
            self.engine[env_name] = adapter.engine
            self.base_command_mysql[env_name] = adapter.base_command_mysql
            self.base_command_mysqldump[env_name] = adapter.base_command_mysqldump
            self.subprocess_env[env_name] = adapter.subprocess_env

        # Adapter composition for migrated functionality
        self._execute_adapter = EngineExecuteAdapter(self)
        self._initiate_adapter = EngineInitiateAdapter(self)
        self._test_adapter = EngineTestAdapter(self)
        self._compare_adapter = DataCompareAdapter(self)
        self._copy_adapter = DataCopyAdapter(self)
        self._export_adapter = DataExportAdapter(self)
        self._import_adapter = DataImportAdapter(self)
        self._display_adapter = DisplayAdapter(self)

    #-------------------------------#
    # SSL helpers                   #
    #-------------------------------#
    @staticmethod
    def _normalize_ssl_options(raw_ssl):
        return normalize_ssl_options(raw_ssl)

    @staticmethod
    def _parse_bool(value):
        return parse_bool(value)

    @classmethod
    def _build_ssl_connect_args(cls, ssl_options):
        return build_ssl_connect_args(ssl_options)

    @classmethod
    def _detect_cli_option_names(cls, base_command):
        return detect_cli_option_names(base_command)

    @classmethod
    def _build_ssl_cli_flags(cls, ssl_options, supported_options=None, engine_flavor=None):
        return build_ssl_cli_flags(ssl_options, supported_options=supported_options, engine_flavor=engine_flavor)


    #----------------------------------#
    # Method: Check if database exists #
    #----------------------------------#
    def drop_database(self, engine_name, schema_name):
        connection = self.engine[engine_name].connect()
        try:
            connection.execute(text(f'DROP DATABASE IF EXISTS {schema_name}'))
        finally:
            connection.close()

    #--------------------------------------------#
    # Method: Execute a single-row upsert safely #
    #--------------------------------------------#

    # Helper: Normalize Python values for SQL
    @staticmethod
    def _normalize_sql_value(value):
        if value is None:
            return None

        if isinstance(value, str):
            stripped = value.strip()
            if stripped in {"NULL", "null", "NaN", "nan"}: # fix: 3ghj54
                return None
            return value

        try:
            if pd.isna(value):
                return None
        except Exception:
            pass

        return value

    # Method: Execute a single-row upsert safely
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

    #----------------------------------#
    # Method: Set cell values in table #
    #----------------------------------#
    def set_cells(self, engine_name, schema_name, table_name, set=(), where=(), verbose=False):

        # Check if there are any columns to set
        if len(set) == 0:
            sysmsg.error("No columns to set. Please provide at least one column and value pair.")
            return

        # Generate the SET clause
        set_clause = ', '.join([f"{col} = '{val}'" for col, val in set])

        # Generate the WHERE clause
        if len(where) > 0:
            where_clause = ' AND '.join([f"{col} = '{val}'" for col, val in where])
        else:
            where_clause = "TRUE"

        # Generate the SQL query
        sql_query = f"""
            UPDATE {schema_name}.{table_name}
               SET {set_clause}
             WHERE {where_clause}
        """

        # Execute the query in the MySQL shell
        self.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose=verbose)

    #------------------------------------#
    # Method: Get cell values from table #
    #------------------------------------#
    def get_cells(self, engine_name, schema_name, table_name, select=(), where=(), verbose=False):

        # Generate the WHERE clause
        if len(where) > 0:
            where_clause = ' AND '.join([f"{col} = '{val}'" if col is not None else f"({val})" for col, val in where])
        else:
            where_clause = "TRUE"

        # Generate the SQL query
        sql_query = f"""
            SELECT {', '.join(select) if len(select) > 0 else '*'}
              FROM {schema_name}.{table_name}
             WHERE {where_clause}
        """

        # Execute the query in the MySQL shell
        result = self.execute_query(engine_name=engine_name, query=sql_query) # TODO: add verbose
        if len(result) == 0:
            return []
        
        # Return the result as a list of tuples
        return result


    #-------------------------------------------------#
    # Method: Apply data types to a table (from JSON) #
    #-------------------------------------------------#
    def apply_datatypes(self, engine_name, schema_name, table_name, datatypes_json, display_elapsed_time=False, estimated_num_rows=False):

        # Display processing time estimate
        if estimated_num_rows:
                
                # Display the current time
                print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")
    
                # Calculate the estimated processing time
                processing_time = processing_times_per_row['apply_datatypes'] * estimated_num_rows
    
                # Display the estimated processing time in # hours, # min and # sec format
                print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Get the column names
        column_names = self.get_column_names(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Build the sql query for applying data types
        sql_query = f"ALTER TABLE {schema_name}.{table_name} "

        # Loop over the column names
        for column_name in column_names:
            if column_name in datatypes_json:
                sql_query += f"MODIFY COLUMN {column_name} {datatypes_json[column_name]}, "

        # Remove the trailing comma and space
        if sql_query.endswith(', '):
            sql_query = sql_query[:-2]

        # Execute the query
        self.execute_query_in_shell(engine_name=engine_name, query=sql_query)

        # Print the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")

    #-------------------------------------------#
    # Method: Apply keys to a table (from JSON) #
    #-------------------------------------------#
    def apply_keys(self, engine_name, schema_name, table_name, keys_json, display_elapsed_time=False, estimated_num_rows=False):

        # Display processing time estimate
        if estimated_num_rows:
                
                # Display the current time
                print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")
    
                # Calculate the estimated processing time
                processing_time = processing_times_per_row['apply_keys'] * estimated_num_rows
    
                # Display the estimated processing time in # hours, # min and # sec format
                print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Get the column names
        column_names = self.get_column_names(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Build composite primary key
        composite_primary_key = ''
        for column_name in keys_json:
            if keys_json[column_name] == 'PRIMARY KEY' and column_name in column_names:
                composite_primary_key += column_name + ', '
        
        # Remove the trailing comma and space
        if composite_primary_key.endswith(', '):
            composite_primary_key = composite_primary_key[:-2]

        # Build the sql query for applying keys
        sql_query = f"ALTER TABLE {schema_name}.{table_name} "

        # Append the composite primary key
        if composite_primary_key:
            sql_query += f"ADD PRIMARY KEY ({composite_primary_key}), "
            sql_query += f"ADD UNIQUE KEY uid ({composite_primary_key}), "

        # Check if primary key already defined
        if self.has_primary_key(engine_name=engine_name, schema_name=schema_name, table_name=table_name):
            print(f"Table {schema_name}.{table_name} already has a primary key defined.")
            return

        # Loop over the remaining keys
        for column_name in keys_json:
            if column_name in column_names:
                sql_query += f"ADD {keys_json[column_name].replace('PRIMARY KEY', 'KEY')} {column_name} ({column_name}), "

        # Remove the trailing comma and space
        if sql_query.endswith(', '):
            sql_query = sql_query[:-2]

        # Execute the query
        self.execute_query_in_shell(engine_name=engine_name, query=sql_query)

        # Display the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")

    #----------------------------------------------#
    # Method: Materialise a view to the cache      #
    #----------------------------------------------#
    def materialise_view(self, source_schema, source_view, target_schema, target_table, drop_table=False, use_replace=False, auto_increment_column=False, datatypes_json=False, keys_json=False, display_elapsed_time=False, estimated_num_rows=False, verbose=False, engine_name=None):
        engine_name = engine_name or self.default_engine_name

        # Display processing time estimate
        if estimated_num_rows:

            # Display the current time
            print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")

            # Calculate the estimated processing time
            processing_time = processing_times_per_row['materialise_view'] * estimated_num_rows

            # Display the estimated processing time in # hours, # min and # sec format
            print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Drop the target table if it exists
        if drop_table:
            self.execute_query(engine_name=engine_name, query=f"DROP TABLE IF EXISTS {target_schema}.{target_table}")

        # If use_replace, set the REPLACE statement
        insert_or_replace_statement = 'REPLACE' if use_replace else 'INSERT'

        # Create the target table
        self.execute_query(engine_name=engine_name, query=f"CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} AS SELECT * FROM {source_schema}.{source_view} WHERE 1=0")

        # Set auto increment column
        if auto_increment_column:
            self.execute_query(engine_name=engine_name, query=f"ALTER TABLE {target_schema}.{target_table} MODIFY COLUMN row_id INT AUTO_INCREMENT UNIQUE KEY")

        # Populate the target table
        self.execute_query_in_shell(engine_name=engine_name, query=f"{insert_or_replace_statement} INTO {target_schema}.{target_table} SELECT * FROM {source_schema}.{source_view}")

        # Print the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")

        # Apply datatypes
        if datatypes_json:
            if verbose:
                sysmsg.info(f"Applying datatypes to {target_schema}.{target_table} ...")
            self.apply_datatypes(engine_name=engine_name, schema_name=target_schema, table_name=target_table, datatypes_json=datatypes_json, display_elapsed_time=display_elapsed_time, estimated_num_rows=estimated_num_rows)

        # Create keys JSON
        if keys_json:
            if verbose:
                sysmsg.info(f"Applying keys to {target_schema}.{target_table} ...")
            self.apply_keys(engine_name=engine_name, schema_name=target_schema, table_name=target_table, keys_json=keys_json, display_elapsed_time=display_elapsed_time, estimated_num_rows=estimated_num_rows)

    #----------------------------------------------#
    # Method: Materialise a view to the cache      #
    #----------------------------------------------#
    def update_table_from_view(self, engine_name, source_schema, source_view, target_schema, target_table, verbose=False):

        # Fetch list of columns in the source view
        source_columns = self.get_column_names(engine_name=engine_name, schema_name=source_schema, table_name=source_view)
        
        # Generate the SQL query
        SQLQuery = f"REPLACE INTO {target_schema}.{target_table} ({', '.join(source_columns)}) SELECT * FROM {source_schema}.{source_view};"

        # Print status and the SQL query if verbose
        if verbose:
            sysmsg.info(f"Updating table '{target_table}' from view '{source_view}' ...")

        # Execute the query
        self.execute_query_in_shell(engine_name=engine_name, query=f"REPLACE INTO {target_schema}.{target_table} ({', '.join(source_columns)}) SELECT * FROM {source_schema}.{source_view}")

        # Print status
        if verbose:
            sysmsg.success(f"Table '{target_table}' updated from view '{source_view}'.")

    #----------------------------------------------------------------------------#
    # Method: Delete rows from table for which keys don't exist in another table #
    #----------------------------------------------------------------------------#
    def delete_orphaned_rows(self, engine_name, upd_schema, upd_table, upd_key, ref_schema, ref_table, ref_key, upd_where='TRUE', ref_where=None, actions=()):

        # Check if update table exists (return if not)
        if not self.table_exists(engine_name=engine_name, schema_name=upd_schema, table_name=upd_table):
            # sysmsg.warning(f"Table {upd_schema}.{upd_table} does not exist.")
            return

        # Build equality predicate for composite keys: u.k1 = r.k1 AND ...
        preds = " AND ".join([f"u.{uk} = r.{rk}" for uk, rk in zip(upd_key, ref_key)])

        # Build additional reference filter
        ref_filter = f" AND ({ref_where})" if ref_where else ""

        # Evaluation action
        if 'eval' in actions:

            # Generate the SQL evaluation query
            query_eval = f"""
                SELECT COUNT(*) AS n_to_delete
                  FROM {upd_schema}.{upd_table} u
                 WHERE NOT EXISTS (SELECT 1
                                    FROM {ref_schema}.{ref_table} r
                                   WHERE {preds}{ref_filter})
                   AND ({upd_where});
            """

            # Print the evaluation query
            if 'print' in actions:
                print(query_eval)

            # Execute the evaluation query and print the results
            out = self.execute_query(engine_name=engine_name, query=query_eval)
            if len(out) > 0:
                df = pd.DataFrame(out, columns=['rows to delete'])
                if df['rows to delete'][0] == 0:
                    sysmsg.warning(f"⚠️  No orphaned rows found in table '{upd_table}' for key {upd_key}.")
                    return
                print_dataframe(df, title=f"\n🔍 Evaluation results for '{upd_table}' and key {upd_key}:")

        # Generate the SQL commit query
        query_commit = f"""
               USE {upd_schema};
            DELETE u
              FROM {upd_schema}.{upd_table} u
             WHERE NOT EXISTS (SELECT 1
                                 FROM {ref_schema}.{ref_table} r
                                WHERE {preds}{ref_filter})
               AND ({upd_where});
        """

        # Print the commit query
        if 'print' in actions:
            print(query_commit)

        # Execute the commit query
        if 'commit' in actions:
            self.execute_query_in_shell(engine_name=engine_name, query=query_commit)
            sysmsg.success(f"✅ Orphaned rows deleted from table '{upd_table}' for key {upd_key}.")

#================#
# Main execution #
#================#
if __name__ == "__main__":
    db = GraphDB()
    if db.test() is True:
        sysmsg.success("✅ MySQL client test passed.")
    else:
        sysmsg.error("❌ MySQL client test failed.")

