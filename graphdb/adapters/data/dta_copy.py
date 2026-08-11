from __future__ import annotations

import os
import subprocess
from typing import Any

from loguru import logger as sysmsg
from sqlalchemy import text
from tqdm import tqdm


class DataCopyAdapter:
    """Adapter for data operations."""

    def __init__(self, graphdb: Any) -> None:
        self._graphdb = graphdb

    #--------------------------------------------------#
    # Method: Drop all keys in a table (except row_id) #
    #--------------------------------------------------#
    def copy_create_table(self, source_engine_name, source_schema_name, source_table_name, target_engine_name, target_schema_name, target_table_name, ignore_if_exists=False, drop_table=False, drop_keys=False):

        # Check if the target table exists
        if ignore_if_exists:
            if self._graphdb.table_exists(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name):
                sysmsg.warning(f"Table {target_schema_name}.{target_table_name} already exists. Flag 'ignore_if_exists' set to {ignore_if_exists}.")
                sysmsg.warning("Table not copied.")
                return

        # Get the create table SQL
        create_table_sql = self._graphdb.get_create_table(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name)

        # Drop the target table if it exists
        if drop_table:
            self._graphdb.drop_table(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name)

        # Use the target database
        self._graphdb.execute_query(engine_name=target_engine_name, query=f'USE {target_schema_name}')

        # Fix missing namespace in the create table SQL
        create_table_sql = create_table_sql.replace("CREATE TABLE ", f"CREATE TABLE {target_schema_name}.")

        # Execute the create table SQL
        self._graphdb.execute_query(engine_name=target_engine_name, query=create_table_sql)

        # Drop all keys in the target table
        if drop_keys:
            self._graphdb.drop_keys(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name)

    #----------------------------------------------#
    # Method: Copies a table from source to target #
    #----------------------------------------------#
    def copy_table_LEGACY(self, engine_name, source_schema_name, source_table_name, target_schema_name, target_table_name, list_of_columns=False, where_condition='TRUE', row_id_name=None, chunk_size=1000000, create_table=False, drop_keys=False, use_replace_or_ignore=False):

        # Create the target table if it does not exist
        if create_table:
            self._graphdb.create_table_like(engine_name=engine_name, source_schema_name=source_schema_name, source_table_name=source_table_name, target_schema_name=target_schema_name, target_table_name=target_table_name, drop_table=False, drop_keys=True)

        # Drop all keys in the target table
        if drop_keys:
            self._graphdb.drop_keys(engine_name=engine_name, schema_name=target_schema_name, table_name=target_table_name)

        # Define the insert or replace statement
        if use_replace_or_ignore == 'REPLACE':
            insert_replace_or_ignore = 'REPLACE'
            print('Using REPLACE ...')
        elif use_replace_or_ignore == 'IGNORE':
            insert_replace_or_ignore = 'INSERT IGNORE'
            print('Using INSERT IGNORE ...')
        else:
            insert_replace_or_ignore = 'INSERT'
            print('Using INSERT (default) ...')

        # Get min and max row_id
        row_num_min = self._graphdb.execute_query(engine_name=engine_name, query=f"SELECT MIN({row_id_name}) FROM {source_schema_name}.{source_table_name}")[0][0]
        row_num_max = self._graphdb.execute_query(engine_name=engine_name, query=f"SELECT MAX({row_id_name}) FROM {source_schema_name}.{source_table_name}")[0][0]
        n_rows = row_num_max - row_num_min + 1

        # Process table in chunks
        for offset in tqdm(range(row_num_min, row_num_max, chunk_size), desc=f'Copying table', unit='rows', total=round(n_rows/chunk_size)):

            # Generate SQL query
            sql_query = f"""
                {insert_replace_or_ignore} INTO {target_schema_name}.{target_table_name}
                                                {' ' if list_of_columns is False else '(%s)' % ', '.join(list_of_columns)}
                                         SELECT {'*' if list_of_columns is False else  '%s'  % ', '.join(list_of_columns)}
                                           FROM {source_schema_name}.{source_table_name}
                                          WHERE {where_condition}
            """

            # Add row_id condition if specified
            if row_id_name is not None:
                sql_query += f"""AND {row_id_name} BETWEEN {offset} AND {offset + chunk_size - 1}"""

            # Execute the query
            self._graphdb.execute_query_in_shell(engine_name=engine_name, query=sql_query)

            # Break if not processed in chunks
            if row_id_name is None:
                break

    #---------------------------------------------#
    # Method: Copies a view from source to target #
    #---------------------------------------------#
    def copy_view_definition(self, engine_name, source_schema_name, source_view_name, target_schema_name, target_view_name, drop_view=False):

        # Drop the target view if it exists
        if drop_view:
            self._graphdb.drop_view(engine_name=engine_name, schema_name=target_schema_name, view_name=target_view_name)

        # Get the view definition
        # view_definition = self._graphdb.get_create_table(engine_name=engine_name, schema_name=source_schema_name, table_name=source_view_name)
        view_definition = self._graphdb.get_create_view(engine_name=engine_name, schema_name=source_schema_name, view_name=source_view_name)

        # Fix the view definition
        view_definition = view_definition.replace(f'`{source_schema_name}`', f'`{target_schema_name}`')
        view_definition = re.sub(r"CREATE ALGORITHM=UNDEFINED DEFINER=`[^`]*`@`[^`]*` SQL SECURITY DEFINER VIEW `[^`]*`.`[^`]*` AS ", "", view_definition)

        # Create the view in the target schema
        self._graphdb.create_view(engine_name=engine_name, schema_name=target_schema_name, view_name=target_view_name, query=view_definition)



    #========================================================#
    #                                                        #
    #   METHOD GROUP: Migrate tables across engines/servers  #
    #                                                        #
    #========================================================#

    #-----------------------------------#
    # Method: Copy table across engines #
    #-----------------------------------#
    def copy_table(self, source_engine_name, source_schema_name, target_engine_name, target_schema_name, table_name, filter_by='TRUE', chunk_size=1000000, create_keys_after_import=False, compress=False):

        # Print status message
        sysmsg.info(f"📝 Copy table across MySQL servers.")

        # Print parameters
        sysmsg.trace(    f"Engines .......... {source_engine_name} --> {target_engine_name}")
        sysmsg.trace(    f"Table ............ {table_name}")
        if filter_by!='TRUE':
            sysmsg.trace(f"Filter by ........ {filter_by}")
        if chunk_size!=1000000:
            sysmsg.trace(f"Chunk size ....... {chunk_size}")
        sysmsg.trace(f"'compress' set to {'TRUE' if compress else 'FALSE'}.")
        sysmsg.trace(f"""'create_keys_after_import' set to {'TRUE' if create_keys_after_import else 'FALSE'}.""")

        # Get current date in YYYY-MM-DD format
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # Generate random MD5 hash
        md5_hash = hashlib.md5(str(source_engine_name+source_schema_name+target_engine_name+target_schema_name+table_name+filter_by+str(chunk_size)+str(create_keys_after_import)+str(compress)).encode()).hexdigest()[:8]

        # Generate the full folder path for temporary export
        temp_output_path = os.path.join(self.config.export_root(), current_date, md5_hash)

        # Print parameters
        sysmsg.trace(f"Temporary folder: {temp_output_path}")

        # Print status message
        sysmsg.info(f"⚙️  Exporting table from '{source_engine_name}' engine into temporary folder ...")

        # Export the table from source engine to temporary folder
        self._graphdb.export_table(engine_name=source_engine_name, schema_name=source_schema_name, table_name=table_name, output_folder=temp_output_path, filter_by=filter_by, chunk_size=chunk_size, include_create_tables=True, compress=compress)

        # Print status message
        sysmsg.info("☑️  Data export completed.")

        # Print status message
        sysmsg.info(f"⚙️  Importing table from temporary folder into '{target_engine_name}' engine ...")

        # Import the table from temporary folder to target engine
        self._graphdb.import_table(engine_name=target_engine_name, schema_name=target_schema_name, input_folder=os.path.join(temp_output_path, source_schema_name, table_name), create_keys_after_import=create_keys_after_import)

        # Print status message
        sysmsg.info("☑️  Data import completed.")

        # Print status message
        sysmsg.success(f"✅ Done copying table.")

    #--------------------------------------#
    # Method: Copy database across engines #
    #--------------------------------------#
    def copy_database(self, source_engine_name, source_schema_name, target_engine_name, target_schema_name, filter_by='TRUE', chunk_size=1000000, create_keys_after_import=False, compress=False):

        # Print status message
        sysmsg.info(f"📝 Copy database across MySQL servers.")

        # Print parameters
        sysmsg.trace(    f"Engines .......... {source_engine_name} --> {target_engine_name}")
        sysmsg.trace(    f"Database ......... {source_schema_name} --> {target_schema_name}")
        if filter_by!='TRUE':
            sysmsg.trace(f"Filter by ........ {filter_by}")
        if chunk_size!=1000000:
            sysmsg.trace(f"Chunk size ....... {chunk_size}")
        sysmsg.trace(f"'compress' set to {'TRUE' if compress else 'FALSE'}.")
        sysmsg.trace(f"""'create_keys_after_import' set to {'TRUE' if create_keys_after_import else 'FALSE'}.""")

        # Get current date in YYYY-MM-DD format
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # Generate random MD5 hash
        md5_hash = hashlib.md5(str(source_engine_name+source_schema_name+target_engine_name+target_schema_name+filter_by+str(chunk_size)+str(create_keys_after_import)+str(compress)).encode()).hexdigest()[:8]

        # Generate the full folder path for temporary export
        temp_output_path = os.path.join(self.config.export_root(), current_date, md5_hash)

        # Print parameters
        sysmsg.trace(f"Temporary folder: {temp_output_path}")

        # Print status message
        sysmsg.info(f"⚙️  Exporting database tables from '{source_engine_name}' engine into temporary folder ...")

        # Export the database from source engine to temporary folder
        self._graphdb.export_database(engine_name=source_engine_name, schema_name=source_schema_name, output_folder=temp_output_path, filter_by=filter_by, chunk_size=chunk_size, include_create_tables=True, compress=compress)

        # Print status message
        sysmsg.info("☑️  Data export completed.")

        # Print status message
        sysmsg.info(f"⚙️  Importing database tables from temporary folder into '{target_engine_name}' engine ...")

        # Import the database from temporary folder to target engine
        self._graphdb.import_database(engine_name=target_engine_name, schema_name=target_schema_name, input_folder=os.path.join(temp_output_path, source_schema_name), create_keys_after_import=create_keys_after_import)

        # Print status message
        sysmsg.info("☑️  Data import completed.")

        # Print status message
        sysmsg.success(f"✅ Done copying database.")

