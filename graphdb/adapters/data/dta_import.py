from __future__ import annotations

import glob
import os
import re
import subprocess
from typing import Any

from loguru import logger as sysmsg
from sqlalchemy import text


class DataImportAdapter:
    """Adapter for data operations."""

    def __init__(self, graphdb: Any) -> None:
        self._graphdb = graphdb

    #===============================================#
    #                                               #
    #   METHOD GROUP: Import tables and databases   #
    #                                               #
    #===============================================#

    #----------------------------------------------#
    # Method: Import table definitions from folder #
    #----------------------------------------------#
    def import_create_table(self, engine_name, schema_name, input_folder, include_keys=True, ignore_existing=False, verbose=False):

        # Create the target schema if it does not exist
        if not self._graphdb.database_exists(engine_name=engine_name, schema_name=schema_name):
            self._graphdb.create_database(engine_name=engine_name, schema_name=schema_name)

        # Check if table exists and ignore if requested
        if not ignore_existing and self._graphdb.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=os.path.basename(input_folder)):
            sysmsg.warning(f"Table {schema_name}.{os.path.basename(input_folder)} already exists. Flag 'ignore_existing' set to {ignore_existing}.")
            sysmsg.warning("Table definition not imported.")
            return

        # Check if keys should be included
        file_path = f"{input_folder}/CREATE_TABLE.sql" if include_keys else f"{input_folder}/CREATE_TABLE_NO_KEYS.sql"

        # Impose soft ignore by replacing 'CREATE TABLE' with 'CREATE TABLE IF NOT EXISTS'
        # Replace existing file with the modified content.
        if ignore_existing:
            with open(file_path, 'r') as file:
                file_data = file.read()
            if 'IF NOT EXISTS' not in file_data:
                file_data = file_data.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ')
                with open(file_path, 'w') as file:
                    file.write(file_data)

        # Execute the SQL file
        self._graphdb.execute_query_from_file(engine_name=engine_name, database=schema_name, file_path=file_path, verbose=verbose)

    #---------------------------------------#
    # Method: Import table data from folder #
    #---------------------------------------#
    def import_table_data(self, engine_name, schema_name, input_folder, ignore_existing=False, verbose=False, compress=False):

        # Get list of data files from the input folder (plain or gzip-compressed SQL)
        if compress:
            list_of_sql_files = sorted(glob.glob(f'{input_folder}/*.sql.gz'))
            list_of_sql_files = [p for p in list_of_sql_files
                                if os.path.basename(p) not in ('CREATE_KEYS.sql.gz', 'CREATE_TABLE_NO_KEYS.sql.gz', 'CREATE_TABLE.sql.gz')]
        else:
            plain_sql_files = [p for p in glob.glob(f'{input_folder}/*.sql') if not p.endswith('.gz')]
            gz_sql_files = glob.glob(f'{input_folder}/*.sql.gz')
            list_of_sql_files = sorted(plain_sql_files + gz_sql_files)
            list_of_sql_files = [p for p in list_of_sql_files
                                if os.path.basename(p) not in ('CREATE_KEYS.sql', 'CREATE_TABLE_NO_KEYS.sql', 'CREATE_TABLE.sql',
                                                                 'CREATE_KEYS.sql.gz', 'CREATE_TABLE_NO_KEYS.sql.gz', 'CREATE_TABLE.sql.gz')]

        # Execute SQL files
        with tqdm(list_of_sql_files, unit='offset') as pb:
            for file_path in pb:

                # Extract table name from file path
                table_name = os.path.basename(os.path.dirname(file_path))

                # Update progress bar description
                pb.set_description(f"⚙️  Table: {table_name}".ljust(PBWIDTH)[:PBWIDTH])

                # Impose soft ignore by replacing 'INSERT INTO' with 'INSERT IGNORE INTO'.
                # Use sed in a native pipe so we don't load large files into memory.
                if ignore_existing:
                    sysmsg.warning(f"Imposing 'INSERT IGNORE' for file: {file_path}")
                    temp_path = file_path + '.tmp'
                    quoted_path = shlex.quote(file_path)
                    quoted_temp = shlex.quote(temp_path)
                    if file_path.endswith('.gz'):
                        sed_cmd = f"gzip -dc {quoted_path} | sed 's/INSERT INTO /INSERT IGNORE INTO /g' | gzip -c > {quoted_temp}"
                    else:
                        sed_cmd = f"sed 's/INSERT INTO /INSERT IGNORE INTO /g' {quoted_path} > {quoted_temp}"
                    sed_result = subprocess.run(sed_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    if sed_result.returncode != 0:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                        raise RuntimeError(
                            f"Failed to impose INSERT IGNORE for file: {file_path}\n"
                            f"STDERR: {sed_result.stderr.strip()}"
                        )
                    shutil.move(temp_path, file_path)

                # Execute import query from SQL file
                self._graphdb.execute_query_from_file(engine_name=engine_name, database=schema_name, file_path=file_path, verbose=verbose)

    #---------------------------------------------#
    # Method: Import/apply table keys from folder #
    #---------------------------------------------#
    def import_table_keys(self, engine_name, schema_name, input_folder, verbose=False):

        # Check if keys should be included
        file_path = f"{input_folder}/CREATE_KEYS.sql"

        # If no keys file exists, there is nothing to do
        if not os.path.isfile(file_path):
            if verbose:
                sysmsg.info(f"\n🔑 No CREATE_KEYS.sql found in '{input_folder}'. Skipping key import.")
            return

        # Open keys file
        with open(file_path, 'r') as file:
            sql_commands = file.read()

        # Extract table name from SQL content
        table_name_matches = re.findall(r'ALTER\s+TABLE\s+`([^`]+)`', sql_commands, re.IGNORECASE)
        if not table_name_matches:
            if verbose:
                sysmsg.info(f"\n🔑 No ALTER TABLE statements in '{file_path}'. Skipping key import.")
            return
        table_name = table_name_matches[0]

        # Loop over key creation commands
        for sql_command in sql_commands.split(';'):

            # If command is empty, skip
            if not sql_command.strip():
                continue

            # Extract key name from command
            key_name_matches = re.findall(r'\bADD\s+(?:UNIQUE\s+|FULLTEXT\s+|SPATIAL\s+)?(?:KEY|INDEX)\s+`([^`]+)`', sql_command, re.IGNORECASE)
            if not key_name_matches:
                continue
            key_name_match = key_name_matches[0]

            # Apply command if key doesn't exist
            if not self._graphdb.key_exists(engine_name=engine_name, schema_name=schema_name, table_name=table_name, key_name=key_name_match):
                if verbose:
                    sysmsg.info(f"\n🔑 Applying key '{key_name_match}' to table '{schema_name}.{table_name}' ...")
                self._graphdb.execute_query(engine_name=engine_name, schema_name=schema_name, query=sql_command, verbose=verbose)

    #----------------------------------#
    # Method: Import table from folder #
    #----------------------------------#
    def import_table(self, engine_name, schema_name, input_folder, create_keys_after_import=False, ignore_existing=False, verbose=False, compress=False):

        # Print messages only if not already printed by referring method
        if sys._getframe(1).f_code.co_name not in ['import_database', 'copy_table', 'copy_database']:

            # Print status message
            sysmsg.info(f"📝 Import table into MySQL server.")

            # Print parameters
            sysmsg.trace(f"Target engine: {engine_name}")
            sysmsg.trace(f"Table: {schema_name}")
            sysmsg.trace(f"""'create_keys_after_import' set to {'TRUE' if create_keys_after_import else 'FALSE'}.""")
            sysmsg.trace(f"Input folder: {input_folder}")

            # Print status message
            sysmsg.info(f"⚙️  Importing table from input folder into '{engine_name}' engine ...")

        # Import the table definition
        self._graphdb.import_create_table(engine_name=engine_name, schema_name=schema_name, input_folder=input_folder, include_keys=not create_keys_after_import, ignore_existing=ignore_existing, verbose=verbose)

        # Import the table data
        self._graphdb.import_table_data(engine_name=engine_name, schema_name=schema_name, input_folder=input_folder, ignore_existing=ignore_existing, verbose=verbose, compress=compress)

        # Import/apply the table keys
        if create_keys_after_import:
            self._graphdb.import_table_keys(engine_name=engine_name, schema_name=schema_name, input_folder=input_folder, verbose=verbose)

        # Print status message
        if sys._getframe(1).f_code.co_name not in ['import_database', 'copy_table', 'copy_database']:
            sysmsg.success(f"✅ Done importing table.")

    #-------------------------------------#
    # Method: Import database from folder #
    #-------------------------------------#
    def import_database(self, engine_name, schema_name, input_folder, create_keys_after_import=False, ignore_existing=False, verbose=False, compress=False):

        # Print messages only if not already printed by referring method
        if sys._getframe(1).f_code.co_name not in ['copy_table', 'copy_database']:

            # Print status message
            sysmsg.info(f"📝 Import database into MySQL server.")

            # Print parameters
            sysmsg.trace(f"Target engine: {engine_name}")
            sysmsg.trace(f"Target database: {schema_name}")
            sysmsg.trace(f"""'create_keys_after_import' set to {'TRUE' if create_keys_after_import else 'FALSE'}.""")
            sysmsg.trace(f"Input folder: {input_folder}")

            # Print status message
            sysmsg.info(f"⚙️  Importing database tables from input folder into '{engine_name}' engine ...")

        # Create the database/schema if it does not exist
        self._graphdb.execute_query(engine_name=engine_name, query=f"CREATE DATABASE IF NOT EXISTS {schema_name}")

        # Get list of table subfolders
        list_of_table_folders = [f.path for f in os.scandir(input_folder) if f.is_dir()]

        # Import each table
        for table_folder in sorted(list_of_table_folders):
            self._graphdb.import_table(engine_name=engine_name, schema_name=schema_name, input_folder=table_folder, create_keys_after_import=create_keys_after_import, ignore_existing=ignore_existing, verbose=verbose, compress=compress)

        # Print status message
        if sys._getframe(1).f_code.co_name not in ['copy_table', 'copy_database']:
            sysmsg.success(f"✅ Done importing database.")
