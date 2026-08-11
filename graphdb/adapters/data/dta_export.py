from __future__ import annotations

import gzip
import os
import shutil
import subprocess
from typing import Any

from loguru import logger as sysmsg
from sqlalchemy import text
from tqdm import tqdm


class DataExportAdapter:
    """Adapter for data operations."""

    def __init__(self, graphdb: Any) -> None:
        self._graphdb = graphdb

        #===============================================#
        #                                               #
        #   METHOD GROUP: Export tables and databases   #
        #                                               #
        #===============================================#

        #--------------------------------------------#
        # Method: Export table definitions to folder #
        #--------------------------------------------#
        def export_create_table(self, engine_name, schema_name, table_name, output_folder):

            # Append schema and table name to output folder
            output_folder = f"{output_folder}/{schema_name}/{table_name}"

            # Create the output folder if it does not exist
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)

            # Get table definition
            create_table_sql = self._graphdb.get_create_table(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

            # Fix auto increment issues
            create_table_sql = create_table_sql.replace("`row_id` int NOT NULL AUTO_INCREMENT,", "`row_id` int NOT NULL AUTO_INCREMENT UNIQUE KEY,")
            create_table_sql = create_table_sql.replace("`row_id` int unsigned NOT NULL AUTO_INCREMENT,", "`row_id` int NOT NULL AUTO_INCREMENT UNIQUE KEY,")
            create_table_sql = re.sub(r"AUTO_INCREMENT=\d+", "AUTO_INCREMENT=1", create_table_sql)

            # Extract only the keys definition chunk
            create_table_sql_keys_chunk = '\n'.join(re.findall(r'(?m)^\s*(?!PRIMARY KEY)(?:UNIQUE KEY|KEY|INDEX|CONSTRAINT)\b.*$', create_table_sql))

            # Generate table definition with no keys (except the PRIMARY KEY)
            create_table_no_keys_sql = create_table_sql.replace(create_table_sql_keys_chunk, "").replace(",\n\n) ENGINE", "\n) ENGINE")

            # Generate the ALTER TABLE operation to add the keys to the existing table
            create_keys_sql = ''
            for line in create_table_sql_keys_chunk.split('\n'):
                if 'UNIQUE KEY' in line or 'KEY' in line or 'INDEX' in line or 'CONSTRAINT' in line:
                    line = line.strip()
                    line = line[:-1] if line.endswith(',') else line
                    # TODO: ...
                    # for key_type in ('UNIQUE KEY', 'KEY', 'INDEX', 'CONSTRAINT'):
                    #     if "IF NOT EXISTS" not in line:
                    #         line = line.replace(key_type, key_type+" IF NOT EXISTS ")
                    create_keys_sql += f"ALTER TABLE `{table_name}` ADD {line};\n"

            # Save all definitions to output folder
            with open(f"{output_folder}/CREATE_TABLE.sql", "w") as f:
                f.write(create_table_sql + ";\n")
            with open(f"{output_folder}/CREATE_TABLE_NO_KEYS.sql", "w") as f:
                f.write(create_table_no_keys_sql + ";\n")
            with open(f"{output_folder}/CREATE_KEYS.sql", "w") as f:
                f.write(create_keys_sql + "\n")

        #-------------------------------------#
        # Method: Export table data to folder #
        #-------------------------------------#
        def export_table_data(self, engine_name, schema_name, table_name, output_folder, filter_by='TRUE', chunk_size=1000000, compress=False):

            # Append schema and table name to output folder
            output_folder = f"{output_folder}/{schema_name}/{table_name}"

            # Create the output folder if it does not exist
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)

            def _clean_dump_stderr(stderr_text):
                warning_fragment = "Using a password on the command line interface can be insecure."
                lines = []
                for line in (stderr_text or "").splitlines():
                    if warning_fragment in line:
                        continue
                    lines.append(line)
                return "\n".join(lines).strip()

            # Check if row_id column exists in the table
            check_column_query = f"""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'row_id'
            """
            has_row_id = int(self._graphdb.execute_query(engine_name=engine_name, query=check_column_query)[0][0]) > 0

            # If row_id exists, proceed with chunked dump
            if has_row_id:

                # Get minimum row_id
                min_row_id = self._graphdb.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MIN(row_id),0) FROM {schema_name}.{table_name} WHERE {filter_by}")[0][0]

                # Get maximum row_id
                max_row_id = self._graphdb.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MAX(row_id),0) FROM {schema_name}.{table_name} WHERE {filter_by}")[0][0]

                # Convert values to integers
                min_row_id = int(min_row_id)
                max_row_id = int(max_row_id)

                # Check if there are any rows to process
                if min_row_id > max_row_id:
                    sysmsg.warning(f"No rows found in table {schema_name}.{table_name} with filter '{filter_by}'.")
                    return

                # Process table in chunks (from min to max row_id)
                with tqdm(range(min_row_id-1, max_row_id+1, chunk_size), unit='offset') as pb:
                    for offset in pb:

                        # Update progress bar description
                        pb.set_description(f"⚙️  Table: {table_name}".ljust(PBWIDTH)[:PBWIDTH])

                        # Generate output file path
                        if compress:
                            output_file = f'{output_folder}/{table_name}_{str(offset).zfill(10)}.sql.gz'
                            # Avoid re-exporting an existing plain SQL file too
                            existing_plain = output_file[:-3]
                        else:
                            output_file = f'{output_folder}/{table_name}_{str(offset).zfill(10)}.sql'
                            existing_plain = output_file

                        # Check if the output file already exists
                        if os.path.exists(output_file) or os.path.exists(existing_plain):
                            continue

                        # Generate shell command to dump table chunck using mysqldump executable
                        shell_command = self._graphdb.base_command_mysqldump[engine_name] + [
                            schema_name,
                            table_name,
                            f'--where={filter_by} AND (row_id BETWEEN {offset} AND {offset + chunk_size - 1})',
                        ]

                        # Run the command and capture stdout and stderr
                        if compress:
                            proc = subprocess.Popen(
                                shell_command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=self._graphdb.subprocess_env.get(engine_name),
                            )
                            try:
                                with gzip.open(output_file, 'wb', compresslevel=6) as gz_out:
                                    while True:
                                        chunk = proc.stdout.read(65536)
                                        if not chunk:
                                            break
                                        gz_out.write(chunk)
                            finally:
                                stderr_bytes = proc.stderr.read()
                                result = proc.wait()
                            clean_stderr = _clean_dump_stderr(stderr_bytes.decode('utf-8', errors='replace'))
                            if result != 0:
                                message = (
                                    f"Failed to dump table chunk {schema_name}.{table_name} (offset={offset}).\n"
                                    f"Return code: {result}\n"
                                    f"STDERR:\n{clean_stderr or '<empty>'}\n"
                                    f"STDOUT:\n<captured into gzip file>"
                                )
                                sysmsg.critical(message)
                                raise RuntimeError(message)
                        else:
                            shell_command = shell_command + ['--result-file=' + output_file]
                            result = subprocess.run(
                                shell_command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                env=self._graphdb.subprocess_env.get(engine_name),
                            )
                            clean_stderr = _clean_dump_stderr(result.stderr)
                            if result.returncode != 0:
                                message = (
                                    f"Failed to dump table chunk {schema_name}.{table_name} (offset={offset}).\n"
                                    f"Return code: {result.returncode}\n"
                                    f"STDERR:\n{clean_stderr or '<empty>'}\n"
                                    f"STDOUT:\n{result.stdout.strip() or '<empty>'}"
                                )
                                sysmsg.critical(message)
                                raise RuntimeError(message)

            # Else, if row_id does not exist, dump the entire table at once
            else:

                # Generate output file path
                if compress:
                    output_file = f'{output_folder}/{table_name}_FULL.sql.gz'
                    existing_plain = output_file[:-3]
                else:
                    output_file = f'{output_folder}/{table_name}_FULL.sql'
                    existing_plain = output_file

                # Check if the output file already exists
                if os.path.exists(output_file) or os.path.exists(existing_plain):
                    sysmsg.warning(f"Output file {output_file} already exists. Skipping dump for table '{table_name}'.")
                    return

                # Fallback: dump entire table with optional filter
                shell_command = self._graphdb.base_command_mysqldump[engine_name] + [
                    schema_name,
                    table_name,
                    f'--where={filter_by}',
                ]

                # Run the command and capture stdout and stderr
                if compress:
                    proc = subprocess.Popen(
                        shell_command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        env=self._graphdb.subprocess_env.get(engine_name),
                    )
                    try:
                        with gzip.open(output_file, 'wb', compresslevel=6) as gz_out:
                            while True:
                                chunk = proc.stdout.read(65536)
                                if not chunk:
                                    break
                                gz_out.write(chunk)
                    finally:
                        stderr_bytes = proc.stderr.read()
                        result = proc.wait()
                    clean_stderr = _clean_dump_stderr(stderr_bytes.decode('utf-8', errors='replace'))
                    if result != 0:
                        message = (
                            f"Failed to dump table {schema_name}.{table_name}.\n"
                            f"Return code: {result}\n"
                            f"STDERR:\n{clean_stderr or '<empty>'}\n"
                            f"STDOUT:\n<captured into gzip file>"
                        )
                        sysmsg.critical(message)
                        raise RuntimeError(message)
                else:
                    shell_command = shell_command + [f'--result-file={output_file}']
                    result = subprocess.run(
                        shell_command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        env=self._graphdb.subprocess_env.get(engine_name),
                    )
                    clean_stderr = _clean_dump_stderr(result.stderr)
                    if result.returncode != 0:
                        message = (
                            f"Failed to dump table {schema_name}.{table_name}.\n"
                            f"Return code: {result.returncode}\n"
                            f"STDERR:\n{clean_stderr or '<empty>'}\n"
                            f"STDOUT:\n{result.stdout.strip() or '<empty>'}"
                        )
                        sysmsg.critical(message)
                        raise RuntimeError(message)

        #--------------------------------#
        # Method: Export table to folder #
        #--------------------------------#
        def export_table(self, engine_name, schema_name, table_name, output_folder, filter_by='TRUE', chunk_size=1000000, include_create_tables=False, compress=False):

            # Print messages only if not already printed by referring method
            if sys._getframe(1).f_code.co_name not in ['export_database', 'copy_table', 'copy_database']:

                # Print status message
                sysmsg.info(f"📝 Export table from MySQL server.")

                # Print parameters
                sysmsg.trace(f"Target engine: {engine_name}")
                sysmsg.trace(f"Database: {schema_name}")
                sysmsg.trace(f"Table: {table_name}")
                if filter_by!='TRUE':
                    sysmsg.trace(f"WHERE condition: {filter_by}")
                if chunk_size!=1000000:
                    sysmsg.trace(f"Chunk size: {chunk_size}")
                sysmsg.trace(f"""'include_create_tables' set to {'TRUE' if include_create_tables else 'FALSE'}.""")
                sysmsg.trace(f"Output folder: {output_folder}")

                # Print status message
                sysmsg.info(f"⚙️  Exporting table from '{engine_name}' engine to ouput folder ...")

            # Include create table statement if requested
            if include_create_tables:
                self._graphdb.export_create_table(engine_name=engine_name, schema_name=schema_name, table_name=table_name, output_folder=output_folder)

            # Export data to output folder
            self._graphdb.export_table_data(engine_name=engine_name, schema_name=schema_name, table_name=table_name, output_folder=output_folder, filter_by=filter_by, chunk_size=chunk_size, compress=compress)

            # Print status message
            if sys._getframe(1).f_code.co_name not in ['export_database', 'copy_table', 'copy_database']:
                sysmsg.success(f"✅ Done exporting table.")

        #------------------------------------------------------------#
        # Method: Export all table definitions in database to folder #
        #------------------------------------------------------------#
        def export_create_tables_in_database(self, engine_name, schema_name, output_folder):
            for table_name in self._graphdb.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name):
                self._graphdb.export_create_table(engine_name=engine_name, schema_name=schema_name, table_name=table_name, output_folder=output_folder)

        #-----------------------------------------------------#
        # Method: Export all table data in database to folder #
        #-----------------------------------------------------#
        def export_table_data_in_database(self, engine_name, schema_name, output_folder, filter_by='TRUE', chunk_size=1000000, compress=False):
            for table_name in self._graphdb.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name):
                self._graphdb.export_table_data(engine_name=engine_name, schema_name=schema_name, table_name=table_name, output_folder=output_folder, filter_by=filter_by, chunk_size=chunk_size, compress=compress)

        #-----------------------------------------------------#
        # Method: Export all table data in database to folder #
        #-----------------------------------------------------#
        def export_database(self, engine_name, schema_name, output_folder, filter_by='TRUE', chunk_size=1000000, include_create_tables=False, compress=False):

            # Print messages only if not already printed by referring method
            if sys._getframe(1).f_code.co_name not in ['copy_table', 'copy_database']:

                # Print status message
                sysmsg.info(f"📝 Export database from MySQL server.")

                # Print parameters
                sysmsg.trace(f"Target engine: {engine_name}")
                sysmsg.trace(f"Database: {schema_name}")
                if filter_by!='TRUE':
                    sysmsg.trace(f"WHERE condition: {filter_by}")
                if chunk_size!=1000000:
                    sysmsg.trace(f"Chunk size: {chunk_size}")
                sysmsg.trace(f"""'include_create_tables' set to {'TRUE' if include_create_tables else 'FALSE'}.""")
                sysmsg.trace(f"Output folder: {output_folder}")

                # Print status message
                sysmsg.info(f"⚙️  Exporting database tables from '{engine_name}' engine to ouput folder ...")

            # Get list of tables in database
            list_of_tables = self._graphdb.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name)

            # Export all tables
            for table_name in sorted(list_of_tables):
                self._graphdb.export_table(engine_name=engine_name, schema_name=schema_name, table_name=table_name, output_folder=output_folder, filter_by=filter_by, chunk_size=chunk_size, include_create_tables=include_create_tables, compress=compress)

            # Print status message
            if sys._getframe(1).f_code.co_name not in ['copy_table', 'copy_database']:
                sysmsg.success(f"✅ Done exporting database.")

