from __future__ import annotations

import datetime
import time
from typing import Any, Dict, List

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor
from graphdb.domain.models.mdl_table import Column


# Estimated processing times per operation (in seconds per row)
PROCESSING_TIMES_PER_ROW = {
    'apply_datatypes': 0.001,
    'apply_keys': 0.001,
}


class ColumnSchemaAdapter:
    """Adapter for column-level schema introspection against a SQLAlchemy engine."""

    def __init__(self, engine: Engine, graphdb: Any = None) -> None:
        self._exec = SchemaExecutor(engine)
        self._graphdb = graphdb

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        query = (
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema_name}' "
            f"AND TABLE_NAME   = '{table_name}' "
            f"AND COLUMN_NAME  = '{column_name}'"
        )
        return len(self._exec.execute(query)) > 0

    def has_column(self, schema_name: str, table_name: str, column_name: str) -> bool:
        """Alias for column_exists; matches legacy GraphDB naming."""
        return self.column_exists(schema_name, table_name, column_name)

    def get_column_names(self, schema_name: str, table_name: str) -> List[str]:
        query = f"SHOW COLUMNS FROM {schema_name}.{table_name}"
        return [row[0] for row in self._exec.execute(query) if row is not None]

    def get_column_datatypes(self, schema_name: str, table_name: str) -> Dict[str, str]:
        query = f"SHOW COLUMNS FROM {schema_name}.{table_name}"
        datatypes: Dict[str, str] = {}
        for row in self._exec.execute(query):
            if row is None:
                continue
            datatypes[row[0]] = row[1]
        return datatypes

    def get_columns(self, schema_name: str, table_name: str) -> List[Column]:
        """Return column definitions as domain entities (extension)."""
        query = (
            "SELECT column_name, column_type, is_nullable, column_default "
            "FROM information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        )
        columns: List[Column] = []
        for row in self._exec.execute(query):
            name, datatype, nullable_str, default = row[0], row[1], row[2], row[3]
            nullable = {"YES": True, "NO": False}.get(nullable_str) if nullable_str else None
            columns.append(Column(name=name, datatype=datatype, nullable=nullable, default=default))
        return columns

    #-------------------------------------------------#
    # Method: Apply data types to a table (from JSON) #
    #-------------------------------------------------#
    def apply_datatypes(self, schema_name, table_name, datatypes_json, display_elapsed_time=False, estimated_num_rows=False):

        # Display processing time estimate
        if estimated_num_rows:

            # Display the current time
            print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")

            # Calculate the estimated processing time
            processing_time = PROCESSING_TIMES_PER_ROW['apply_datatypes'] * estimated_num_rows

            # Display the estimated processing time in # hours, # min and # sec format
            print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Get the column names
        column_names = self.get_column_names(schema_name=schema_name, table_name=table_name)

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
        if self._graphdb is not None:
            self._graphdb.execute_query_in_shell(query=sql_query)
        else:
            self._exec.execute(sql_query)

        # Print the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
