from __future__ import annotations

import datetime
import time
from typing import Any, Dict, List, Optional

from sqlalchemy.engine import Engine

from graphdb.adapters.schema.shared import SchemaExecutor
from graphdb.domain.models.mdl_table import Key


# Estimated processing times per operation (in seconds per row)
PROCESSING_TIMES_PER_ROW = {
    'apply_datatypes': 0.001,
    'apply_keys': 0.001,
}


class KeySchemaAdapter:
    """Adapter for key/index-level schema operations.

    Executes the same SQL as graphdb.application.core.app_graphdb.GraphDB.
    """

    def __init__(self, engine: Engine, graphdb: Any = None) -> None:
        self._exec = SchemaExecutor(engine)
        self.engine = engine
        self._graphdb = graphdb

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        query = (
            "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
            f"WHERE TABLE_SCHEMA = '{schema_name}' "
            f"AND TABLE_NAME   = '{table_name}' "
            f"AND INDEX_NAME   = '{key_name}'"
        )
        return len(self._exec.execute(query)) > 0

    def has_primary_key(self, schema_name: str, table_name: str) -> bool:
        query = f"SHOW KEYS FROM {schema_name}.{table_name} WHERE Key_name = 'PRIMARY'"
        return len(self._exec.execute(query)) > 0

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        query = f"SHOW KEYS FROM {schema_name}.{table_name} WHERE Key_name = 'PRIMARY'"
        return [row[4] for row in self._exec.execute(query) if row is not None]

    def get_keys(self, schema_name: str, table_name: str) -> Dict[str, List[str]]:
        query = f"SHOW KEYS FROM {schema_name}.{table_name}"
        keys: Dict[str, List[str]] = {}
        for row in self._exec.execute(query):
            if row is None:
                continue
            key_name = row[2]
            keys.setdefault(key_name, []).append(row[4])
        return keys

    def get_key_entities(self, schema_name: str, table_name: str) -> List[Key]:
        """Return keys/indexes as domain entities (extension)."""
        query = (
            "SELECT index_name, column_name, non_unique "
            "FROM information_schema.statistics "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
            "ORDER BY index_name, seq_in_index"
        )
        key_columns: Dict[str, List[str]] = {}
        key_unique: Dict[str, bool] = {}
        for row in self._exec.execute(query):
            name, column, non_unique = row[0], row[1], row[2]
            key_columns.setdefault(name, []).append(column)
            key_unique[name] = non_unique == 0
        return [
            Key(name=name, columns=columns, unique=key_unique.get(name, False), primary=name == "PRIMARY")
            for name, columns in key_columns.items()
        ]

    def drop_keys(self, schema_name: str, table_name: str, ignore_keys: Optional[List[str]] = None) -> None:
        ignore_keys = ignore_keys or ["row_id"]
        keys = self.get_keys(schema_name, table_name)
        if len(keys) == 0:
            return

        query = f"ALTER TABLE {schema_name}.{table_name}"
        for key_name in keys:
            if key_name in ignore_keys:
                continue
            if key_name == "PRIMARY":
                query += " DROP PRIMARY KEY,"
            else:
                query += f" DROP KEY {key_name},"

        if query.endswith(","):
            query = query[:-1]

        self._exec.execute_ddl(query)

    #-------------------------------------------#
    # Method: Apply keys to a table (from JSON) #
    #-------------------------------------------#
    def apply_keys(self, schema_name, table_name, keys_json, display_elapsed_time=False, estimated_num_rows=False):

        # Display processing time estimate
        if estimated_num_rows:

            # Display the current time
            print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")

            # Calculate the estimated processing time
            processing_time = PROCESSING_TIMES_PER_ROW['apply_keys'] * estimated_num_rows

            # Display the estimated processing time in # hours, # min and # sec format
            print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Get the column names
        if self._graphdb is not None and hasattr(self._graphdb, 'get_column_names'):
            column_names = self._graphdb.get_column_names(schema_name=schema_name, table_name=table_name)
        else:
            from graphdb.adapters.schema.sch_column import ColumnSchemaAdapter
            column_names = ColumnSchemaAdapter(self.engine).get_column_names(schema_name, table_name)

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
        if self.has_primary_key(schema_name=schema_name, table_name=table_name):
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
        if self._graphdb is not None:
            self._graphdb.execute_query_in_shell(query=sql_query)
        else:
            self._exec.execute_ddl(sql_query)

        # Display the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
