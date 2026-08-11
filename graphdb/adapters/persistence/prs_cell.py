from __future__ import annotations

from typing import Any

from loguru import logger as sysmsg


class DataCellAdapter:
    """Adapter for cell-level update/read operations."""

    def __init__(self, graphdb: Any) -> None:
        self._graphdb = graphdb

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
        self._graphdb.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose=verbose)

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
        result = self._graphdb.execute_query(engine_name=engine_name, query=sql_query) # TODO: add verbose
        if len(result) == 0:
            return []

        # Return the result as a list of tuples
        return result

