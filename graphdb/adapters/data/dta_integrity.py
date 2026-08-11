from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger as sysmsg

from graphdb.adapters.rendering.rdr_dataframe import print_dataframe


class DataIntegrityAdapter:
    """Adapter for data integrity operations."""

    def __init__(self, graphdb: Any) -> None:
        self._graphdb = graphdb

    #----------------------------------------------------------------------------#
    # Method: Delete rows from table for which keys don't exist in another table #
    #----------------------------------------------------------------------------#
    def delete_orphaned_rows(self, engine_name, upd_schema, upd_table, upd_key, ref_schema, ref_table, ref_key, upd_where='TRUE', ref_where=None, actions=()):

        # Check if update table exists (return if not)
        if not self._graphdb.table_exists(engine_name=engine_name, schema_name=upd_schema, table_name=upd_table):
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
            out = self._graphdb.execute_query(engine_name=engine_name, query=query_eval)
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
            self._graphdb.execute_query_in_shell(engine_name=engine_name, query=query_commit)
            sysmsg.success(f"✅ Orphaned rows deleted from table '{upd_table}' for key {upd_key}.")
