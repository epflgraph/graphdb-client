from __future__ import annotations

import inspect
import re
from typing import Any

import pandas as pd
from tabulate import tabulate

from graphdb.application.core.cfg_config import GraphDBConfigError


def print_colour(msg, colour='white', background='black', style='normal', display_method=False):
    """Print a message with ANSI colour/style codes."""
    colour_codes = {
        'black': 30, 'red': 31, 'green': 32, 'yellow': 33,
        'blue': 34, 'purple': 35, 'magenta': 35, 'cyan': 36, 'white': 37
    }
    background_codes = {
        'black': 40, 'red': 41, 'green': 42, 'yellow': 43,
        'blue': 44, 'purple': 45, 'magenta': 45, 'cyan': 46, 'white': 47
    }
    style_codes = {
        'normal': 0, 'bold': 1, 'underline': 4,
        'blink': 5, 'reverse': 7, 'hidden': 8
    }

    if display_method:
        frame = inspect.currentframe().f_back
        method = frame.f_code.co_name
        class_name = None
        if 'self' in frame.f_locals:
            class_name = type(frame.f_locals['self']).__name__
        elif 'cls' in frame.f_locals:
            class_name = frame.f_locals['cls'].__name__
        prefix = f"[{class_name}.{method}] " if class_name else f"[{method}] "
        msg = prefix + msg

    colour_code = colour_codes.get(colour, 37)
    background_code = background_codes.get(background, 40)
    style_code = style_codes.get(style, 0)
    print(f"\033[{style_code};{colour_code};{background_code}m{msg}\033[0m")


class DisplayAdapter:
    """Adapter for display/printing operations."""

    def __init__(self, graphdb: Any) -> None:
        self._graphdb = graphdb

    #----------------------------------------------#
    # Method: Print list of schemas in an engine   #
    #----------------------------------------------#
    def print_schemas(self, engine_name):
        print(f"List of schemas in {engine_name}:")
        for r in self._graphdb.execute_query(engine_name=engine_name, query='SHOW DATABASES'):
            print(' - ', r[0])

    #----------------------------------------#
    # Method: Get list of tables in a schema #
    #----------------------------------------#
    def print_tables_in_schema(self, engine_name, schema_name):
        print(f"Tables in schema {schema_name}:")
        for r in self._graphdb.execute_query(engine_name=engine_name, query=f'SHOW TABLES IN {schema_name}'):
            print(' - ', r[0])

    #----------------------------------------------#
    # Method: Print list of tables in the cache    #
    #----------------------------------------------#
    def print_tables_in_cache(self):
        if not self._graphdb.config.schema_cache:
            raise GraphDBConfigError("Missing config key: schema_cache")
        self._graphdb.print_tables_in_schema(engine_name=self._graphdb.default_engine_name, schema_name=self._graphdb.config.schema_cache)

    #----------------------------------------------#
    # Method: Print list of tables in the test     #
    #----------------------------------------------#
    def print_tables_in_test(self):
        if not self._graphdb.config.schema_test:
            raise GraphDBConfigError("Missing config key: schema_test")
        self._graphdb.print_tables_in_schema(engine_name=self._graphdb.default_engine_name, schema_name=self._graphdb.config.schema_test)

    #----------------------------#
    # Method: Get database stats #
    #----------------------------#
    def print_database_stats(self, engine_name, schema_name, re_include=[], re_exclude=[]):

        # Get list of tables in the schema
        list_of_tables = sorted(self._graphdb.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name))

        # Apply include/exclude filters
        if len(re_include) > 0:
            list_of_tables = [t for t in list_of_tables if     any(re.search(pattern, t) for pattern in re_include)]
        if len(re_exclude) > 0:
            list_of_tables = [t for t in list_of_tables if not any(re.search(pattern, t) for pattern in re_exclude)]

        # Print line break
        print('')

        # Loop over the tables
        for table_name in list_of_tables:

            # Get the row count
            row_count = self._graphdb.execute_query(engine_name=engine_name, query=f"SELECT COUNT(*) FROM {schema_name}.{table_name};")[0][0]

            # Print table : row count (in red if =0 else in blue)
            if row_count > 0:
                print(f"\033[34m{table_name}: {row_count}\033[0m")
            else:
                print(f"\033[31m{table_name}: {row_count}\033[0m")

        # Print line break
        print('')
