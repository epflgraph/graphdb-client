# graphdb/cli/specs.py
# This module defines the specifications for the CLI commands, including their arguments and handler functions.
from typing import Any, Dict
from graphdb.core.config import GraphDBConfig, GraphDBConfigError

# Import all command handler functions
from graphdb.cli.commands import (
    cmd_config_index,
    cmd_test,
    cmd_export,
    cmd_import,
    cmd_copy,
    cmd_compare
)

def _load_cli_env_settings():
    try:
        cfg = GraphDBConfig.from_default_file()
        envs = tuple(cfg.env_names())
        default_env = cfg.default_env
    except (GraphDBConfigError, OSError, ValueError):
        envs = ('test',)
        default_env = 'test'
    return envs, default_env


_CLI_ENVS, _CLI_DEFAULT_ENV = _load_cli_env_settings()
_CLI_SECOND_ENV = _CLI_ENVS[1] if len(_CLI_ENVS) > 1 else _CLI_DEFAULT_ENV

# Global common arguments
global_common_args = {
    'env' : dict(
        flags = ('--env',),
        kwargs = dict(
            help = "Specify environment.",
            choices = _CLI_ENVS,
            default = _CLI_DEFAULT_ENV
        )
    )
}

#===================================================#
# CLI Definitions for all Subcommands and Arguments #
#===================================================#
cli_definitions: Dict[str, Any] = {
    'config' : dict(
        help = "Inspect and validate Registry configuration files.",
        common_args = {},
        commands = {
            'index' : dict(
                help = "Print out index config.",
                func = cmd_config_index,
                requires_db = False,
                args = [],
                common_args = [],
            )
        }
    ),

    'test' : dict(
        help = "Test server connectivity.",
        common_args = {
            'env': global_common_args['env']
        },
        func = cmd_test,
        args = [],
        common_args_order = ['env'],
    ),

    'export' : dict(
        help = "Export database into local folder.",
        common_args = {
            'env': global_common_args['env']
        },
        func = cmd_export,
        args = [
            dict(flags = ('--schema_name'  ,), kwargs = dict(required=True,  type=str, help="Name of the database/schema to export.")),
            dict(flags = ('--output_folder',), kwargs = dict(required=True,  type=str, help="Output folder to save the exported data into.")),
            dict(flags = ('--table_name'   ,), kwargs = dict(required=False, type=str, default=None,    help="Name of the table to export (if exporting only one table).")),
            dict(flags = ('--filter_by'    ,), kwargs = dict(required=False, type=str, default='TRUE',  help="Filter condition to apply to all tables.")),
            dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of documents to export per batch (default=1000000).")),
            dict(flags = ('--include_create_tables', '-c'), kwargs = dict(action='store_true', default=False, help="Include table definitions in export.")),
            dict(flags = ('--include_data'         , '-d'), kwargs = dict(action='store_true', default=False, help="Include data in export."))
        ],
        common_args_order = ['env'],
    ),

    'import' : dict(
        help = "Import database from local folder.",
        common_args = {
            'env': global_common_args['env']
        },
        func = cmd_import,
        args = [
            dict(flags = ('--schema_name' ,), kwargs = dict(required=True,  type=str, help="Name of the database/schema to import.")),
            dict(flags = ('--input_folder',), kwargs = dict(required=True,  type=str, help="Input folder containing the data to import.")),
            dict(flags = ('--table_name'  ,), kwargs = dict(required=False, type=str, default=None, help="Name of the table to import (if importing only one table).")),
            dict(flags = ('--include_create_tables', '-c'), kwargs = dict(action='store_true', default=False, help="Include table definitions in import.")),
            dict(flags = ('--include_data'         , '-d'), kwargs = dict(action='store_true', default=False, help="Include data in import.")),
            dict(flags = ('--ignore_existing'      , '-i'), kwargs = dict(action='store_true', default=False, help="Soft ignore table creation and existing rows."))
        ],
        common_args_order = ['env'],
    ),

    'copy' : dict(
        help = "Copy database or tables across servers.",
        common_args = {},
        func = cmd_copy,
        args = [
            dict(flags = ('--from_env'   ,), kwargs = dict(required=False, type=str, choices=_CLI_ENVS, default=_CLI_DEFAULT_ENV, help="Source environment.")),
            dict(flags = ('--to_env'     ,), kwargs = dict(required=False, type=str, choices=_CLI_ENVS, default=_CLI_SECOND_ENV,  help="Target environment.")),
            dict(flags = ('--from_schema',), kwargs = dict(required=True,  type=str, help="Name of the source database/schema to copy from.")),
            dict(flags = ('--to_schema'  ,), kwargs = dict(required=True,  type=str, help="Name of the target database/schema to copy to.")),
            dict(flags = ('--table_name' ,), kwargs = dict(required=False, type=str, default=None,    help="Name of the table to export (optional).")),
            dict(flags = ('--chunk_size' ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of rows to copy per batch (default=1000000).")),
        ],
        common_args_order = [],
    ),

    'compare' : dict(
        help = "Compare database or tables across servers.",
        common_args = {},
        func = cmd_compare,
        args = [
            dict(flags = ('--from_env'   ,), kwargs = dict(required=False, type=str, choices=_CLI_ENVS, default=_CLI_DEFAULT_ENV, help="Source environment.")),
            dict(flags = ('--to_env'     ,), kwargs = dict(required=False, type=str, choices=_CLI_ENVS, default=_CLI_SECOND_ENV,  help="Target environment.")),
            dict(flags = ('--from_schema',), kwargs = dict(required=True,  type=str, help="Name of the source database/schema to compare.")),
            dict(flags = ('--to_schema'  ,), kwargs = dict(required=True,  type=str, help="Name of the target database/schema to compare.")),
            dict(flags = ('--table_name' ,), kwargs = dict(required=False, type=str, default=None, help="Name of the table to compare (if comparing only one table).")),
            dict(flags = ('--exact_row_count', '-e'), kwargs = dict(action='store_true', default=False, help="Calculate exact row counts (slower)."))
        ],
        common_args_order = [],
    )
}
