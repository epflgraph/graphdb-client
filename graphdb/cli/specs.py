# graphdb/cli/specs.py
# This module defines the specifications for the CLI commands, including their arguments and handler functions.
from functools import lru_cache
from typing import Any, Dict, Tuple

from graphdb.core.config import GraphDBConfig, GraphDBConfigError

# Import all command handler functions
from graphdb.cli.commands import (
    cmd_config,
    cmd_test,
    cmd_inspect,
    cmd_export,
    cmd_import,
    cmd_copy,
    cmd_compare,
)


@lru_cache(maxsize=1)
def _load_cli_env_settings() -> Tuple[Tuple[str, ...], str, str]:
    """Load CLI environment settings from the default config file."""
    try:
        cfg = GraphDBConfig.from_default_file()
        envs = tuple(cfg.env_names())
        default_env = cfg.default_env
    except (GraphDBConfigError, OSError, ValueError):
        raise RuntimeError(
            "Failed to load CLI environment settings from configuration file. "
            "Please ensure the config file is present and properly formatted."
        )
    second_env = envs[1] if len(envs) > 1 else default_env
    return envs, default_env, second_env


def get_cli_env_settings() -> Tuple[Tuple[str, ...], str, str]:
    """Return configured environments, default environment, and fallback second environment."""
    return _load_cli_env_settings()


def global_common_args(envs: Tuple[str, ...], default_env: str) -> Dict[str, Any]:
    return {
        "env": dict(
            flags=("--env",),
            kwargs=dict(
                help="Specify environment.",
                choices=envs,
                default=default_env,
            ),
        )
    }


def get_cli_definitions() -> Dict[str, Any]:
    """Build CLI command definitions using the configured environments."""
    envs, default_env, second_env = _load_cli_env_settings()
    common_args = global_common_args(envs, default_env)

    return {
        #-----------------#
        # Command: config #
        #-----------------#
        "config": dict(
            help="Manage and print configuration options.",
            common_args={},
            commands={
                "print": dict(
                    help="Print out config options.",
                    func=cmd_config,
                    requires_db=False,
                    args=[],
                    common_args=[],
                )
            },
        ),

        #---------------#
        # Command: test #
        #---------------#
        "test": dict(
            help="Test server connectivity.",
            common_args={"env": common_args["env"]},
            func=cmd_test,
            args=[],
            common_args_order=["env"],
        ),

        #------------------#
        # Command: inspect #
        #------------------#
        "inspect": dict(
            help="Inspect and render SQLQuery capabilities from the CLI.",
            common_args={"env": common_args["env"]},
            func=cmd_inspect,
            requires_db=False,
            args=[
                dict(flags=("--query",), kwargs=dict(required=False, type=str, default=None, help="Raw SQL query text.")),
                dict(flags=("--select",), kwargs=dict(required=False, type=str, default=None, help="SELECT clause for SQLQuery.from_parts.")),
                dict(flags=("--from",), kwargs=dict(dest="from_", required=False, type=str, default=None, help="FROM clause for SQLQuery.from_parts.")),
                dict(flags=("--where",), kwargs=dict(required=False, type=str, default=None, help="Optional WHERE clause for SQLQuery.from_parts.")),
                dict(flags=("--description",), kwargs=dict(required=False, type=str, default="", help="Human-readable SQL description.")),
                dict(flags=("--title",), kwargs=dict(required=False, type=str, default="SQL", help="Render title.")),
                dict(flags=("--params-json",), kwargs=dict(required=False, type=str, default=None, help="JSON object/array of parameters.")),
                dict(flags=("--elapsed-ms",), kwargs=dict(required=False, type=float, default=None, help="Preset elapsed time in ms.")),
                dict(flags=("--row-count",), kwargs=dict(required=False, type=int, default=None, help="Preset row count metadata.")),
                dict(flags=("--error",), kwargs=dict(required=False, type=str, default=None, help="Preset error metadata.")),
                dict(flags=("--box-style",), kwargs=dict(required=False, type=str, default="minimal", choices=("rounded", "heavy", "double", "minimal", "simple", "none"), help="Rich panel box style.")),
                dict(flags=("--theme",), kwargs=dict(required=False, type=str, default="monokai", help="Rich syntax theme.")),
                dict(flags=("--copyable",), kwargs=dict(action="store_true", default=False, help="Render in copyable mode.")),
                dict(flags=("--debug",), kwargs=dict(action="store_true", default=False, help="Render debug panel with fingerprint and query id.")),
                dict(flags=("--snapshot",), kwargs=dict(action="store_true", default=False, help="Print debug snapshot dict.")),
                dict(flags=("--show-fingerprint",), kwargs=dict(action="store_true", default=False, help="Print SQL fingerprint.")),
                dict(flags=("--fingerprint-with-params",), kwargs=dict(action="store_true", default=False, help="Include params when generating fingerprint.")),
                dict(flags=("--show-canonical",), kwargs=dict(action="store_true", default=False, help="Print canonical SQL (single-line normalized).")),
                dict(flags=("--show-one-line",), kwargs=dict(action="store_true", default=False, help="Print one-line SQL preview.")),
                dict(flags=("--one-line-len",), kwargs=dict(required=False, type=int, default=120, help="Max length for --show-one-line.")),
                dict(flags=("--no-redact-params",), kwargs=dict(action="store_true", default=False, help="Display params without redaction.")),
                dict(flags=("--time-demo",), kwargs=dict(action="store_true", default=False, help="Run execute_with_timing() with a successful fake executor.")),
                dict(flags=("--time-fail-demo",), kwargs=dict(action="store_true", default=False, help="Run execute_with_timing() with a failing fake executor.")),
            ],
            common_args_order=["env"],
        ),

        #-----------------#
        # Command: export #
        #-----------------#
        "export": dict(
            help="Export database into local folder.",
            common_args={"env": common_args["env"]},
            func=cmd_export,
            args=[
                dict(flags=("--schema_name",), kwargs=dict(required=True, type=str, help="Name of the database/schema to export.")),
                dict(flags=("--output_folder",), kwargs=dict(required=True, type=str, help="Output folder to save the exported data into.")),
                dict(flags=("--table_name",), kwargs=dict(required=False, type=str, default=None, help="Name of the table to export (if exporting only one table).")),
                dict(flags=("--filter_by",), kwargs=dict(required=False, type=str, default="TRUE", help="Filter condition to apply to all tables.")),
                dict(flags=("--chunk_size",), kwargs=dict(required=False, type=int, default=10000, help="Number of documents to export per batch (default=10000).")),
                dict(flags=("--include_create_tables", "-c"), kwargs=dict(action="store_true", default=False, help="Include table definitions in export.")),
                dict(flags=("--include_data", "-d"), kwargs=dict(action="store_true", default=False, help="Include data in export.")),
                dict(flags=("--compress", "-z"), kwargs=dict(action="store_true", default=False, help="Compress exported SQL chunks with gzip as .sql.gz.")),
            ],
            common_args_order=["env"],
        ),

        #-----------------#
        # Command: import #
        #-----------------#
        "import": dict(
            help="Import database from local folder.",
            common_args={"env": common_args["env"]},
            func=cmd_import,
            args=[
                dict(flags=("--schema_name",), kwargs=dict(required=True, type=str, help="Name of the database/schema to import.")),
                dict(flags=("--input_folder",), kwargs=dict(required=True, type=str, help="Input folder containing the data to import.")),
                dict(flags=("--table_name",), kwargs=dict(required=False, type=str, default=None, help="Name of the table to import (if importing only one table).")),
                dict(flags=("--include_create_tables", "-c"), kwargs=dict(action="store_true", default=False, help="Include table definitions in import.")),
                dict(flags=("--include_data", "-d"), kwargs=dict(action="store_true", default=False, help="Include data in import.")),
                dict(flags=("--ignore_existing", "-i"), kwargs=dict(action="store_true", default=False, help="Soft ignore table creation and existing rows.")),
                dict(flags=("--verbose", "-v"), kwargs=dict(action="store_true", default=False, help="Verbose mode.")),
                dict(flags=("--compress", "-z"), kwargs=dict(action="store_true", default=False, help="Import gzip-compressed .sql.gz data files.")),
            ],
            common_args_order=["env"],
        ),

        #---------------#
        # Command: copy #
        #---------------#
        "copy": dict(
            help="Copy database or tables across servers.",
            common_args={},
            func=cmd_copy,
            args=[
                dict(flags=("--from_env",), kwargs=dict(required=False, type=str, choices=envs, default=default_env, help="Source environment.")),
                dict(flags=("--to_env",), kwargs=dict(required=False, type=str, choices=envs, default=second_env, help="Target environment.")),
                dict(flags=("--from_schema",), kwargs=dict(required=True, type=str, help="Name of the source database/schema to copy from.")),
                dict(flags=("--to_schema",), kwargs=dict(required=True, type=str, help="Name of the target database/schema to copy to.")),
                dict(flags=("--table_name",), kwargs=dict(required=False, type=str, default=None, help="Name of the table to export (optional).")),
                dict(flags=("--chunk_size",), kwargs=dict(required=False, type=int, default=10000, help="Number of rows to copy per batch (default=10000).")),
                dict(flags=("--compress", "-z"), kwargs=dict(action="store_true", default=False, help="Compress exported SQL chunks with gzip as .sql.gz to save disk space.")),
            ],
            common_args_order=[],
        ),

        #------------------#
        # Command: compare #
        #------------------#
        "compare": dict(
            help="Compare database or tables across servers.",
            common_args={},
            func=cmd_compare,
            args=[
                dict(flags=("--from_env",), kwargs=dict(required=False, type=str, choices=envs, default=default_env, help="Source environment.")),
                dict(flags=("--to_env",), kwargs=dict(required=False, type=str, choices=envs, default=second_env, help="Target environment.")),
                dict(flags=("--from_schema",), kwargs=dict(required=True, type=str, help="Name of the source database/schema to compare.")),
                dict(flags=("--to_schema",), kwargs=dict(required=True, type=str, help="Name of the target database/schema to compare.")),
                dict(flags=("--table_name",), kwargs=dict(required=False, type=str, default=None, help="Name of the table to compare (if comparing only one table).")),
                dict(flags=("--row_count_tolerance",), kwargs=dict(required=False, type=float, default=0.10, help="Relative row-count difference below which a mismatch is reported as a warning instead of an error (default: 0.10 for 10%%).")),
                dict(flags=("--ignore_warnings", "-iw"), kwargs=dict(action="store_true", default=False, help="Skip output for tables that have only warnings and no errors.")),
            ],
            common_args_order=[],
        ),
    }
