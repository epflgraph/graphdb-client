# graphdb/entrypoints/cli/__init__.py
# Re-export CLI command handlers for backward-compatible registration.
from graphdb.entrypoints.cli.cmd_config import cmd_config
from graphdb.entrypoints.cli.cmd_test import cmd_test
from graphdb.entrypoints.cli.cmd_inspect import cmd_inspect
from graphdb.entrypoints.cli.cmd_export import cmd_export
from graphdb.entrypoints.cli.cmd_import import cmd_import
from graphdb.entrypoints.cli.cmd_copy import cmd_copy
from graphdb.entrypoints.cli.cmd_compare import cmd_compare

__all__ = [
    "cmd_config",
    "cmd_test",
    "cmd_inspect",
    "cmd_export",
    "cmd_import",
    "cmd_copy",
    "cmd_compare",
]
