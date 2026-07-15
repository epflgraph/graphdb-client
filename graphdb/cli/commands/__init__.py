# graphdb/cli/commands/__init__.py
# Re-export command handlers for backward-compatible registration.
from graphdb.cli.commands.config import cmd_config
from graphdb.cli.commands.test import cmd_test
from graphdb.cli.commands.inspect import cmd_inspect
from graphdb.cli.commands.export import cmd_export
from graphdb.cli.commands.import_ import cmd_import
from graphdb.cli.commands.copy import cmd_copy
from graphdb.cli.commands.compare import cmd_compare

__all__ = [
    "cmd_config",
    "cmd_test",
    "cmd_inspect",
    "cmd_export",
    "cmd_import",
    "cmd_copy",
    "cmd_compare",
]
