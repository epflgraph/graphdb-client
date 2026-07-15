# graphdb/cli/commands/config.py
from pathlib import Path

import rich

from graphdb.application.config_service import ConfigService
from graphdb.cli.context import CLIContext


def cmd_config(args):
    print("🖥️  ~ GraphDB client CLI. Print out config options.")

    config = ConfigService.from_default_file()
    config_path = config.config.default_path()
    raw = config.load_raw(config_path)
    rich.print_json(data=ConfigService.redact(raw))

    print("🖥️  ~ Done.")
