# graphdb/cli/commands/config.py
import sys
from pathlib import Path

import rich

from graphdb.application.config_service import ConfigService
from graphdb.cli.context import CLIContext
from graphdb.domain.config import GraphDBConfigError


def cmd_config(args):
    print("🖥️  ~ GraphDB client CLI. Print out config options.")

    config_path = getattr(args, "config_path", None)
    try:
        if config_path:
            config = ConfigService.from_file(config_path)
        else:
            config = ConfigService.from_default_file()
    except (GraphDBConfigError, FileNotFoundError) as exc:
        print(f"❌ {exc}")
        if config_path:
            print(f"💡 Check that the file exists: {config_path}")
        else:
            print("💡 Set GRAPHDB_CONFIG, add it to a .env file, or pass --config PATH.")
        sys.exit(1)

    raw_path = Path(config_path) if config_path else config.config.default_path()
    raw = config.load_raw(raw_path)
    rich.print_json(data=ConfigService.redact(raw))

    print("🖥️  ~ Done.")
