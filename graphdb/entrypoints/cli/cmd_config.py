# graphdb/entrypoints/cli/cmd_config.py
import sys
from pathlib import Path

import rich
from yaml import safe_load

from graphdb.application.operations.ops_config import ConfigOperations
from graphdb.domain.mdl_config import GraphDBConfig, GraphDBConfigError


def cmd_config(args):
    print("🖥️  ~ GraphDB client CLI. Print out config options.")

    config_path = getattr(args, "config_path", None)
    try:
        if config_path:
            config = GraphDBConfig.from_file(config_path)
        else:
            config = GraphDBConfig.from_default_file()
    except (GraphDBConfigError, FileNotFoundError) as exc:
        print(f"❌ {exc}")
        if config_path:
            print(f"💡 Check that the file exists: {config_path}")
        else:
            print("💡 Set GRAPHDB_CONFIG, add it to a .env file, or pass --config PATH.")
        sys.exit(1)

    raw_path = Path(config_path) if config_path else config.default_path()
    with raw_path.open("r", encoding="utf-8") as f:
        raw = safe_load(f) or {}

    operations = ConfigOperations(config)
    rich.print_json(data=operations.redact_data(raw))

    print("🖥️  ~ Done.")
