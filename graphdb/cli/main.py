# graphdb/cli/main.py
# Main entry point for the GraphDB CLI application.
import argparse

from graphdb.application.adapter_registry import AdapterRegistry
from graphdb.application.config_service import ConfigService
from graphdb.cli.context import CLIContext
from graphdb.cli.register import register


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="graphdb",
        description="GraphDB command-line interface (CLI) for managing MySQL server actions.")

    subparsers = parser.add_subparsers(dest="domain", required=True)

    for cmd_name in ['config', 'test', 'inspect', 'export', 'import', 'copy', 'compare']:
        register(subparsers, cmd_name)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    if getattr(args, "requires_db", True):
        config = ConfigService.from_default_file().config
        registry = AdapterRegistry(config)
        args.ctx = CLIContext(registry=registry)

    return args.func(args) or 0
