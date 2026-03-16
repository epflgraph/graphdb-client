# graphdb/cli/register.py
from graphdb.cli.specs import cli_definitions

#==============================#
# Register domain and commands #
#==============================#
def register(subparsers, cmd_name):
    f"""
    Register '{cmd_name}' domain commands.

    Usage:
      graphdb {cmd_name} [-h|...]
    """

    spec = cli_definitions[cmd_name]

    # Register Level 1 parser
    # >> graphdb cmd_name [-h|...]
    parser = subparsers.add_parser(cmd_name, help=spec['help'])

    # Nested command (e.g., graphdb config index)
    if 'commands' in spec:
        subcmd_cmd_name = parser.add_subparsers(dest=f"{cmd_name}_cmd", metavar="<command>", required=True,
            help=f"<{cmd_name}> subcommands. Use \"graphdb {cmd_name} <command> -h\" for options.")

        for name, sub_spec in spec['commands'].items():
            p = subcmd_cmd_name.add_parser(name, help=sub_spec['help'])

            for arg_name in sub_spec.get('common_args', []):
                arg = spec['common_args'][arg_name]
                p.add_argument(*arg['flags'], **arg['kwargs'])

            for arg in sub_spec.get('args', []):
                p.add_argument(*arg['flags'], **arg['kwargs'])

            p.set_defaults(
                func=sub_spec['func'],
                requires_db=sub_spec.get('requires_db', True),
            )
        return

    # Flat command (e.g., graphdb test)
    for arg_name in spec.get('common_args_order', []):
        arg = spec['common_args'][arg_name]
        parser.add_argument(*arg['flags'], **arg['kwargs'])

    for arg in spec.get('args', []):
        parser.add_argument(*arg['flags'], **arg['kwargs'])

    parser.set_defaults(
        func=spec['func'],
        requires_db=spec.get('requires_db', True),
    )
