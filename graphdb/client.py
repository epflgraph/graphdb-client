# graphdb-client/graphdb/client.py
"""Public downstream surface of graphdb-client.

This module is the stable entry point for services that consume
graphdb-client as a library (e.g. GraphRegistry). Everything exported here
is the supported downstream contract and follows semantic versioning;
anything not exported from this module is internal and may change or move
without notice.

Typical usage::

    from graphdb.client import Environments
    from graphdb.domain.models.mdl_config import GraphDBConfig

    environments = Environments(GraphDBConfig.from_file("config.yaml"))
    gateway = environments.get("test")

    gateway.engine                     # SQLAlchemy engine pool
    gateway.query_executor             # raw SQL execution (incl. shell paths)
    gateway.table / gateway.column     # schema introspection and DDL
    gateway.database / gateway.key / gateway.view

    ImportOperations(environments).import_database(
        "test", "my_schema", "./dump_folder", ignore_existing=True,
    )
    ExportOperations(environments).export_database(
        "test", "my_schema", "./dump_folder", compress=True,
    )

Deliberately internal (not exported): the mysql/dump CLI gateways, the
filesystem gateway, and the base_command/subprocess_env properties.
Shell-executed SQL goes through the query executor (execute_query_in_shell,
execute_query_from_file); bulk export/import goes through
ImportOperations / ExportOperations, which orchestrate those internals.

Importing this module pulls in the SQLAlchemy/PyMySQL stack; the package
root stays deliberately lightweight, which is why the re-exports live here
instead of in ``graphdb/__init__.py``.
"""
from __future__ import annotations

# Redundant aliases make the re-exports explicit for type checkers and
# readers: these names are the published contract, not incidental imports.
from graphdb.adapters.environments import EnvironmentGateway as EnvironmentGateway
from graphdb.adapters.environments import Environments as Environments
from graphdb.application.operations.ops_export import ExportOperations as ExportOperations
from graphdb.application.operations.ops_import import ImportOperations as ImportOperations
from graphdb.application.ports.engine.prt_execute import EngineExecutePort as EngineExecutePort
from graphdb.application.ports.schema.prt_column import ColumnSchemaPort as ColumnSchemaPort
from graphdb.application.ports.schema.prt_database import DatabaseSchemaPort as DatabaseSchemaPort
from graphdb.application.ports.schema.prt_key import KeySchemaPort as KeySchemaPort
from graphdb.application.ports.schema.prt_table import TableSchemaPort as TableSchemaPort
from graphdb.application.ports.schema.prt_view import ViewSchemaPort as ViewSchemaPort

# The downstream contract: the only names other services may rely on.
__all__ = [
    "ColumnSchemaPort",
    "DatabaseSchemaPort",
    "EngineExecutePort",
    "EnvironmentGateway",
    "Environments",
    "ExportOperations",
    "ImportOperations",
    "KeySchemaPort",
    "TableSchemaPort",
    "ViewSchemaPort",
]
