"""Structural conformance tests: fakes must satisfy the application-layer ports.

These tests use ``runtime_checkable`` Protocols so that ``isinstance(fake, Port)``
validates the fake exposes every member declared by the port. They exist to
prevent the fakes and ports from silently drifting apart again (the failure
mode that left the previous ports as unused ceremony).

They deliberately import only the domain models and the port Protocols, so they
run without a database, without ``rich``, and without SQLAlchemy being
configured.
"""
from __future__ import annotations

from tests.fakes import (
    FakeDatabaseAdapter,
    FakeDumpAdapter,
    FakeEnvironmentAdapter,
    FakeFilesystemAdapter,
    FakeSchemaAdapter,
)

from graphdb.application.ports.gateways.prt_dbclient import DBClientPort
from graphdb.application.ports.gateways.prt_environment import EnvironmentPort
from graphdb.application.ports.gateways.prt_filesystem import FilesystemPort
from graphdb.application.ports.gateways.prt_mysqldump import MySQLDumpBinaryPort
from graphdb.application.ports.gateways.prt_sqlalchemy import SQLAlchemyQueryExecutorPort
from graphdb.application.ports.schema.prt_column import ColumnSchemaPort
from graphdb.application.ports.schema.prt_database import DatabaseSchemaPort
from graphdb.application.ports.schema.prt_key import KeySchemaPort
from graphdb.application.ports.schema.prt_table import TableSchemaPort
from graphdb.application.ports.schema.prt_view import ViewSchemaPort


def _env() -> FakeEnvironmentAdapter:
    return FakeEnvironmentAdapter()


def test_environment_fake_conforms_to_environment_port():
    env = _env()
    assert isinstance(env, EnvironmentPort)


def test_environment_fake_exposes_conforming_sub_adapters():
    env = _env()
    # Each property of EnvironmentPort must return an object conforming to its
    # own sub-port. This is what stops the composite fake from drifting.
    assert isinstance(env.database, DatabaseSchemaPort)
    assert isinstance(env.table, TableSchemaPort)
    assert isinstance(env.view, ViewSchemaPort)
    assert isinstance(env.column, ColumnSchemaPort)
    assert isinstance(env.key, KeySchemaPort)
    assert isinstance(env.query_executor, SQLAlchemyQueryExecutorPort)
    assert isinstance(env.mysql_client, DBClientPort)
    assert isinstance(env.dump_client, MySQLDumpBinaryPort)
    assert isinstance(env.filesystem, FilesystemPort)


def test_schema_fake_conforms_to_all_schema_ports():
    schema = FakeSchemaAdapter()
    # The fake schema adapter intentionally implements the union of the split
    # schema ports; it must conform to each one individually.
    assert isinstance(schema, DatabaseSchemaPort)
    assert isinstance(schema, TableSchemaPort)
    assert isinstance(schema, ColumnSchemaPort)
    assert isinstance(schema, KeySchemaPort)
    assert isinstance(schema, ViewSchemaPort)


def test_database_fake_conforms_to_executor_and_dbclient_ports():
    db = FakeDatabaseAdapter()
    assert isinstance(db, SQLAlchemyQueryExecutorPort)
    assert isinstance(db, DBClientPort)


def test_dump_fake_conforms_to_mysqldump_port():
    assert isinstance(FakeDumpAdapter(), MySQLDumpBinaryPort)


def test_filesystem_fake_conforms_to_filesystem_port():
    assert isinstance(FakeFilesystemAdapter(), FilesystemPort)


def test_non_conforming_object_is_rejected():
    """Sanity check that the Protocol isinstance check actually fails on drift."""

    class MissingEverything:
        pass

    assert not isinstance(MissingEverything(), EnvironmentPort)
    assert not isinstance(MissingEverything(), TableSchemaPort)
