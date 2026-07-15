"""Safe end-to-end tests for the legacy GraphDB façade against matrix containers."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB


def _clear_config_cache() -> None:
    from graphdb.domain.config import _resolve_dotenv_override
    _resolve_dotenv_override.cache_clear()


@pytest.fixture(scope="module")
def graphdb(deployment, graphdb_config_path: Path) -> GraphDB:
    """GraphDB instance wired to a matrix container (no external DB required)."""
    _clear_config_cache()
    os.environ["GRAPHDB_CONFIG"] = str(graphdb_config_path)
    return GraphDB()


@pytest.fixture(scope="module")
def engine_name(graphdb: GraphDB) -> str:
    return graphdb.default_engine_name


@pytest.fixture
def scratch_schema(graphdb: GraphDB, engine_name: str):
    """Create a temporary schema for write tests and drop it afterwards."""
    schema = "_e2e_scratch"
    graphdb.drop_database(engine_name=engine_name, schema_name=schema)
    graphdb.create_database(engine_name=engine_name, schema_name=schema)
    graphdb.execute_query(
        engine_name=engine_name,
        query=f"""
            CREATE TABLE {schema}.test_table (
                row_id INT NOT NULL AUTO_INCREMENT,
                code VARCHAR(32) NOT NULL,
                value INT NULL,
                record_updated_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (row_id),
                UNIQUE KEY uid_code (code)
            ) ENGINE=InnoDB
        """,
        commit=True,
    )
    yield schema
    graphdb.drop_database(engine_name=engine_name, schema_name=schema)


def test_connectivity(graphdb: GraphDB, engine_name: str) -> None:
    assert graphdb.test(engine_name=engine_name) is True


def test_database_exists(graphdb: GraphDB, engine_name: str) -> None:
    assert graphdb.database_exists(engine_name=engine_name, schema_name="source") is True
    assert graphdb.database_exists(engine_name=engine_name, schema_name="_nonexistent") is False


def test_table_and_column_introspection(graphdb: GraphDB, engine_name: str) -> None:
    assert graphdb.table_exists(engine_name=engine_name, schema_name="source", table_name="users") is True
    assert graphdb.column_exists(engine_name=engine_name, schema_name="source", table_name="users", column_name="email") is True
    assert graphdb.column_exists(engine_name=engine_name, schema_name="source", table_name="users", column_name="nope") is False


def test_key_introspection(graphdb: GraphDB, engine_name: str) -> None:
    assert graphdb.key_exists(engine_name=engine_name, schema_name="source", table_name="users", key_name="PRIMARY") is True
    assert graphdb.key_exists(engine_name=engine_name, schema_name="source", table_name="users", key_name="uid_user_id") is True
    assert graphdb.key_exists(engine_name=engine_name, schema_name="source", table_name="users", key_name="missing") is False


def test_get_tables_and_views(graphdb: GraphDB, engine_name: str) -> None:
    tables = graphdb.get_tables_in_schema(engine_name=engine_name, schema_name="source")
    assert "users" in tables
    assert "posts" in tables

    views = graphdb.get_views_in_schema(engine_name=engine_name, schema_name="source")
    assert "active_users" in views


def test_get_create_table(graphdb: GraphDB, engine_name: str) -> None:
    ddl = graphdb.get_create_table(engine_name=engine_name, schema_name="source", table_name="users")
    assert "CREATE TABLE" in ddl
    assert "users" in ddl


def test_execute_query(graphdb: GraphDB, engine_name: str) -> None:
    rows = graphdb.execute_query(
        engine_name=engine_name,
        query="SELECT COUNT(*) FROM source.users",
    )
    assert rows[0][0] == 4


def test_execute_query_in_shell(graphdb: GraphDB, engine_name: str) -> None:
    graphdb.execute_query_in_shell(
        engine_name=engine_name,
        query="SELECT 1 FROM source.users LIMIT 1",
    )


def test_count_rows_in_table(graphdb: GraphDB, engine_name: str) -> None:
    assert graphdb.count_rows_in_table(engine_name=engine_name, schema_name="source", table_name="users") == 4


def test_get_column_names_and_datatypes(graphdb: GraphDB, engine_name: str) -> None:
    columns = graphdb.get_column_names(engine_name=engine_name, schema_name="source", table_name="users")
    assert "row_id" in columns
    assert "email" in columns

    datatypes = graphdb.get_column_datatypes(engine_name=engine_name, schema_name="source", table_name="users")
    assert any("int" in dt.lower() for dt in datatypes.values())


def test_get_primary_keys_and_keys(graphdb: GraphDB, engine_name: str) -> None:
    pks = graphdb.get_primary_keys(engine_name=engine_name, schema_name="source", table_name="users")
    assert pks == ["row_id"]

    keys = graphdb.get_keys(engine_name=engine_name, schema_name="source", table_name="users")
    assert "PRIMARY" in keys
    assert "uid_user_id" in keys
    assert keys["PRIMARY"] == ["row_id"]


def test_create_and_drop_table(graphdb: GraphDB, engine_name: str, scratch_schema: str) -> None:
    assert graphdb.table_exists(engine_name=engine_name, schema_name=scratch_schema, table_name="test_table") is True
    graphdb.drop_table(engine_name=engine_name, schema_name=scratch_schema, table_name="test_table")
    assert graphdb.table_exists(engine_name=engine_name, schema_name=scratch_schema, table_name="test_table") is False


def test_execute_upsert_row(graphdb: GraphDB, engine_name: str, scratch_schema: str) -> None:
    graphdb.execute_upsert_row(
        engine_name=engine_name,
        schema_name=scratch_schema,
        table_name="test_table",
        key_column_names=["code"],
        key_column_values=["A"],
        upd_column_names=["value"],
        upd_column_values=[100],
        actions=("commit",),
    )
    rows = graphdb.execute_query(
        engine_name=engine_name,
        query=f"SELECT value FROM {scratch_schema}.test_table WHERE code = 'A'",
    )
    assert rows[0][0] == 100

    # Update existing row
    graphdb.execute_upsert_row(
        engine_name=engine_name,
        schema_name=scratch_schema,
        table_name="test_table",
        key_column_names=["code"],
        key_column_values=["A"],
        upd_column_names=["value"],
        upd_column_values=[200],
        actions=("commit",),
    )
    rows = graphdb.execute_query(
        engine_name=engine_name,
        query=f"SELECT value FROM {scratch_schema}.test_table WHERE code = 'A'",
    )
    assert rows[0][0] == 200


def test_copy_create_table(graphdb: GraphDB, engine_name: str, scratch_schema: str) -> None:
    graphdb.copy_create_table(
        source_engine_name=engine_name,
        source_schema_name="source",
        source_table_name="users",
        target_engine_name=engine_name,
        target_schema_name=scratch_schema,
        target_table_name="users_copy",
    )
    assert graphdb.table_exists(engine_name=engine_name, schema_name=scratch_schema, table_name="users_copy") is True
    graphdb.drop_table(engine_name=engine_name, schema_name=scratch_schema, table_name="users_copy")
