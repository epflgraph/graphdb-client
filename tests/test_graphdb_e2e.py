from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from graphdb.core.graphdb import GraphDB


SCHEMA = "_pytests_"
E2E_ENGINE = os.getenv("GRAPHDB_E2E_ENV", "pytests")


def _drop_all_objects(db: GraphDB, engine_name: str, schema: str) -> None:
    for view_name in db.get_views_in_schema(engine_name=engine_name, schema_name=schema):
        db.drop_view(engine_name=engine_name, schema_name=schema, view_name=view_name)
    for table_name in db.get_tables_in_schema(engine_name=engine_name, schema_name=schema):
        db.drop_table(engine_name=engine_name, schema_name=schema, table_name=table_name)


@pytest.fixture(scope="session")
def graphdb_env() -> tuple[GraphDB, str]:
    db = GraphDB()
    if E2E_ENGINE not in db.engine:
        configured = ", ".join(sorted(db.engine.keys()))
        pytest.skip(
            f"E2E environment '{E2E_ENGINE}' is missing in config. "
            f"Configured environments: [{configured}]. "
            f"Set GRAPHDB_E2E_ENV to a valid environment to run E2E tests."
        )
    if not db.test(engine_name=E2E_ENGINE):
        pytest.skip(
            f"E2E environment '{E2E_ENGINE}' is configured but not reachable."
        )
    return db, E2E_ENGINE


@pytest.fixture(scope="session", autouse=True)
def pytest_schema(graphdb_env):
    db, engine_name = graphdb_env
    db.drop_database(engine_name=engine_name, schema_name=SCHEMA)
    db.create_database(engine_name=engine_name, schema_name=SCHEMA)
    yield
    db.drop_database(engine_name=engine_name, schema_name=SCHEMA)


@pytest.fixture(autouse=True)
def clean_schema(graphdb_env, pytest_schema):
    db, engine_name = graphdb_env
    db.create_database(engine_name=engine_name, schema_name=SCHEMA)
    _drop_all_objects(db, engine_name, SCHEMA)
    yield
    _drop_all_objects(db, engine_name, SCHEMA)


@pytest.fixture(scope="session")
def mysql_cli_available(graphdb_env):
    db, engine_name = graphdb_env
    mysql_bin = db.base_command_mysql[engine_name][0]
    dump_bin = db.base_command_mysqldump[engine_name][0]
    if shutil.which(mysql_bin) is None:
        pytest.skip(f"mysql client not found in PATH: {mysql_bin}")
    if shutil.which(dump_bin) is None:
        pytest.skip(f"mysqldump client not found in PATH: {dump_bin}")


def _create_source_table(db: GraphDB, engine_name: str) -> None:
    db.execute_query(
        engine_name=engine_name,
        query=f"""
        CREATE TABLE {SCHEMA}.source (
            row_id INT NOT NULL AUTO_INCREMENT,
            code VARCHAR(32) NOT NULL,
            value INT NULL,
            record_updated_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (row_id),
            UNIQUE KEY uid_code (code),
            KEY idx_value (value)
        ) ENGINE=InnoDB
        """,
    )
    db.execute_query(
        engine_name=engine_name,
        query=f"""
        INSERT INTO {SCHEMA}.source (code, value)
        VALUES ('A', 10), ('B', 20), ('C', 30)
        """,
        commit=True,
    )


def test_e2e_core_schema_and_query_methods(graphdb_env, mysql_cli_available, tmp_path, capsys):
    db, engine_name = graphdb_env

    assert db.test(engine_name=engine_name)
    assert not db.database_exists(engine_name=engine_name, schema_name=SCHEMA + "_tmp")

    db.create_database(engine_name=engine_name, schema_name=SCHEMA + "_tmp")
    assert db.database_exists(engine_name=engine_name, schema_name=SCHEMA + "_tmp")
    db.drop_database(engine_name=engine_name, schema_name=SCHEMA + "_tmp")
    assert not db.database_exists(engine_name=engine_name, schema_name=SCHEMA + "_tmp")

    _create_source_table(db, engine_name)

    assert db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="source")
    assert db.column_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="source", column_name="code")
    assert db.has_column(engine_name=engine_name, schema_name=SCHEMA, table_name="source", column_name="value")
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="source") == 3
    assert db.get_table_size(engine_name=engine_name, schema_name=SCHEMA, table_name="source") == 3

    create_table_sql = db.get_create_table(engine_name=engine_name, schema_name=SCHEMA, table_name="source")
    assert "CREATE TABLE" in create_table_sql.upper()
    assert "source" in create_table_sql

    cols = db.get_column_names(engine_name=engine_name, schema_name=SCHEMA, table_name="source")
    assert cols[:3] == ["row_id", "code", "value"]

    dtypes = db.get_column_datatypes(engine_name=engine_name, schema_name=SCHEMA, table_name="source")
    assert "value" in dtypes
    assert db.has_primary_key(engine_name=engine_name, schema_name=SCHEMA, table_name="source")
    assert "row_id" in db.get_primary_keys(engine_name=engine_name, schema_name=SCHEMA, table_name="source")

    keys = db.get_keys(engine_name=engine_name, schema_name=SCHEMA, table_name="source")
    assert "PRIMARY" in keys
    assert db.key_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="source", key_name="uid_code")

    rows = db.execute_query(engine_name=engine_name, query=f"SELECT code FROM {SCHEMA}.source ORDER BY row_id")
    assert [r[0] for r in rows] == ["A", "B", "C"]

    db.execute_query_in_shell(engine_name=engine_name, query=f"INSERT INTO {SCHEMA}.source (code, value) VALUES ('D', 40)")
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="source") == 4

    stream_file = tmp_path / "stream.jsonl"
    db.execute_query_stream_to_file(
        engine_name=engine_name,
        query=f"SELECT code, value FROM {SCHEMA}.source ORDER BY row_id",
        output_file=str(stream_file),
        fetch_size=2,
    )
    stream_lines = [json.loads(line) for line in stream_file.read_text(encoding="utf-8").splitlines()]
    assert stream_lines[0]["code"] == "A"
    assert stream_lines[-1]["code"] == "D"

    sql_file = tmp_path / "extra.sql"
    sql_file.write_text(f"INSERT INTO {SCHEMA}.source (code, value) VALUES ('E', 50);", encoding="utf-8")
    result = db.execute_query_from_file(engine_name=engine_name, file_path=str(sql_file), database=SCHEMA)
    assert result.returncode == 0
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="source") == 5

    db.execute_query(
        engine_name=engine_name,
        query=f"CREATE TABLE {SCHEMA}.safe_target (code VARCHAR(32) NOT NULL, value INT NULL, PRIMARY KEY (code))",
    )
    db.execute_query_as_safe_inserts(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="safe_target",
        query=f"SELECT code, value FROM {SCHEMA}.source WHERE code IN ('A', 'B', 'C')",
        key_column_names=["code"],
        upd_column_names=["value"],
        actions=("commit",),
    )
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="safe_target") == 3

    db.execute_query(
        engine_name=engine_name,
        query=f"CREATE TABLE {SCHEMA}.chunk_source (row_id INT PRIMARY KEY, code VARCHAR(32) NOT NULL, value INT NULL)",
    )
    db.execute_query(
        engine_name=engine_name,
        query=f"INSERT INTO {SCHEMA}.chunk_source VALUES (1,'A',101),(2,'B',202),(3,'C',303)",
        commit=True,
    )
    db.execute_query(
        engine_name=engine_name,
        query=f"CREATE TABLE {SCHEMA}.safe_target_chunk (code VARCHAR(32) NOT NULL, value INT NULL, PRIMARY KEY (code))",
    )
    db.execute_query_as_safe_inserts_in_chunks(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="safe_target_chunk",
        query=f"SELECT row_id, code, value FROM {SCHEMA}.chunk_source",
        key_column_names=["code"],
        upd_column_names=["value"],
        actions=("commit",),
        table_to_chunk=f"{SCHEMA}.chunk_source",
        chunk_size=2,
        row_id_name="row_id",
    )
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="safe_target_chunk") == 3

    db.execute_query(
        engine_name=engine_name,
        query=f"CREATE TABLE {SCHEMA}.chunk_dest (row_id INT PRIMARY KEY, code VARCHAR(32), value INT)",
    )
    db.execute_query_in_chunks(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="chunk_source",
        query=f"INSERT INTO {SCHEMA}.chunk_dest (row_id, code, value) SELECT row_id, code, value FROM {SCHEMA}.chunk_source",
        chunk_size=2,
        row_id_name="row_id",
    )
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="chunk_dest") == 3

    db.execute_query(
        engine_name=engine_name,
        query=f"""
        CREATE TABLE {SCHEMA}.upsert_t (
            code VARCHAR(32) NOT NULL,
            value INT NULL,
            record_updated_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (code)
        )
        """,
    )
    db.execute_upsert_row(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="upsert_t",
        key_column_names=["code"],
        key_column_values=["A"],
        upd_column_names=["value"],
        upd_column_values=[111],
        actions=("commit",),
    )
    db.execute_upsert_row(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="upsert_t",
        key_column_names=["code"],
        key_column_values=["A"],
        upd_column_names=["value"],
        upd_column_values=[222],
        actions=("commit",),
    )
    assert db.get_cells(engine_name=engine_name, schema_name=SCHEMA, table_name="upsert_t", select=("value",), where=(("code", "A"),))[0][0] == 222

    db.set_cells(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="upsert_t",
        set=(("value", "333"),),
        where=(("code", "A"),),
    )
    assert db.get_cells(engine_name=engine_name, schema_name=SCHEMA, table_name="upsert_t", select=("value",), where=(("code", "A"),))[0][0] == 333

    db.execute_query(
        engine_name=engine_name,
        query=f"CREATE TABLE {SCHEMA}.key_ops (row_id INT NOT NULL, code VARCHAR(32), value INT)",
    )
    db.apply_datatypes(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="key_ops",
        datatypes_json={"row_id": "INT NOT NULL", "value": "BIGINT"},
    )
    db.apply_keys(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="key_ops",
        keys_json={"row_id": "PRIMARY KEY", "code": "KEY"},
    )
    assert db.has_primary_key(engine_name=engine_name, schema_name=SCHEMA, table_name="key_ops")
    assert db.key_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="key_ops", key_name="code")

    db.drop_keys(engine_name=engine_name, schema_name=SCHEMA, table_name="key_ops", ignore_keys=[])
    assert not db.has_primary_key(engine_name=engine_name, schema_name=SCHEMA, table_name="key_ops")

    db.print_schemas(engine_name=engine_name)
    db.print_tables_in_schema(engine_name=engine_name, schema_name=SCHEMA)
    object.__setattr__(db.config, "schema_cache", SCHEMA)
    object.__setattr__(db.config, "schema_test", SCHEMA)
    db.print_tables_in_cache()
    db.print_tables_in_test()
    printed = capsys.readouterr().out
    assert SCHEMA in printed


def test_e2e_views_and_table_copy_methods(graphdb_env):
    db, engine_name = graphdb_env
    _create_source_table(db, engine_name)

    db.create_view(
        engine_name=engine_name,
        schema_name=SCHEMA,
        view_name="v_source",
        query=f"SELECT code, value FROM {SCHEMA}.source",
    )
    assert db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="v_source")
    assert db.is_view(engine_name=engine_name, schema_name=SCHEMA, name="v_source")
    assert "CREATE" in db.get_create_view(engine_name=engine_name, schema_name=SCHEMA, view_name="v_source").upper()

    db.copy_view_definition(
        engine_name=engine_name,
        source_schema_name=SCHEMA,
        source_view_name="v_source",
        target_schema_name=SCHEMA,
        target_view_name="v_source_copy",
        drop_view=True,
    )
    assert db.is_view(engine_name=engine_name, schema_name=SCHEMA, name="v_source_copy")

    tables = db.get_tables_in_schema(engine_name=engine_name, schema_name=SCHEMA)
    views = db.get_views_in_schema(engine_name=engine_name, schema_name=SCHEMA)
    assert "source" in tables
    assert "v_source" in views

    db.create_table_like(
        engine_name=engine_name,
        source_schema_name=SCHEMA,
        source_table_name="source",
        target_schema_name=SCHEMA,
        target_table_name="like_source",
        drop_table=True,
    )
    assert db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="like_source")

    db.copy_create_table(
        source_engine_name=engine_name,
        source_schema_name=SCHEMA,
        source_table_name="source",
        target_engine_name=engine_name,
        target_schema_name=SCHEMA,
        target_table_name="source",
        ignore_if_exists=True,
    )
    assert db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="source")

    db.execute_query(
        engine_name=engine_name,
        query=f"INSERT INTO {SCHEMA}.like_source SELECT * FROM {SCHEMA}.source",
        commit=True,
    )
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="like_source") == 3

    db.rename_table(
        engine_name=engine_name,
        schema_name=SCHEMA,
        table_name="like_source",
        rename_to="like_source_renamed",
        replace_existing=False,
    )
    assert db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="like_source_renamed")

    db.materialise_view(
        source_schema=SCHEMA,
        source_view="v_source",
        target_schema=SCHEMA,
        target_table="mat_from_view",
        drop_table=True,
        engine_name=engine_name,
    )
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="mat_from_view") == 3

    db.execute_query(
        engine_name=engine_name,
        query=f"CREATE TABLE {SCHEMA}.upd_from_view (code VARCHAR(32) PRIMARY KEY, value INT)",
    )
    db.update_table_from_view(
        engine_name=engine_name,
        source_schema=SCHEMA,
        source_view="v_source",
        target_schema=SCHEMA,
        target_table="upd_from_view",
    )
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="upd_from_view") == 3

    db.drop_view(engine_name=engine_name, schema_name=SCHEMA, view_name="v_source_copy")
    assert not db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="v_source_copy")

def test_e2e_export_import_methods(graphdb_env, mysql_cli_available, tmp_path):
    db, engine_name = graphdb_env

    db.execute_query(
        engine_name=engine_name,
        query=f"""
        CREATE TABLE {SCHEMA}.export_me (
            row_id INT NOT NULL AUTO_INCREMENT,
            name VARCHAR(32) NOT NULL,
            value INT NULL,
            PRIMARY KEY (row_id),
            KEY idx_name (name)
        ) ENGINE=InnoDB
        """,
    )
    db.execute_query(
        engine_name=engine_name,
        query=f"INSERT INTO {SCHEMA}.export_me (name, value) VALUES ('x',1),('y',2),('z',3)",
        commit=True,
    )

    out1 = tmp_path / "single"
    db.export_create_table(engine_name=engine_name, schema_name=SCHEMA, table_name="export_me", output_folder=str(out1))
    db.export_table_data(engine_name=engine_name, schema_name=SCHEMA, table_name="export_me", output_folder=str(out1), chunk_size=2)
    db.export_table(engine_name=engine_name, schema_name=SCHEMA, table_name="export_me", output_folder=str(out1), chunk_size=2, include_create_tables=True)

    table_folder = out1 / SCHEMA / "export_me"
    assert (table_folder / "CREATE_TABLE.sql").exists()
    assert (table_folder / "CREATE_TABLE_NO_KEYS.sql").exists()
    assert (table_folder / "CREATE_KEYS.sql").exists()
    assert any(p.name.endswith(".sql") and p.name not in {"CREATE_TABLE.sql", "CREATE_TABLE_NO_KEYS.sql", "CREATE_KEYS.sql"} for p in table_folder.glob("*.sql"))

    manual_folder = tmp_path / "manual_table"
    manual_folder.mkdir(parents=True, exist_ok=True)
    (manual_folder / "CREATE_TABLE.sql").write_text(
        """
        CREATE TABLE `manual_table` (
          `row_id` int NOT NULL,
          `name` varchar(32) NOT NULL,
          PRIMARY KEY (`row_id`),
          KEY `idx_name` (`name`)
        ) ENGINE=InnoDB;
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    (manual_folder / "CREATE_TABLE_NO_KEYS.sql").write_text(
        """
        CREATE TABLE `manual_table` (
          `row_id` int NOT NULL,
          `name` varchar(32) NOT NULL,
          PRIMARY KEY (`row_id`)
        ) ENGINE=InnoDB;
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    (manual_folder / "CREATE_KEYS.sql").write_text(
        "ALTER TABLE `manual_table` ADD KEY `idx_name` (`name`);\n",
        encoding="utf-8",
    )
    (manual_folder / "manual_rows.sql").write_text(
        "INSERT INTO `manual_table` (`row_id`, `name`) VALUES (1, 'a'), (2, 'b'), (3, 'c');\n",
        encoding="utf-8",
    )

    db.import_create_table(engine_name=engine_name, schema_name=SCHEMA, input_folder=str(manual_folder), include_keys=False)
    db.import_table_data(engine_name=engine_name, schema_name=SCHEMA, input_folder=str(manual_folder))
    db.import_table_keys(engine_name=engine_name, schema_name=SCHEMA, input_folder=str(manual_folder))
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="manual_table") == 3

    db.drop_table(engine_name=engine_name, schema_name=SCHEMA, table_name="manual_table")
    db.import_table(
        engine_name=engine_name,
        schema_name=SCHEMA,
        input_folder=str(manual_folder),
        create_keys_after_import=True,
        ignore_existing=False,
    )
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="manual_table") == 3

    db.execute_query(
        engine_name=engine_name,
        query=f"CREATE TABLE {SCHEMA}.export_me2 (row_id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(32), value INT)",
    )
    db.execute_query(
        engine_name=engine_name,
        query=f"INSERT INTO {SCHEMA}.export_me2 (name, value) VALUES ('m',10),('n',20)",
        commit=True,
    )

    out2 = tmp_path / "db"
    db.export_create_tables_in_database(engine_name=engine_name, schema_name=SCHEMA, output_folder=str(out2))
    db.export_table_data_in_database(engine_name=engine_name, schema_name=SCHEMA, output_folder=str(out2), chunk_size=2)
    db.export_database(engine_name=engine_name, schema_name=SCHEMA, output_folder=str(out2), include_create_tables=True, chunk_size=2)

    manual_db_root = tmp_path / "manual_db"
    (manual_db_root / "db_table_a").mkdir(parents=True, exist_ok=True)
    (manual_db_root / "db_table_b").mkdir(parents=True, exist_ok=True)
    (manual_db_root / "db_table_a" / "CREATE_TABLE.sql").write_text(
        "CREATE TABLE `db_table_a` (`id` int NOT NULL, `name` varchar(32) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB;\n",
        encoding="utf-8",
    )
    (manual_db_root / "db_table_a" / "CREATE_TABLE_NO_KEYS.sql").write_text(
        "CREATE TABLE `db_table_a` (`id` int NOT NULL, `name` varchar(32) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB;\n",
        encoding="utf-8",
    )
    (manual_db_root / "db_table_a" / "CREATE_KEYS.sql").write_text("", encoding="utf-8")
    (manual_db_root / "db_table_a" / "rows.sql").write_text(
        "INSERT INTO `db_table_a` (`id`, `name`) VALUES (1, 'x'), (2, 'y');\n",
        encoding="utf-8",
    )
    (manual_db_root / "db_table_b" / "CREATE_TABLE.sql").write_text(
        "CREATE TABLE `db_table_b` (`id` int NOT NULL, `value` int NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB;\n",
        encoding="utf-8",
    )
    (manual_db_root / "db_table_b" / "CREATE_TABLE_NO_KEYS.sql").write_text(
        "CREATE TABLE `db_table_b` (`id` int NOT NULL, `value` int NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB;\n",
        encoding="utf-8",
    )
    (manual_db_root / "db_table_b" / "CREATE_KEYS.sql").write_text("", encoding="utf-8")
    (manual_db_root / "db_table_b" / "rows.sql").write_text(
        "INSERT INTO `db_table_b` (`id`, `value`) VALUES (1, 10), (2, 20);\n",
        encoding="utf-8",
    )

    db.drop_database(engine_name=engine_name, schema_name=SCHEMA)
    assert not db.database_exists(engine_name=engine_name, schema_name=SCHEMA)

    db.import_database(
        engine_name=engine_name,
        schema_name=SCHEMA,
        input_folder=str(manual_db_root),
        create_keys_after_import=False,
        ignore_existing=False,
    )

    assert db.database_exists(engine_name=engine_name, schema_name=SCHEMA)
    assert db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="db_table_a")
    assert db.table_exists(engine_name=engine_name, schema_name=SCHEMA, table_name="db_table_b")
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="db_table_a") == 2
    assert db.count_rows_in_table(engine_name=engine_name, schema_name=SCHEMA, table_name="db_table_b") == 2
