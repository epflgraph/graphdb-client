import argparse
import unittest
from pathlib import Path
from unittest.mock import patch

from graphdb.application.operations.ops_config import ConfigOperations
from graphdb.domain.models.mdl_config import GraphDBConfig
from graphdb.entrypoints.cli.cmd_config import cmd_config
from graphdb.entrypoints.cli.cmd_compare import cmd_compare, _print_table_result
from graphdb.entrypoints.cli.cmd_copy import cmd_copy
from graphdb.entrypoints.cli.cmd_export import cmd_export
from graphdb.entrypoints.cli.cmd_import import cmd_import
from graphdb.entrypoints.cli.cmd_test import cmd_test
from graphdb.entrypoints.cli.cli_context import CLIContext
from graphdb.entrypoints.cli.container import Container
from tests.fakes import (
    FakeEnvironments,
    FakeDatabaseAdapter,
    FakeDumpAdapter,
    FakeEnvironmentAdapter,
    FakeFilesystemAdapter,
    FakeSchemaAdapter,
)


def _make_args(**kwargs):
    return argparse.Namespace(**kwargs)


def _make_container(registry=None, config=None):
    return Container(config, environments=registry)


class TestCliConfigCommand(unittest.TestCase):
    @patch("graphdb.domain.models.mdl_config.GraphDBConfig.default_path")
    @patch("graphdb.entrypoints.cli.cmd_config.GraphDBConfig.from_default_file")
    @patch("rich.print_json")
    def test_config_command_redacts_passwords(self, mock_print_json, mock_from_default, mock_default_path):
        from graphdb.domain.models.mdl_config import GraphDBConfig
        import tempfile, os
        cfg = GraphDBConfig.from_dict({
            "client_bin": "mysql",
            "dump_bin": "mysqldump",
            "default_env": "test",
            "environments": {
                "test": {"host_address": "127.0.0.1", "port": 3306, "username": "u", "password": "secret"},
            },
        })
        service = ConfigOperations(cfg)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
            fh.write(
                "client_bin: mysql\n"
                "dump_bin: mysqldump\n"
                "default_env: test\n"
                "environments:\n"
                "  test:\n"
                "    host_address: 127.0.0.1\n"
                "    port: 3306\n"
                "    username: u\n"
                "    password: secret\n"
            )
            path = Path(fh.name)
        try:
            mock_default_path.return_value = path
            mock_from_default.return_value = cfg
            args = _make_args(ctx=CLIContext(container=_make_container(config=cfg)))
            cmd_config(args)
            data = mock_print_json.call_args.kwargs["data"]
            self.assertEqual(data["environments"]["test"]["password"], "***REDACTED***")
        finally:
            os.unlink(path)


class TestCliTestCommand(unittest.TestCase):
    def test_test_command_reports_connectivity(self):
        registry = FakeEnvironments({
            "local": FakeEnvironmentAdapter(database=FakeDatabaseAdapter()),
        })
        args = _make_args(ctx=CLIContext(container=_make_container(registry=registry)), env="local")
        with patch("builtins.print") as mock_print:
            cmd_test(args)
            output = " ".join(str(call.args[0]) for call in mock_print.call_args_list)
            self.assertIn("✅", output)


class TestCliExportCommand(unittest.TestCase):
    def test_export_command_calls_dump_for_each_table(self):
        dump = FakeDumpAdapter()
        schema = FakeSchemaAdapter(tables={"mydb": ["users"]})
        registry = FakeEnvironments({
            "local": FakeEnvironmentAdapter(schema=schema, dump=dump, filesystem=FakeFilesystemAdapter()),
        })
        args = _make_args(
            ctx=CLIContext(container=_make_container(registry=registry)),
            env="local",
            schema_name="mydb",
            output_folder="/tmp/out",
            table_name=None,
            include_create_tables=False,
            include_data=True,
            filter_by="TRUE",
            chunk_size=10000,
            compress=False,
        )
        cmd_export(args)
        self.assertEqual(len(dump.dumps), 1)


class TestCliImportCommand(unittest.TestCase):
    def test_import_command_executes_create_table_file(self):
        fs = FakeFilesystemAdapter()
        fs.files[Path("/tmp/in/users/CREATE_TABLE.sql")] = "CREATE TABLE users (id INT);"
        schema = FakeSchemaAdapter(databases=set())
        db = FakeDatabaseAdapter()
        registry = FakeEnvironments({
            "local": FakeEnvironmentAdapter(database=db, schema=schema, filesystem=fs),
        })
        args = _make_args(
            ctx=CLIContext(container=_make_container(registry=registry)),
            env="local",
            schema_name="mydb",
            input_folder="/tmp/in",
            table_name="users",
            include_create_tables=True,
            include_data=False,
            ignore_existing=False,
            verbose=False,
            compress=False,
        )
        cmd_import(args)
        self.assertIn("/tmp/in/users/CREATE_TABLE_NO_KEYS.sql", db.files_executed)


class TestCliCopyCommand(unittest.TestCase):
    def test_copy_command_dumps_and_imports_table(self):
        source_dump = FakeDumpAdapter()
        target_db = FakeDatabaseAdapter()
        source_schema = FakeSchemaAdapter(tables={"src": ["users"]})
        target_schema = FakeSchemaAdapter(databases=set())
        fs = FakeFilesystemAdapter()
        registry = FakeEnvironments({
            "src": FakeEnvironmentAdapter(schema=source_schema, dump=source_dump, filesystem=fs),
            "dst": FakeEnvironmentAdapter(database=target_db, schema=target_schema, filesystem=fs),
        })
        args = _make_args(
            ctx=CLIContext(container=_make_container(registry=registry)),
            from_env="src",
            from_schema="src",
            to_env="dst",
            to_schema="dst",
            table_name="users",
            chunk_size=10000,
            compress=False,
        )
        cmd_copy(args)
        self.assertEqual(len(source_dump.dumps), 1)


class TestCliCompareCommand(unittest.TestCase):
    def test_compare_command_prints_table_result(self):
        db = FakeDatabaseAdapter(responses={
            ("SELECT COUNT(*) FROM `src`.`users`", "src"): [[100]],
            ("SELECT COUNT(*) FROM `dst`.`users`", "dst"): [[100]],
        })
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": ["users"]})
        registry = FakeEnvironments({
            "src": FakeEnvironmentAdapter(database=db, schema=schema),
            "dst": FakeEnvironmentAdapter(database=db, schema=schema),
        })
        args = _make_args(
            ctx=CLIContext(container=_make_container(registry=registry)),
            from_env="src",
            from_schema="src",
            to_env="dst",
            to_schema="dst",
            table_name="users",
            row_count_tolerance=0.10,
            ignore_warnings=False,
            random_sampling=False,
            sample_size=1024,
        )
        with patch("builtins.print"):
            cmd_compare(args)


if __name__ == "__main__":
    unittest.main()
