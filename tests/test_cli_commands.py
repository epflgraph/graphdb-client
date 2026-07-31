import argparse
import unittest
from pathlib import Path
from unittest.mock import patch

from graphdb.application.config_service import ConfigService
from graphdb.cli.commands.config import cmd_config
from graphdb.cli.commands.compare import cmd_compare, _print_table_result
from graphdb.cli.commands.copy import cmd_copy
from graphdb.cli.commands.export import cmd_export
from graphdb.cli.commands.import_ import cmd_import
from graphdb.cli.commands.test import cmd_test
from graphdb.domain.config import GraphDBConfig
from tests.fakes import (
    FakeAdapterRegistry,
    FakeDatabaseAdapter,
    FakeDumpAdapter,
    FakeEnvironmentAdapter,
    FakeFilesystemAdapter,
    FakeSchemaAdapter,
)


def _make_args(**kwargs):
    return argparse.Namespace(**kwargs)


class TestCliConfigCommand(unittest.TestCase):
    @patch("graphdb.domain.config.GraphDBConfig.default_path")
    @patch("graphdb.cli.commands.config.ConfigService.from_default_file")
    @patch("rich.print_json")
    def test_config_command_redacts_passwords(self, mock_print_json, mock_from_default, mock_default_path):
        from graphdb.domain.config import GraphDBConfig
        import tempfile, os
        cfg = GraphDBConfig.from_dict({
            "client_bin": "mysql",
            "dump_bin": "mysqldump",
            "default_env": "test",
            "environments": {
                "test": {"host_address": "127.0.0.1", "port": 3306, "username": "u", "password": "secret"},
            },
        })
        service = ConfigService(cfg)
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
            mock_from_default.return_value = service
            args = _make_args(ctx=None)
            cmd_config(args)
            data = mock_print_json.call_args.kwargs["data"]
            self.assertEqual(data["environments"]["test"]["password"], "***REDACTED***")
        finally:
            os.unlink(path)


class TestCliTestCommand(unittest.TestCase):
    def test_test_command_reports_connectivity(self):
        registry = FakeAdapterRegistry({
            "local": FakeEnvironmentAdapter(database=FakeDatabaseAdapter()),
        })
        args = _make_args(ctx=_make_args(registry=registry), env="local")
        with patch("builtins.print") as mock_print:
            cmd_test(args)
            output = " ".join(str(call.args[0]) for call in mock_print.call_args_list)
            self.assertIn("✅", output)


class TestCliExportCommand(unittest.TestCase):
    def test_export_command_calls_dump_for_each_table(self):
        dump = FakeDumpAdapter()
        schema = FakeSchemaAdapter(tables={"mydb": ["users"]})
        registry = FakeAdapterRegistry({
            "local": FakeEnvironmentAdapter(schema=schema, dump=dump, filesystem=FakeFilesystemAdapter()),
        })
        args = _make_args(
            ctx=_make_args(registry=registry),
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
        registry = FakeAdapterRegistry({
            "local": FakeEnvironmentAdapter(database=db, schema=schema, filesystem=fs),
        })
        args = _make_args(
            ctx=_make_args(registry=registry),
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
        registry = FakeAdapterRegistry({
            "src": FakeEnvironmentAdapter(schema=source_schema, dump=source_dump, filesystem=fs),
            "dst": FakeEnvironmentAdapter(database=target_db, schema=target_schema, filesystem=fs),
        })
        args = _make_args(
            ctx=_make_args(registry=registry),
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
    def test_compare_command_random_sampling_requires_table_name(self):
        registry = FakeAdapterRegistry({})
        args = _make_args(
            ctx=_make_args(registry=registry),
            from_env="src",
            from_schema="src",
            to_env="dst",
            to_schema="dst",
            table_name=None,
            row_count_tolerance=0.10,
            ignore_warnings=False,
            random_sampling=True,
        )
        with patch("builtins.print") as mock_print:
            cmd_compare(args)
            mock_print.assert_any_call("❌ --random-sampling requires --table_name")

    @patch("graphdb.application.compare_service.GraphDB")
    def test_compare_command_uses_random_sampling(self, mock_graphdb_cls):
        db = FakeDatabaseAdapter(responses={})
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": ["users"]})
        config = GraphDBConfig.from_dict({
            "client_bin": "mysql",
            "dump_bin": "mysqldump",
            "default_env": "src",
            "environments": {
                "src": {"host_address": "127.0.0.1", "port": 3306, "username": "u", "password": "p"},
                "dst": {"host_address": "127.0.0.1", "port": 3307, "username": "u", "password": "p"},
            },
        })
        registry = FakeAdapterRegistry({
            "src": FakeEnvironmentAdapter(database=db, schema=schema),
            "dst": FakeEnvironmentAdapter(database=db, schema=schema),
        }, config=config)
        args = _make_args(
            ctx=_make_args(registry=registry),
            from_env="src",
            from_schema="src",
            to_env="dst",
            to_schema="dst",
            table_name="users",
            row_count_tolerance=0.10,
            ignore_warnings=False,
            random_sampling=True,
            sample_size=2048,
        )
        with patch("builtins.print"):
            cmd_compare(args)
        mock_graphdb_cls.assert_called_once()
        mock_graphdb_cls.return_value.compare_tables_by_random_sampling.assert_called_once()
        call_kwargs = mock_graphdb_cls.return_value.compare_tables_by_random_sampling.call_args.kwargs
        self.assertEqual(call_kwargs.get("sample_size"), 2048)

    def test_compare_command_prints_table_result(self):
        db = FakeDatabaseAdapter(responses={
            ("SELECT COUNT(*) FROM `src`.`users`", "src"): [[100]],
            ("SELECT COUNT(*) FROM `dst`.`users`", "dst"): [[100]],
        })
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": ["users"]})
        registry = FakeAdapterRegistry({
            "src": FakeEnvironmentAdapter(database=db, schema=schema),
            "dst": FakeEnvironmentAdapter(database=db, schema=schema),
        })
        args = _make_args(
            ctx=_make_args(registry=registry),
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
