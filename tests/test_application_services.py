import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from graphdb.application.operations.ops_compare import CompareOperations
from graphdb.application.operations.ops_connectivity import ConnectivityOperations
from graphdb.application.operations.ops_export import ExportOperations
from graphdb.application.operations.ops_import import ImportOperations
from graphdb.application.operations.ops_copy import CopyOperations
from tests.fakes import (
    FakeEnvironments,
    FakeDatabaseAdapter,
    FakeDumpAdapter,
    FakeEnvironmentAdapter,
    FakeFilesystemAdapter,
    FakeSchemaAdapter,
    FakeStatusMessage,
)


class TestConnectivityOperations(unittest.TestCase):
    def test_test_all_reports_status_per_environment(self):
        registry = FakeEnvironments({
            "up": FakeEnvironmentAdapter(database=FakeDatabaseAdapter()),
            "down": FakeEnvironmentAdapter(database=FakeDatabaseAdapter()),
        })
        registry.get("down").db.connected = False
        service = ConnectivityOperations(registry)
        result = service.test_all()
        self.assertTrue(result["up"])
        self.assertFalse(result["down"])


class TestExportOperations(unittest.TestCase):
    def test_export_create_table_writes_normalized_files(self):
        schema = FakeSchemaAdapter(
            tables={"mydb": ["users"]},
            create_statements={
                "mydb.users": "CREATE TABLE `users` (\n  `row_id` int NOT NULL AUTO_INCREMENT,\n  `name` varchar(64),\n  PRIMARY KEY (`row_id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB AUTO_INCREMENT=123",
            },
        )
        fs = FakeFilesystemAdapter()
        registry = FakeEnvironments({
            "env": FakeEnvironmentAdapter(schema=schema, filesystem=fs),
        })
        service = ExportOperations(registry)
        service.export_create_table("env", "mydb", "users", "/tmp/export")

        paths = list(fs.files.keys())
        self.assertTrue(any("CREATE_TABLE" in str(p) for p in paths))

    def test_export_database_dumps_all_tables(self):
        schema = FakeSchemaAdapter(tables={"mydb": ["users", "posts"]})
        dump = FakeDumpAdapter()
        fs = FakeFilesystemAdapter()
        registry = FakeEnvironments({
            "env": FakeEnvironmentAdapter(schema=schema, dump=dump, filesystem=fs),
        })
        service = ExportOperations(registry)
        service.export_database("env", "mydb", "/tmp/export", include_create_tables=False)
        self.assertEqual(len(dump.dumps), 2)

    def test_export_create_tables_in_database_writes_all_create_table_files(self):
        schema = FakeSchemaAdapter(
            tables={"mydb": ["users", "posts"]},
            create_statements={
                "mydb.users": "CREATE TABLE `users` (\n  `row_id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`row_id`)\n) ENGINE=InnoDB",
                "mydb.posts": "CREATE TABLE `posts` (\n  `row_id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`row_id`)\n) ENGINE=InnoDB",
            },
        )
        fs = FakeFilesystemAdapter()
        registry = FakeEnvironments({
            "env": FakeEnvironmentAdapter(schema=schema, filesystem=fs),
        })
        service = ExportOperations(registry)
        service.export_create_tables_in_database("env", "mydb", "/tmp/export")

        paths = list(fs.files.keys())
        self.assertEqual(len(paths), 6)  # 3 files per table, 2 tables
        self.assertTrue(any("users/CREATE_TABLE.sql" in str(p) for p in paths))
        self.assertTrue(any("posts/CREATE_TABLE.sql" in str(p) for p in paths))

    def test_export_table_data_in_database_dumps_all_tables(self):
        schema = FakeSchemaAdapter(tables={"mydb": ["users", "posts"]})
        dump = FakeDumpAdapter()
        fs = FakeFilesystemAdapter()
        registry = FakeEnvironments({
            "env": FakeEnvironmentAdapter(schema=schema, dump=dump, filesystem=fs),
        })
        service = ExportOperations(registry)
        service.export_table_data_in_database("env", "mydb", "/tmp/export")
        self.assertEqual(len(dump.dumps), 2)
        self.assertEqual({d["table"] for d in dump.dumps}, {"users", "posts"})


class TestCompareOperations(unittest.TestCase):
    def test_compare_tables_by_metadata_reports_fatal_when_table_missing(self):
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": []})
        registry = FakeEnvironments({
            "src": FakeEnvironmentAdapter(schema=schema),
            "dst": FakeEnvironmentAdapter(schema=schema),
        })
        service = CompareOperations(registry, status=FakeStatusMessage())
        result = service.compare_tables_by_metadata("src", "src", "dst", "dst", "missing")
        # A missing table yields a fatal diff entry, not a clean comparison.
        self.assertIn("fatal", result["diffs"])
        self.assertTrue(len(result["diffs"]["fatal"]) > 0)

    def test_compare_tables_by_metadata_returns_structure_for_missing_table(self):
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": ["users"]})
        registry = FakeEnvironments({
            "src": FakeEnvironmentAdapter(schema=schema),
            "dst": FakeEnvironmentAdapter(schema=schema),
        })
        service = CompareOperations(registry, status=FakeStatusMessage())
        # "missing" is not in either schema's table list, so neither side can
        # be fetched; the result must report unavailable source/target metadata.
        result = service.compare_tables_by_metadata("src", "src", "dst", "dst", "missing")
        self.assertIsNone(result["source"])
        self.assertIsNone(result["target"])
        self.assertIsNone(result["table"])

    def test_compare_tables_by_random_sampling_requires_uid_key(self):
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": ["users"]})
        registry = FakeEnvironments({
            "src": FakeEnvironmentAdapter(schema=schema),
            "dst": FakeEnvironmentAdapter(schema=schema),
        })
        service = CompareOperations(registry, status=FakeStatusMessage())
        result = service.compare_tables_by_random_sampling(
            "src", "src", "users",
            "dst", "dst", "users",
            sample_size=10
        )
        self.assertIn("error", result)
        self.assertIn("uid", result["error"])

    # ---------------------------------------------------------------
    # Sampling fixtures: fake schema with a 'uid' key on both sides.
    # sample_size=4 -> round(4/2)=2 uids sampled per side, so the
    # concatenated key set covers ('u1',) and ('u2',) on both ends.
    # ---------------------------------------------------------------
    FLAWLESS_SAMPLING_RESPONSES = {
        ("SELECT `uid` FROM `src`.`users` ORDER BY RAND() LIMIT 2;", "src"): [["u1"], ["u2"]],
        ("SELECT `uid` FROM `dst`.`users` ORDER BY RAND() LIMIT 2;", "dst"): [["u1"], ["u2"]],
        ("SELECT `uid`, `name` FROM `src`.`users` WHERE (`uid`) IN ('u1', 'u2', 'u1', 'u2');", "src"): [["u1", "same"], ["u2", "same"]],
        ("SELECT `uid`, `name` FROM `dst`.`users` WHERE (`uid`) IN ('u1', 'u2', 'u1', 'u2');", "dst"): [["u1", "same"], ["u2", "same"]],
    }

    MISMATCH_SAMPLING_RESPONSES = {
        ("SELECT `uid` FROM `src`.`users` ORDER BY RAND() LIMIT 2;", "src"): [["u1"], ["u2"]],
        ("SELECT `uid` FROM `dst`.`users` ORDER BY RAND() LIMIT 2;", "dst"): [["u1"], ["u2"]],
        ("SELECT `uid`, `name` FROM `src`.`users` WHERE (`uid`) IN ('u1', 'u2', 'u1', 'u2');", "src"): [["u1", "source-value"], ["u2", "source-value"]],
        ("SELECT `uid`, `name` FROM `dst`.`users` WHERE (`uid`) IN ('u1', 'u2', 'u1', 'u2');", "dst"): [["u1", "target-value"], ["u2", "target-value"]],
    }

    def _make_sampling_service(self, responses=None):
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": ["users"]})
        schema.columns = {"src.users": ["uid", "name"], "dst.users": ["uid", "name"]}
        schema.keys = {
            "src.users": {"uid": ["uid"]},
            "dst.users": {"uid": ["uid"]},
        }
        db = FakeDatabaseAdapter(responses=responses or {})
        registry = FakeEnvironments({
            "src": FakeEnvironmentAdapter(database=db, schema=schema),
            "dst": FakeEnvironmentAdapter(database=db, schema=schema),
        })
        return CompareOperations(registry, status=FakeStatusMessage())

    def test_compare_sampling_ignore_warnings_suppresses_empty_tables(self):
        # Empty on both ends + ignore_warnings -> no output at all.
        service = self._make_sampling_service()
        with patch("builtins.print") as mock_print:
            result = service.compare_tables_by_random_sampling(
                "src", "src", "users",
                "dst", "dst", "users",
                sample_size=2, ignore_warnings=True,
            )
        mock_print.assert_not_called()
        self.assertIn("error", result)
        self.assertIn("No rows found", result["error"])

    def test_compare_sampling_prints_empty_table_warning_by_default(self):
        # Empty on both ends without ignore_warnings -> header + warning printed.
        service = self._make_sampling_service()
        with patch("builtins.print") as mock_print:
            service.compare_tables_by_random_sampling(
                "src", "src", "users",
                "dst", "dst", "users",
                sample_size=2,
            )
        printed = " ".join(str(call) for call in mock_print.call_args_list)
        self.assertIn("Source table:", printed)
        self.assertIn("No rows found", printed)

    def test_compare_sampling_hide_flawless_matches_suppresses_output(self):
        # Flawless match + hide_flawless_matches -> no output at all, but the
        # structured result still reports the comparison stats.
        service = self._make_sampling_service(responses=self.FLAWLESS_SAMPLING_RESPONSES)
        with patch("builtins.print") as mock_print:
            result = service.compare_tables_by_random_sampling(
                "src", "src", "users",
                "dst", "dst", "users",
                sample_size=4, hide_flawless_matches=True,
            )
        mock_print.assert_not_called()
        self.assertEqual(result["sample_size"], 2)
        self.assertEqual(result["matched"], 2)
        self.assertEqual(result["mismatched"], 0)

    def test_compare_sampling_prints_flawless_result_by_default(self):
        # Flawless match without hide_flawless_matches -> stats + test result printed.
        service = self._make_sampling_service(responses=self.FLAWLESS_SAMPLING_RESPONSES)
        with patch("builtins.print") as mock_print:
            service.compare_tables_by_random_sampling(
                "src", "src", "users",
                "dst", "dst", "users",
                sample_size=4,
            )
        printed = " ".join(str(call) for call in mock_print.call_args_list)
        self.assertIn("Test result:", printed)
        self.assertIn("Flawless match", printed)

    def test_compare_sampling_hide_flawless_matches_keeps_changed_tables(self):
        # hide_flawless_matches must not suppress tables with actual changes.
        service = self._make_sampling_service(responses=self.MISMATCH_SAMPLING_RESPONSES)
        with patch("builtins.print") as mock_print:
            result = service.compare_tables_by_random_sampling(
                "src", "src", "users",
                "dst", "dst", "users",
                sample_size=4, hide_flawless_matches=True,
            )
        printed = " ".join(str(call) for call in mock_print.call_args_list)
        self.assertIn("Test result:", printed)
        self.assertEqual(result["mismatched"], 2)

    def test_compare_all_tables_sampling_passes_suppression_flags(self):
        # The batch method must forward both flags to every table comparison.
        service = self._make_sampling_service()
        with patch("builtins.print") as mock_print:
            results = service.compare_all_tables_by_random_sampling(
                "src", "src",
                "dst", "dst",
                sample_size=2, ignore_warnings=True, hide_flawless_matches=True,
            )
        mock_print.assert_not_called()
        self.assertEqual(len(results), 1)
        self.assertIn("error", results[0])


class TestImportOperations(unittest.TestCase):
    def test_import_database_passes_fully_qualified_table_folders(self):
        # Regression: import_database used to enumerate bare folder names, so
        # execute_from_file resolved SQL files against the process working
        # directory and crashed on any export folder outside the CWD.
        with tempfile.TemporaryDirectory() as tmpdir:
            export_root = Path(tmpdir) / "mydb"
            for table_name in ["posts", "users"]:
                table_folder = export_root / table_name
                table_folder.mkdir(parents=True)
                create_sql = f"CREATE TABLE `{table_name}` (`row_id` int);"
                (table_folder / "CREATE_TABLE.sql").write_text(create_sql)
                (table_folder / "CREATE_TABLE_NO_KEYS.sql").write_text(create_sql)

            registry = FakeEnvironments({
                "target": FakeEnvironmentAdapter(database=FakeDatabaseAdapter()),
            })
            service = ImportOperations(registry)
            service.import_database("target", "mydb", str(export_root))

        db = registry.get("target").db
        self.assertEqual(len(db.files_executed), 2)
        for file_path in db.files_executed:
            # Every executed SQL file must live under the export folder, never
            # be a bare name resolved against the working directory.
            self.assertTrue(file_path.startswith(str(export_root)), file_path)

    def test_import_database_raises_for_missing_input_folder(self):
        registry = FakeEnvironments({
            "target": FakeEnvironmentAdapter(database=FakeDatabaseAdapter()),
        })
        service = ImportOperations(registry)
        with self.assertRaises(FileNotFoundError):
            service.import_database("target", "mydb", "/nonexistent/import/folder")
        # The target database must not be created when the input is invalid.
        self.assertFalse(registry.get("target").database.database_exists("mydb"))


if __name__ == "__main__":
    unittest.main()
