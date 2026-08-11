import unittest

from graphdb.application.operations.ops_compare import CompareOperations
from graphdb.application.operations.ops_connectivity import ConnectivityOperations
from graphdb.application.operations.ops_export import ExportOperations
from graphdb.application.operations.ops_import import ImportOperations
from graphdb.application.operations.ops_copy import CopyOperations
from tests.fakes import (
    FakeAdapterRegistry,
    FakeDatabaseAdapter,
    FakeDumpAdapter,
    FakeEnvironmentAdapter,
    FakeFilesystemAdapter,
    FakeSchemaAdapter,
)


class TestConnectivityOperations(unittest.TestCase):
    def test_test_all_reports_status_per_environment(self):
        registry = FakeAdapterRegistry({
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
        registry = FakeAdapterRegistry({
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
        registry = FakeAdapterRegistry({
            "env": FakeEnvironmentAdapter(schema=schema, dump=dump, filesystem=fs),
        })
        service = ExportOperations(registry)
        service.export_database("env", "mydb", "/tmp/export", include_create_tables=False)
        self.assertEqual(len(dump.dumps), 2)


class TestCompareOperations(unittest.TestCase):
    def test_compare_tables_reports_error_when_table_missing(self):
        schema = FakeSchemaAdapter(tables={"src": ["users"], "dst": []})
        registry = FakeAdapterRegistry({
            "src": FakeEnvironmentAdapter(schema=schema),
            "dst": FakeEnvironmentAdapter(schema=schema),
        })
        service = CompareOperations(registry)
        result = service.compare_tables("src", "src", "dst", "dst", "missing")
        self.assertIn("error", result)

    def test_compare_tables_reports_ok_for_equal_tables(self):
        db = FakeDatabaseAdapter(responses={
            ("SELECT COUNT(*) FROM `src`.`users`", "src"): [[100]],
            ("SELECT COUNT(*) FROM `dst`.`users`", "dst"): [[100]],
        })
        schema = FakeSchemaAdapter(
            tables={"src": ["users"], "dst": ["users"]},
        )
        registry = FakeAdapterRegistry({
            "src": FakeEnvironmentAdapter(database=db, schema=schema),
            "dst": FakeEnvironmentAdapter(database=db, schema=schema),
        })
        service = CompareOperations(registry)
        result = service.compare_tables("src", "src", "dst", "dst", "users")
        self.assertNotIn("error", result)
        statuses = {r["metric"]: r["status"] for r in result["rows"]}
        self.assertEqual(statuses["table_rows"], "OK")


if __name__ == "__main__":
    unittest.main()
