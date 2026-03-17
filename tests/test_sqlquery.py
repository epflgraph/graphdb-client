import unittest

from rich.console import Console

from graphdb.models.sqlquery import SQLQuery, print_sql


class TestSQLQuery(unittest.TestCase):
    def test_query_validation_rejects_blank(self):
        with self.assertRaises(ValueError):
            SQLQuery(query="   \n   ")

    def test_alignment_and_one_line(self):
        q = SQLQuery(
            query="""
                SELECT id, name
                FROM users
                WHERE active = 1
                ORDER BY id DESC
            """
        )
        aligned = q.aligned_sql()
        self.assertIn("SELECT id, name", aligned)
        self.assertIn("FROM users", aligned)
        self.assertIn("WHERE active = 1", aligned)
        self.assertIn("ORDER BY id DESC", aligned)
        self.assertIn("SELECT id, name FROM users WHERE active = 1", q.one_line_sql())

    def test_redaction_in_meta_by_default(self):
        q = SQLQuery(query="SELECT 1", params={"password": "secret", "token": "abc", "id": 7})
        meta = q.meta_text().plain
        self.assertIn("***REDACTED***", meta)
        self.assertNotIn("secret", meta)
        self.assertNotIn("abc", meta)
        self.assertIn("'id': 7", meta)

    def test_fingerprint_changes_when_including_params(self):
        q = SQLQuery(query="SELECT * FROM users WHERE id = :id", params={"id": 123})
        fp_sql_only = q.fingerprint(include_params=False)
        fp_with_params = q.fingerprint(include_params=True)
        self.assertNotEqual(fp_sql_only, fp_with_params)

    def test_debug_snapshot_contains_expected_keys(self):
        q = SQLQuery(query="SELECT 1", db="prod", description="healthcheck")
        snap = q.debug_snapshot()
        self.assertEqual(snap["db"], "prod")
        self.assertEqual(snap["description"], "healthcheck")
        self.assertIn("fingerprint", snap)
        self.assertIn("sql", snap)

    def test_execute_with_timing_success_sets_elapsed_and_rowcount(self):
        q = SQLQuery(query="SELECT * FROM users")
        out = q.execute_with_timing(lambda: [1, 2, 3])
        self.assertEqual(out, [1, 2, 3])
        self.assertIsNotNone(q.elapsed_ms)
        self.assertEqual(q.row_count, 3)
        self.assertIsNone(q.error)

    def test_execute_with_timing_failure_sets_error(self):
        q = SQLQuery(query="SELECT * FROM users")

        def _boom():
            raise RuntimeError("db timeout")

        with self.assertRaises(RuntimeError):
            q.execute_with_timing(_boom)
        self.assertIsNotNone(q.elapsed_ms)
        self.assertEqual(q.error, "db timeout")

    def test_print_sql_wrapper_uses_redaction(self):
        console = Console(record=True, width=160)
        print_sql(
            "SELECT 1",
            params={"password": "x", "token": "y", "id": 2},
            title="Wrapper",
            console=console,
        )
        output = console.export_text()
        self.assertIn("***REDACTED***", output)
        self.assertNotIn("password': 'x'", output)
        self.assertNotIn("token': 'y'", output)


if __name__ == "__main__":
    unittest.main()
