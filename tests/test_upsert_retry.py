import unittest
from unittest.mock import patch

from graphdb.core.config import GraphDBConfig
from graphdb.core.exceptions import GraphDBConfigError, GraphDBOperationalError
from graphdb.core.graphdb import GraphDB


class TestGraphDBOperationalError(unittest.TestCase):
    def test_exposes_dbapi_attributes(self):
        exc = GraphDBOperationalError(
            "lock wait timeout",
            dbapi_code=1205,
            dbapi_msg="Lock wait timeout exceeded",
            error_type="OperationalError",
            table="test.nodes",
            params={"id": 1},
        )
        self.assertEqual(exc.dbapi_code, 1205)
        self.assertEqual(exc.dbapi_msg, "Lock wait timeout exceeded")
        self.assertEqual(exc.error_type, "OperationalError")
        self.assertEqual(exc.table, "test.nodes")
        self.assertEqual(exc.params, {"id": 1})
        self.assertTrue(exc.is_transient)

    def test_is_transient_for_deadlock(self):
        exc = GraphDBOperationalError("deadlock", dbapi_code=1213)
        self.assertTrue(exc.is_transient)

    def test_is_not_transient_for_other_codes(self):
        exc = GraphDBOperationalError("syntax error", dbapi_code=1064)
        self.assertFalse(exc.is_transient)


class TestExecuteUpsertRowRetry(unittest.TestCase):
    def setUp(self):
        # Reset the singleton so each test gets a fresh instance.
        GraphDB._instance = None
        self.config = GraphDBConfig.from_dict(
            {
                "client_bin": "mysql",
                "dump_bin": "mysqldump",
                "default_env": "test_env",
                "environments": {
                    "test_env": {
                        "host_address": "127.0.0.1",
                        "port": 3306,
                        "username": "user",
                        "password": "pass",
                    }
                },
            }
        )
        self.db = GraphDB(config=self.config)

    def tearDown(self):
        GraphDB._instance = None

    @patch("graphdb.core.graphdb.GraphDB.execute_query")
    def test_duplicate_key_logs_warning_and_continues(self, mock_query):
        mock_query.return_value = ("OperationalError", "Duplicate entry", 1062)
        result = self.db.execute_upsert_row(
            "test_env", "graph_registry", "Nodes_N_Object",
            ["object_type", "object_id"], ["Publication", "pub-1"],
            ["title"], ["New title"],
            actions=("commit",),
        )
        self.assertIsNone(result)
        mock_query.assert_called_once()

    @patch("graphdb.core.graphdb.GraphDB.execute_query")
    def test_retries_lock_wait_timeout_then_succeeds(self, mock_query):
        mock_query.side_effect = [
            ("OperationalError", "Lock wait timeout exceeded", 1205),
            ("OperationalError", "Lock wait timeout exceeded", 1205),
            [],  # successful commit returns a list
        ]
        result = self.db.execute_upsert_row(
            "test_env", "graph_registry", "Nodes_N_Object",
            ["object_type", "object_id"], ["Publication", "pub-1"],
            ["title"], ["New title"],
            actions=("commit",),
            max_retries=3,
            retry_delay=0,
        )
        self.assertIsNone(result)  # no 'eval' action
        self.assertEqual(mock_query.call_count, 3)

    @patch("graphdb.core.graphdb.GraphDB.execute_query")
    def test_raises_typed_error_after_exhausting_retries(self, mock_query):
        mock_query.return_value = ("OperationalError", "Lock wait timeout exceeded", 1205)
        with self.assertRaises(GraphDBOperationalError) as ctx:
            self.db.execute_upsert_row(
                "test_env", "graph_registry", "Nodes_N_Object",
                ["object_type", "object_id"], ["Publication", "pub-1"],
                ["title"], ["New title"],
                actions=("commit",),
                max_retries=2,
                retry_delay=0,
            )
        self.assertEqual(ctx.exception.dbapi_code, 1205)
        self.assertEqual(ctx.exception.table, "graph_registry.Nodes_N_Object")
        self.assertIn("persisted after 2 retries", str(ctx.exception))
        self.assertEqual(mock_query.call_count, 3)

    @patch("graphdb.core.graphdb.GraphDB.execute_query")
    def test_raises_typed_error_immediately_for_non_transient_errors(self, mock_query):
        mock_query.return_value = ("OperationalError", "Access denied", 1045)
        with self.assertRaises(GraphDBOperationalError) as ctx:
            self.db.execute_upsert_row(
                "test_env", "graph_registry", "Nodes_N_Object",
                ["object_type", "object_id"], ["Publication", "pub-1"],
                ["title"], ["New title"],
                actions=("commit",),
                max_retries=2,
                retry_delay=0,
            )
        self.assertEqual(ctx.exception.dbapi_code, 1045)
        self.assertFalse(ctx.exception.is_transient)
        mock_query.assert_called_once()


class TestGraphDBConfigErrorStillImportable(unittest.TestCase):
    def test_can_import_from_config_module(self):
        from graphdb.core.config import GraphDBConfigError as ConfigError
        self.assertTrue(issubclass(ConfigError, ValueError))

    def test_can_import_from_exceptions_module(self):
        from graphdb.core.exceptions import GraphDBConfigError as ConfigError
        self.assertTrue(issubclass(ConfigError, ValueError))
