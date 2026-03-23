import unittest

from graphdb.core.config import GraphDBConfig, GraphDBConfigError


class TestGraphDBConfig(unittest.TestCase):
    def _base_config(self):
        return {
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

    def test_export_root_prefers_export_path(self):
        cfg = self._base_config()
        cfg["export_path"] = "/custom/export/path"
        parsed = GraphDBConfig.from_dict(cfg)
        self.assertEqual(parsed.export_root(), "/custom/export/path")

    def test_export_root_uses_legacy_data_path_export(self):
        cfg = self._base_config()
        cfg["data_path"] = {"export": "/legacy/export/path"}
        parsed = GraphDBConfig.from_dict(cfg)
        self.assertEqual(parsed.export_root(), "/legacy/export/path")

    def test_export_root_raises_when_missing_all_export_paths(self):
        parsed = GraphDBConfig.from_dict(self._base_config())
        with self.assertRaises(GraphDBConfigError):
            parsed.export_root()


if __name__ == "__main__":
    unittest.main()
