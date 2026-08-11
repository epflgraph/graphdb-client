import unittest

from graphdb.domain.mdl_config import GraphDBConfig, GraphDBConfigError, EnvironmentConfig


class TestDomainConfig(unittest.TestCase):
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

    def test_config_parsed_from_dict(self):
        cfg = GraphDBConfig.from_dict(self._base_config())
        # Both environment keys and default_env have the `_env` suffix normalised away.
        self.assertEqual(cfg.default_env, "test")
        self.assertIn("test", cfg.environments)

    def test_environment_config_requires_password(self):
        raw = {"host_address": "127.0.0.1", "port": 3306, "username": "user"}
        with self.assertRaises(GraphDBConfigError):
            EnvironmentConfig.from_dict("test", raw)

    def test_legacy_env_block_parsing(self):
        cfg = GraphDBConfig.from_dict({
            "client_bin": "mysql",
            "dump_bin": "mysqldump",
            "default_env": "prod",
            "prod_env": {
                "host_address": "db.example.com",
                "port": 3306,
                "username": "user",
                "password": "pass",
            },
        })
        self.assertIn("prod", cfg.environments)
        self.assertEqual(cfg.environments["prod"].host_address, "db.example.com")


if __name__ == "__main__":
    unittest.main()
