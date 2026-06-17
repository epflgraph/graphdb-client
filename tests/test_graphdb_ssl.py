import ssl
import unittest

from graphdb.core.graphdb import GraphDB


class TestGraphDBSSLHelpers(unittest.TestCase):
    def test_build_ssl_connect_args_disables_verification(self):
        opts = GraphDB._build_ssl_connect_args({
            "ssl_ca": "/tmp/ca.pem",
            "verify_server_cert": False,
        })
        self.assertEqual(opts["ca"], "/tmp/ca.pem")
        self.assertEqual(opts["cert_reqs"], ssl.CERT_NONE)
        self.assertFalse(opts["check_hostname"])

    def test_build_ssl_connect_args_enforces_required_with_ca(self):
        opts = GraphDB._build_ssl_connect_args({
            "cert": "/tmp/client.pem",
        })
        self.assertEqual(opts["cert"], "/tmp/client.pem")
        self.assertEqual(opts["cert_reqs"], ssl.CERT_REQUIRED)
        self.assertTrue(opts["check_hostname"])

    def test_build_ssl_connect_args_parses_string_verification_flags(self):
        opts = GraphDB._build_ssl_connect_args({
            "ca": "/tmp/ca.pem",
            "ssl_verify_server_cert": "false",
        })
        self.assertEqual(opts["cert_reqs"], ssl.CERT_NONE)
        self.assertFalse(opts["check_hostname"])

    def test_build_ssl_cli_flags_uses_ssl_mode_when_supported(self):
        flags = GraphDB._build_ssl_cli_flags(
            {"mode": "VERIFY_CA", "ca": "/tmp/ca.pem"},
            supported_options={"ssl-mode", "ssl-ca"},
        )
        self.assertIn("--ssl-mode=VERIFY_CA", flags)
        self.assertIn("--ssl-ca=/tmp/ca.pem", flags)
        self.assertNotIn("--ssl", flags)

    def test_build_ssl_cli_flags_uses_legacy_verify_toggles_when_supported(self):
        true_flags = GraphDB._build_ssl_cli_flags(
            {"verify_server_cert": True},
            supported_options={"ssl", "ssl-verify-server-cert", "skip-ssl-verify-server-cert"},
        )
        false_flags = GraphDB._build_ssl_cli_flags(
            {"verify_server_cert": False},
            supported_options={"ssl", "ssl-verify-server-cert", "skip-ssl-verify-server-cert"},
        )
        self.assertIn("--ssl", true_flags)
        self.assertIn("--ssl-verify-server-cert", true_flags)
        self.assertIn("--ssl", false_flags)
        self.assertIn("--skip-ssl-verify-server-cert", false_flags)


if __name__ == "__main__":
    unittest.main()
