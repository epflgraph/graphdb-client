import ssl
import unittest

from graphdb.common.cmn_ssl_options import (
    build_ssl_connect_args,
    build_ssl_cli_flags,
    normalize_ssl_options,
    parse_bool,
)


class TestInfrastructureSSLOptions(unittest.TestCase):
    def test_normalize_ssl_options_lowercases_and_converts_hyphens(self):
        opts = normalize_ssl_options({"SSL-CA": "/tmp/ca.pem", "VERIFY-SERVER-CERT": True})
        self.assertEqual(opts["ssl_ca"], "/tmp/ca.pem")
        self.assertTrue(opts["verify_server_cert"])

    def test_parse_bool_handles_various_strings(self):
        self.assertTrue(parse_bool("yes"))
        self.assertFalse(parse_bool("no"))
        self.assertTrue(parse_bool(True))
        self.assertIsNone(parse_bool("maybe"))

    def test_build_ssl_connect_args_disables_verification(self):
        opts = build_ssl_connect_args({"ssl_ca": "/tmp/ca.pem", "verify_server_cert": False})
        self.assertEqual(opts["ca"], "/tmp/ca.pem")
        self.assertEqual(opts["cert_reqs"], ssl.CERT_NONE)
        self.assertFalse(opts["check_hostname"])

    def test_build_ssl_connect_args_defaults_to_required_with_certs(self):
        opts = build_ssl_connect_args({"cert": "/tmp/client.pem"})
        self.assertEqual(opts["cert"], "/tmp/client.pem")
        self.assertEqual(opts["cert_reqs"], ssl.CERT_REQUIRED)

    def test_build_ssl_cli_flags_prefers_ssl_mode(self):
        flags = build_ssl_cli_flags(
            {"mode": "VERIFY_CA", "ca": "/tmp/ca.pem"},
            supported_options={"ssl-mode", "ssl-ca"},
        )
        self.assertIn("--ssl-mode=VERIFY_CA", flags)
        self.assertIn("--ssl-ca=/tmp/ca.pem", flags)
        self.assertNotIn("--ssl", flags)

    def test_build_ssl_cli_flags_uses_legacy_when_ssl_mode_unsupported(self):
        flags = build_ssl_cli_flags(
            {"verify_server_cert": False},
            supported_options={"ssl", "skip-ssl-verify-server-cert"},
        )
        self.assertIn("--ssl", flags)
        self.assertIn("--skip-ssl-verify-server-cert", flags)


if __name__ == "__main__":
    unittest.main()
