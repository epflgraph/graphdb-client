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


if __name__ == "__main__":
    unittest.main()
