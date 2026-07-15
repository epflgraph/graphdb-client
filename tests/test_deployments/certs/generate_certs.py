#!/usr/bin/env python3
"""Generate self-signed CA and server certificate for GraphDB test matrix."""
from __future__ import annotations

import subprocess
from pathlib import Path


HERE = Path(__file__).resolve().parent


def run(cmd: list[str], **kwargs) -> None:
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=HERE, **kwargs)


def generate() -> None:
    key_file = HERE / "server-key.pem"
    if (HERE / "ca.pem").exists():
        print("Certificates already exist; skipping generation.")
        if key_file.exists():
            key_file.chmod(0o644)
        return

    # CA key and certificate
    run(["openssl", "genrsa", "-out", "ca-key.pem", "2048"])
    run([
        "openssl", "req", "-new", "-x509",
        "-key", "ca-key.pem",
        "-out", "ca.pem",
        "-days", "365",
        "-subj", "/CN=graphdb-test-ca",
    ])

    # Server key and CSR
    run(["openssl", "genrsa", "-out", "server-key.pem", "2048"])
    run([
        "openssl", "req", "-new",
        "-key", "server-key.pem",
        "-out", "server.csr",
        "-subj", "/CN=graphdb-test-server",
    ])

    ext_path = HERE / "server.ext"
    ext_path.write_text(
        "subjectAltName=DNS:localhost,DNS:graphdb-test-mysql-ssl,"
        "DNS:graphdb-test-mariadb-ssl,IP:127.0.0.1\n"
    )
    try:
        run([
            "openssl", "x509", "-req",
            "-in", "server.csr",
            "-CA", "ca.pem",
            "-CAkey", "ca-key.pem",
            "-CAcreateserial",
            "-out", "server-cert.pem",
            "-days", "365",
            "-extfile", str(ext_path),
        ])
    finally:
        ext_path.unlink(missing_ok=True)

    # Make server key readable by container users (mysql/mariadb run as non-root)
    (HERE / "server-key.pem").chmod(0o644)

    # Clean up intermediate files
    for name in ("ca-key.pem", "server.csr", "ca.srl"):
        p = HERE / name
        if p.exists():
            p.unlink()

    print("Generated certificates in", HERE)


if __name__ == "__main__":
    generate()
