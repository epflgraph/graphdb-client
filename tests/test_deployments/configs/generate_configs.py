#!/usr/bin/env python3
"""Generate the 8 config.yaml files for the GraphDB deployment matrix."""
from __future__ import annotations

import shutil
from pathlib import Path

from yaml import safe_dump


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent.parent
CERTS = HERE.parent / "certs"

ENGINES = {
    "mysql": {
        "image": "mysql:8.0",
        "service_nossl": "graphdb-test-mysql-nossl",
        "service_ssl": "graphdb-test-mysql-ssl",
        "host_port_nossl": 13306,
        "host_port_ssl": 13307,
        "local_client_bin": "mysql",
        "local_dump_bin": "mysqldump",
        "docker_client_bin": "docker run --rm --network graphdb-test mysql:8.0 mysql",
        "docker_dump_bin": "docker run --rm --network graphdb-test mysql:8.0 mysqldump",
        "engine_flavor": "mysql",
    },
    "mariadb": {
        "image": "mariadb:11",
        "service_nossl": "graphdb-test-mariadb-nossl",
        "service_ssl": "graphdb-test-mariadb-ssl",
        "host_port_nossl": 13308,
        "host_port_ssl": 13309,
        "local_client_bin": "mariadb",
        "local_dump_bin": "mariadb-dump",
        "docker_client_bin": "docker run --rm --network graphdb-test mariadb:11 mariadb",
        "docker_dump_bin": "docker run --rm --network graphdb-test mariadb:11 mariadb-dump",
        "engine_flavor": "mariadb",
    },
}


def build_config(engine: str, client: str, ssl: bool) -> dict:
    spec = ENGINES[engine]
    service = spec["service_ssl"] if ssl else spec["service_nossl"]
    host_port = spec["host_port_ssl"] if ssl else spec["host_port_nossl"]

    if client == "local":
        client_bin = spec["local_client_bin"]
        dump_bin = spec["local_dump_bin"]
        host_address = "127.0.0.1"
        port = host_port
    else:
        client_bin = spec["docker_client_bin"]
        dump_bin = spec["docker_dump_bin"]
        host_address = service
        port = 3306

    config = {
        "client_bin": client_bin,
        "dump_bin": dump_bin,
        "default_env": "source",
        "export_path": str(HERE.parent / "exports"),
        "environments": {
            "source": {
                "host_address": host_address,
                "port": port,
                "username": "graphdb",
                "password": "graphdb",
                "engine_flavor": spec["engine_flavor"],
            },
            "target": {
                "host_address": host_address,
                "port": port,
                "username": "graphdb",
                "password": "graphdb",
                "engine_flavor": spec["engine_flavor"],
            },
        },
    }

    if ssl:
        ssl_block = {
            "ca": str(CERTS / "ca.pem"),
            "verify_server_cert": True,
        }
        config["environments"]["source"]["ssl"] = ssl_block
        config["environments"]["target"]["ssl"] = ssl_block

    return config


def generate() -> list[Path]:
    generated: list[Path] = []
    for engine in ("mysql", "mariadb"):
        for client in ("local", "docker"):
            for use_ssl in (False, True):
                ssl_label = "ssl" if use_ssl else "nossl"
                name = f"{engine}_{client}_{ssl_label}.yaml"
                path = HERE / name
                config = build_config(engine, client, use_ssl)
                path.write_text(safe_dump(config, sort_keys=False))
                generated.append(path)
    return generated


if __name__ == "__main__":
    for p in generate():
        print("Generated", p)
