#!/usr/bin/env python3
"""Set up source/target schemas and seed data for the deployment matrix."""
from __future__ import annotations

import subprocess
import tempfile
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"
SCHEMA_SQL = FIXTURES / "schema.sql"
DATA_SQL = FIXTURES / "data.sql"

SERVICES = [
    "mysql-nossl",
    "mysql-ssl",
    "mariadb-nossl",
    "mariadb-ssl",
]

ROOT_PASSWORD = "rootpass"

CLIENT_BY_SERVICE = {
    "mysql-nossl": "mysql",
    "mysql-ssl": "mysql",
    "mariadb-nossl": "mariadb",
    "mariadb-ssl": "mariadb",
}


def run_service_sql(service: str, database: str, sql_file: Path) -> None:
    client = CLIENT_BY_SERVICE[service]
    cmd = [
        "docker", "compose",
        "-f", str(HERE / "docker-compose.yml"),
        "exec", "-T", service,
        "bash", "-c",
        f"{client} -h 127.0.0.1 -u root -p{ROOT_PASSWORD} {database} < /dev/stdin",
    ]
    print(f"Loading {sql_file.name} into {service}.{database}")
    with sql_file.open("rb") as fh:
        subprocess.run(cmd, stdin=fh, check=True, cwd=HERE)


def create_databases(service: str) -> None:
    client = CLIENT_BY_SERVICE[service]
    sql = (
        "DROP DATABASE IF EXISTS source;\n"
        "DROP DATABASE IF EXISTS target;\n"
        "DROP DATABASE IF EXISTS _e2e_scratch;\n"
        "CREATE DATABASE source;\n"
        "CREATE DATABASE target;\n"
        "CREATE DATABASE _e2e_scratch;\n"
        "GRANT ALL PRIVILEGES ON *.* TO 'graphdb'@'%';\n"
        "FLUSH PRIVILEGES;\n"
    )
    # Use TCP via 127.0.0.1; the Unix socket may not be ready immediately
    # after the container reports healthy.
    cmd = [
        "docker", "compose",
        "-f", str(HERE / "docker-compose.yml"),
        "exec", "-T", service,
        "bash", "-c",
        f"{client} -h 127.0.0.1 -u root -p{ROOT_PASSWORD} < /dev/stdin",
    ]
    print(f"Creating databases on {service}")

    last_error = None
    for attempt in range(10):
        result = subprocess.run(cmd, input=sql.encode("utf-8"), cwd=HERE, capture_output=True)
        if result.returncode == 0:
            return
        last_error = result.stderr.decode("utf-8", errors="replace").strip()
        print(f"  attempt {attempt + 1} failed: {last_error}")
        time.sleep(1)

    raise RuntimeError(f"Failed to create databases on {service}: {last_error}")


def setup() -> None:
    for service in SERVICES:
        create_databases(service)
        for db in ("source", "target"):
            run_service_sql(service, db, SCHEMA_SQL)
        run_service_sql(service, "source", DATA_SQL)


if __name__ == "__main__":
    setup()
