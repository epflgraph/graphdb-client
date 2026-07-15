#!/usr/bin/env python3
"""Extract mysql/mysqldump and mariadb/mariadb-dump clients from Docker images."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


HERE = Path(__file__).resolve().parent


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print("$ " + " ".join(cmd))
    return subprocess.run(cmd, check=check, cwd=HERE)


def extract(image: str, binaries: list[str], dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    container = f"graphdb-client-extract-{image.replace(':', '-')}"
    run(["docker", "rm", "-f", container], check=False)
    run(["docker", "create", "--name", container, image])
    try:
        for binary in binaries:
            dest = dest_dir / binary
            src = f"{container}:/usr/bin/{binary}"
            run(["docker", "cp", src, str(dest)])
            dest.chmod(0o755)
            print(f"Extracted {binary} -> {dest}")
    finally:
        run(["docker", "rm", "-f", container], check=False)


def extract_all() -> None:
    if shutil.which("docker") is None:
        raise RuntimeError("docker is required to extract client binaries")

    extract("mysql:8.0", ["mysql", "mysqldump"], HERE / "mysql")
    extract("mariadb:11", ["mariadb", "mariadb-dump"], HERE / "mariadb")


if __name__ == "__main__":
    extract_all()
