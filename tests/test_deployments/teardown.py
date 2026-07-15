#!/usr/bin/env python3
"""Stop and remove the deployment-matrix containers and network."""
from __future__ import annotations

import subprocess
from pathlib import Path


HERE = Path(__file__).resolve().parent


def teardown() -> None:
    cmd = [
        "docker", "compose",
        "-f", str(HERE / "docker-compose.yml"),
        "down", "-v", "--remove-orphans",
    ]
    print("Tearing down deployment matrix containers")
    subprocess.run(cmd, check=False, cwd=HERE)


if __name__ == "__main__":
    teardown()
