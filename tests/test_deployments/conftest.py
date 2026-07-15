"""Shared fixtures for deployment-matrix tests."""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from run_matrix import (  # noqa: E402
    generate_certs,
    generate_configs,
    ensure_clients,
    stop_containers,
    start_containers,
    setup_databases,
    _patched_config_for_local,
)


@pytest.fixture(scope="module")
def deployment():
    """Start the four matrix containers and load fixtures."""
    if shutil.which("docker") is None:
        pytest.skip("docker is not installed")

    generate_certs()
    generate_configs()
    ensure_clients()
    stop_containers()
    start_containers()
    setup_databases()
    yield
    stop_containers()


@pytest.fixture(scope="module")
def graphdb_config_path() -> Path:
    """Path to a no-SSL MySQL config using bundled local client binaries."""
    ensure_clients()
    generate_configs()
    return _patched_config_for_local("mysql", "nossl")
