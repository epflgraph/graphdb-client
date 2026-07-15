#!/usr/bin/env python3
"""Run the GraphDB CLI deployment matrix (8 scenarios)."""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

from certs.generate_certs import generate as generate_certs
from configs.generate_configs import generate as generate_configs
from bin.extract_clients import extract_all as extract_client_binaries


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
CONFIGS_DIR = HERE / "configs"
EXPORTS_DIR = HERE / "exports"
BIN_DIR = HERE / "bin"

MYSQL_BIN_DIR = BIN_DIR / "mysql"
MARIADB_BIN_DIR = BIN_DIR / "mariadb"

SCENARIOS: List[Tuple[str, str, str]] = [
    ("mysql", "local", "nossl"),
    ("mysql", "local", "ssl"),
    ("mysql", "docker", "nossl"),
    ("mysql", "docker", "ssl"),
    ("mariadb", "local", "nossl"),
    ("mariadb", "local", "ssl"),
    ("mariadb", "docker", "nossl"),
    ("mariadb", "docker", "ssl"),
]

LOCAL_BIN_REQUIREMENTS = {
    "mysql": ("mysql", "mysqldump"),
    "mariadb": ("mariadb", "mariadb-dump"),
}

BUNDLED_BIN_DIRS = {
    "mysql": MYSQL_BIN_DIR,
    "mariadb": MARIADB_BIN_DIR,
}


def log(msg: str) -> None:
    print(f"\n{'=' * 60}\n{msg}\n{'=' * 60}")


def run_cmd(cmd: list[str], env: Dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess:
    print("$ " + " ".join(cmd))
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(cmd, env=merged_env, check=check, cwd=ROOT)


def run_graphdb(args: list[str], config_path: Path, env: Dict[str, str] | None = None) -> None:
    cmd = [sys.executable, "-m", "graphdb.cli.main"] + args
    merged_env = os.environ.copy()
    merged_env["GRAPHDB_CONFIG"] = str(config_path)
    if env:
        merged_env.update(env)
    run_cmd(cmd, env=merged_env)


def start_containers() -> None:
    log("Starting Docker containers")
    run_cmd(["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "up", "-d", "--wait"])


def stop_containers() -> None:
    log("Stopping Docker containers")
    run_cmd(["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "down", "-v", "--remove-orphans"], check=False)


def setup_databases() -> None:
    log("Setting up databases and fixtures")
    run_cmd([sys.executable, str(HERE / "setup.py")])


def _bin_path(engine: str, name: str) -> Path | None:
    """Return the path to a client binary, preferring host then bundled."""
    host = shutil.which(name)
    if host:
        return Path(host)
    bundled = BUNDLED_BIN_DIRS[engine] / name
    if bundled.exists():
        return bundled
    return None


def scenario_skip_reason(engine: str, client: str, ssl_label: str) -> str | None:
    if client == "local":
        client_bin, dump_bin = LOCAL_BIN_REQUIREMENTS[engine]
        if _bin_path(engine, client_bin) is None:
            return f"local client binary not found: {client_bin}"
        if _bin_path(engine, dump_bin) is None:
            return f"local dump binary not found: {dump_bin}"
    return None


def _patched_config_for_local(engine: str, ssl_label: str) -> Path:
    """Return a config path that uses bundled binaries when host binaries are missing."""
    scenario_name = f"{engine}_local_{ssl_label}"
    original = CONFIGS_DIR / f"{scenario_name}.yaml"
    with open(original, "r", encoding="utf-8") as fh:
        config_text = fh.read()

    client_bin, dump_bin = LOCAL_BIN_REQUIREMENTS[engine]
    client_path = _bin_path(engine, client_bin)
    dump_path = _bin_path(engine, dump_bin)
    assert client_path is not None and dump_path is not None

    config_text = config_text.replace(f"client_bin: {client_bin}", f"client_bin: {client_path}")
    config_text = config_text.replace(f"dump_bin: {dump_bin}", f"dump_bin: {dump_path}")

    patched = CONFIGS_DIR / f"{scenario_name}_bundled.yaml"
    patched.write_text(config_text, encoding="utf-8")
    return patched


def run_scenario(engine: str, client: str, ssl_label: str) -> Dict[str, str]:
    scenario_name = f"{engine}_{client}_{ssl_label}"
    result = {"scenario": scenario_name, "status": "skipped", "reason": ""}

    reason = scenario_skip_reason(engine, client, ssl_label)
    if reason:
        result["reason"] = reason
        log(f"SKIP {scenario_name}: {reason}")
        return result

    if client == "local" and shutil.which(LOCAL_BIN_REQUIREMENTS[engine][0]) is None:
        config_path = _patched_config_for_local(engine, ssl_label)
    else:
        config_path = CONFIGS_DIR / f"{scenario_name}.yaml"

    if not config_path.exists():
        result["reason"] = f"config not found: {config_path}"
        log(f"SKIP {scenario_name}: {result['reason']}")
        return result

    export_dir = EXPORTS_DIR / scenario_name
    export_dir.mkdir(parents=True, exist_ok=True)

    try:
        log(f"RUN {scenario_name}")

        run_graphdb(["config", "print"], config_path)
        run_graphdb(["test", "--env", "source"], config_path)
        run_graphdb(["inspect", "--query", "SELECT 1", "--title", f"{scenario_name} test"], config_path)

        run_graphdb([
            "export",
            "--env", "source",
            "--schema_name", "source",
            "--output_folder", str(export_dir),
            "-c", "-d",
        ], config_path)

        run_graphdb([
            "import",
            "--env", "target",
            "--schema_name", "target",
            "--input_folder", str(export_dir / "source"),
            "-c", "-d",
        ], config_path)

        run_graphdb([
            "compare",
            "--from_env", "source",
            "--from_schema", "source",
            "--to_env", "target",
            "--to_schema", "target",
            "--table_name", "users",
        ], config_path)

        run_graphdb([
            "copy",
            "--from_env", "source",
            "--from_schema", "source",
            "--to_env", "target",
            "--to_schema", "target",
            "--table_name", "posts",
        ], config_path)

        result["status"] = "passed"
    except subprocess.CalledProcessError as exc:
        result["status"] = "failed"
        result["reason"] = str(exc)
        log(f"FAIL {scenario_name}: {exc}")
    except Exception as exc:
        result["status"] = "failed"
        result["reason"] = str(exc)
        log(f"FAIL {scenario_name}: {exc}")

    return result


def ensure_clients() -> None:
    """Extract client binaries from Docker images if host binaries are missing."""
    needs_extraction = False
    for engine, (client_bin, dump_bin) in LOCAL_BIN_REQUIREMENTS.items():
        if shutil.which(client_bin) is None or shutil.which(dump_bin) is None:
            if _bin_path(engine, client_bin) is None or _bin_path(engine, dump_bin) is None:
                needs_extraction = True
                break
    if needs_extraction:
        log("Extracting client binaries from Docker images")
        extract_client_binaries()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run GraphDB CLI deployment matrix")
    parser.add_argument("--skip-teardown", action="store_true", help="Leave containers running after the run")
    parser.add_argument("--skip-setup", action="store_true", help="Assume containers are already running")
    parser.add_argument("--scenario", help="Run a single scenario, e.g. mysql_local_nossl")
    args = parser.parse_args()

    generate_certs()
    generate_configs()
    ensure_clients()

    if not args.skip_setup:
        stop_containers()
        start_containers()
        setup_databases()

    scenarios = SCENARIOS
    if args.scenario:
        parts = args.scenario.split("_")
        if len(parts) != 3:
            print(f"Invalid scenario: {args.scenario}")
            return 1
        scenarios = [tuple(parts)]  # type: ignore[assignment]

    results: List[Dict[str, str]] = []
    for engine, client, ssl_label in scenarios:
        results.append(run_scenario(engine, client, ssl_label))

    log("Matrix results")
    for r in results:
        line = f"{r['scenario']:<30} {r['status']}"
        if r.get("reason"):
            line += f" ({r['reason']})"
        print(line)

    if not args.skip_teardown and not args.skip_setup:
        stop_containers()

    failed = [r for r in results if r["status"] == "failed"]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
