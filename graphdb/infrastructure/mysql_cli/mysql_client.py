from __future__ import annotations

import gzip
import shlex
import signal
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from graphdb.domain.connection import ConnectionParams
from graphdb.domain.errors import QueryExecutionError
from graphdb.infrastructure.mysql_cli.command_builder import build_mysql_base_command


class MySQLClient:
    """Adapter for running SQL through the native mysql CLI."""

    def __init__(self, params: ConnectionParams, env_name: str = "default") -> None:
        self.params = params
        self.env_name = env_name
        self.base_command, self.env = build_mysql_base_command(params)

    def execute_query(
        self,
        query: str,
        database: Optional[str] = None,
        query_id: Optional[str] = None,
    ) -> None:
        cmd = self.base_command.copy()
        if database:
            cmd += ["-D", database]

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self.env,
        )
        try:
            stdout, stderr = proc.communicate(input=query, timeout=None)
        except BrokenPipeError:
            # SIGPIPE from mysql closing stdin early is expected for some statements.
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)
            try:
                stdout, stderr = proc.communicate(input=query)
            finally:
                signal.signal(signal.SIGPIPE, signal.SIG_DFL)

        if proc.returncode != 0:
            raise QueryExecutionError(
                f"mysql shell execution failed{f' [{query_id}]' if query_id else ''}: "
                f"{stderr.strip() or stdout.strip()}"
            )

    def execute_from_file(
        self,
        file_path: str,
        database: Optional[str] = None,
    ) -> None:
        abs_file_path = Path(file_path).resolve()
        if not abs_file_path.exists():
            raise QueryExecutionError(f"SQL file not found: {abs_file_path}")

        cmd = self.base_command.copy()
        if database:
            cmd += ["-D", database]

        opener = gzip.open if str(abs_file_path).endswith(".gz") else open
        try:
            with opener(abs_file_path, "rt", encoding="utf-8") as fh:
                proc = subprocess.Popen(
                    cmd,
                    stdin=fh,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=self.env,
                )
                stdout, stderr = proc.communicate()
                if proc.returncode != 0:
                    raise QueryExecutionError(
                        f"mysql shell execution failed for file {abs_file_path}: "
                        f"{stderr.strip() or stdout.strip()}"
                    )
        except OSError as exc:
            raise QueryExecutionError(f"Failed to open SQL file: {abs_file_path}") from exc

    def execute_file_with_sed(
        self,
        file_path: str,
        database: Optional[str] = None,
        sed_pattern: str = r"s/^INSERT INTO /INSERT IGNORE INTO /",
    ) -> None:
        """Stream a file (plain or gzipped) through sed and into mysql."""
        abs_file_path = Path(file_path).resolve()
        if not abs_file_path.exists():
            raise QueryExecutionError(f"SQL file not found: {abs_file_path}")

        mysql_cmd = " ".join(shlex.quote(arg) for arg in self.base_command)
        if database:
            mysql_cmd += " -D " + shlex.quote(database)

        if str(abs_file_path).endswith(".gz"):
            pipeline = f"zcat {shlex.quote(str(abs_file_path))} | sed {shlex.quote(sed_pattern)} | {mysql_cmd}"
        else:
            pipeline = f"cat {shlex.quote(str(abs_file_path))} | sed {shlex.quote(sed_pattern)} | {mysql_cmd}"

        result = subprocess.run(
            pipeline,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self.env,
        )
        if result.returncode != 0:
            raise QueryExecutionError(
                f"mysql shell execution failed for file {abs_file_path}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
