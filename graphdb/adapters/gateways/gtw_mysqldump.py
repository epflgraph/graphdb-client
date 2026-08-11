from __future__ import annotations

import gzip
import subprocess
import os
import shlex
from pathlib import Path
from typing import Optional
from typing import Any, Dict, Optional, Set, Tuple

from graphdb.domain.err_exceptions import ExportError
from graphdb.domain.mdl_connection import ConnectionParams

from graphdb.adapters.gateways.utils import (
    build_ssl_cli_flags,
    detect_cli_option_names,
)
from graphdb.domain.mdl_connection import ConnectionParams


def _build_mysqldump_base_command(params: ConnectionParams) -> tuple[list[str], Dict[str, str], Set[str]]:
    dump_bin = params.dump_bin or "mysqldump"
    supported_options = detect_cli_option_names(shlex.split(dump_bin))
    ssl_flags = build_ssl_cli_flags(
        params.ssl,
        supported_options=supported_options,
        engine_flavor=params.engine_flavor,
    )

    cmd = shlex.split(dump_bin) + [
        "-u", params.username,
        "-h", params.host_address,
        "-P", str(params.port),
    ]
    if ssl_flags:
        cmd += ssl_flags

    cmd += [
        "-v",
        "--no-create-db",
        "--no-create-info",
        "--skip-lock-tables",
        "--single-transaction",
    ]

    if "column-statistics" in supported_options:
        cmd += ["--column-statistics=0"]

    env = os.environ.copy()
    env["MYSQL_PWD"] = str(params.password)
    return cmd, env, supported_options



def _clean_dump_stderr(stderr_text: Optional[str]) -> str:
    warning_fragment = "Using a password on the command line interface can be insecure."
    lines = [line for line in (stderr_text or "").splitlines() if warning_fragment not in line]
    return "\n".join(lines).strip()


class MySQLDumpBinaryGateway:
    """Adapter for bulk data export using mysqldump."""

    def __init__(self, params: ConnectionParams, env_name: str = "default") -> None:
        self.params = params
        self.env_name = env_name
        self.base_command, self.env, self.supported_options = _build_mysqldump_base_command(params)

    def _run(self, cmd: list[str], capture_stdout: bool = True) -> subprocess.CompletedProcess:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE if capture_stdout else None,
            stderr=subprocess.PIPE,
            text=True,
            env=self.env,
            check=False,
        )
        return result

    def dump_table_data(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str = "TRUE",
        compress: bool = False,
    ) -> None:
        cmd = self.base_command + [
            schema_name,
            table_name,
            f"--where={where}",
        ]

        if compress:
            self._dump_compressed(cmd, output_file, schema_name, table_name, full=True)
        else:
            cmd += [f"--result-file={output_file}"]
            result = self._run(cmd, capture_stdout=False)
            clean_stderr = _clean_dump_stderr(result.stderr)
            if result.returncode != 0:
                raise ExportError(
                    f"Failed to dump table {schema_name}.{table_name}.\n"
                    f"Return code: {result.returncode}\n"
                    f"STDERR:\n{clean_stderr or '<empty>'}"
                )

    def dump_table_chunk(
        self,
        schema_name: str,
        table_name: str,
        output_file: str,
        where: str = "TRUE",
        chunk_column: str = "row_id",
        chunk_start: int = 0,
        chunk_end: int = 0,
        compress: bool = False,
    ) -> None:
        chunk_where = f"{where} AND ({chunk_column} BETWEEN {chunk_start} AND {chunk_end})"
        cmd = self.base_command + [
            schema_name,
            table_name,
            f"--where={chunk_where}",
        ]

        if compress:
            self._dump_compressed(cmd, output_file, schema_name, table_name, full=False)
        else:
            cmd += [f"--result-file={output_file}"]
            result = self._run(cmd, capture_stdout=False)
            clean_stderr = _clean_dump_stderr(result.stderr)
            if result.returncode != 0:
                raise ExportError(
                    f"Failed to dump table chunk {schema_name}.{table_name} (offset={chunk_start}).\n"
                    f"Return code: {result.returncode}\n"
                    f"STDERR:\n{clean_stderr or '<empty>'}"
                )

    def _dump_compressed(
        self,
        cmd: list[str],
        output_file: str,
        schema_name: str,
        table_name: str,
        full: bool = False,
    ) -> None:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self.env,
        )
        try:
            with gzip.open(output_file, "wb", compresslevel=6) as gz_out:
                while True:
                    chunk = proc.stdout.read(65536)
                    if not chunk:
                        break
                    gz_out.write(chunk)
        finally:
            stderr_bytes = proc.stderr.read()
            result = proc.wait()

        clean_stderr = _clean_dump_stderr(stderr_bytes.decode("utf-8", errors="replace"))
        if result != 0:
            label = "table" if full else "table chunk"
            raise ExportError(
                f"Failed to dump {label} {schema_name}.{table_name}.\n"
                f"Return code: {result}\n"
                f"STDERR:\n{clean_stderr or '<empty>'}"
            )
