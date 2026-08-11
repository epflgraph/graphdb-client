from __future__ import annotations

import os
import shlex
from typing import Any, Dict, Optional, Set, Tuple

from graphdb.common.cmn_ssl_options import (
    build_ssl_cli_flags,
    detect_cli_option_names,
)
from graphdb.domain.mdl_connection import ConnectionParams


def build_mysql_base_command(params: ConnectionParams) -> tuple[list[str], Dict[str, str]]:
    client_bin = params.client_bin or "mysql"
    supported_options = detect_cli_option_names(shlex.split(client_bin))
    ssl_flags = build_ssl_cli_flags(
        params.ssl,
        supported_options=supported_options,
        engine_flavor=params.engine_flavor,
    )

    cmd = shlex.split(client_bin) + [
        "-u", params.username,
        "-h", params.host_address,
        "-P", str(params.port),
    ]
    if ssl_flags:
        cmd += ssl_flags

    env = os.environ.copy()
    env["MYSQL_PWD"] = str(params.password)
    return cmd, env


def build_mysqldump_base_command(params: ConnectionParams) -> tuple[list[str], Dict[str, str], Set[str]]:
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
