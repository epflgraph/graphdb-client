from __future__ import annotations

import ssl
import subprocess
import re
import shlex
from typing import Any, Dict, Optional, Set


class SSLConfigError(ValueError):
    pass


def normalize_ssl_options(raw_ssl: Any) -> Dict[str, Any]:
    if not isinstance(raw_ssl, dict):
        return {}
    normalized = {}
    for key, value in raw_ssl.items():
        if not isinstance(key, str):
            continue
        normalized[key.lower().replace("-", "_")] = value
    return normalized


def parse_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized_value = value.strip().lower()
        if normalized_value in {"1", "true", "yes", "on"}:
            return True
        if normalized_value in {"0", "false", "no", "off"}:
            return False
    return None


def build_ssl_connect_args(ssl_options: Any) -> Dict[str, Any]:
    normalized = normalize_ssl_options(ssl_options)
    if not normalized:
        return {}

    connect_args: Dict[str, Any] = {}

    for key, value in normalized.items():
        if key in {"mode", "ssl_mode", "verify_server_cert", "ssl_verify_server_cert"}:
            continue
        if value is None:
            continue
        target_key = key[4:] if key.startswith("ssl_") else key
        connect_args[target_key] = value

    verify_value = None
    for verify_key in ("verify_server_cert", "ssl_verify_server_cert"):
        if verify_key in normalized:
            verify_value = normalized[verify_key]
            break
    verify_bool = parse_bool(verify_value)

    if verify_bool is False:
        connect_args["cert_reqs"] = ssl.CERT_NONE
        connect_args["check_hostname"] = False
    elif verify_bool is True:
        connect_args["cert_reqs"] = ssl.CERT_REQUIRED
        connect_args["check_hostname"] = True
    elif "ca" in connect_args or "cert" in connect_args or "key" in connect_args:
        connect_args.setdefault("cert_reqs", ssl.CERT_REQUIRED)
        connect_args.setdefault("check_hostname", True)

    return connect_args


_cli_option_cache: Dict[str, Set[str]] = {}


def detect_cli_option_names(base_command: list[str]) -> Set[str]:
    if not base_command:
        return set()

    cache_key = " ".join(base_command)
    if cache_key in _cli_option_cache:
        return _cli_option_cache[cache_key]

    options: Set[str] = set()
    try:
        result = subprocess.run(
            base_command + ["--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        help_text = f"{result.stdout}\n{result.stderr}"
        for option in re.findall(
            r"^\s*(?:-[^,\s]+,\s+)?--([a-z0-9][a-z0-9-]*)", help_text, flags=re.MULTILINE
        ):
            options.add(option.lower())
    except Exception:
        options = set()

    _cli_option_cache[cache_key] = options
    return options


def build_ssl_cli_flags(
    ssl_options: Any,
    supported_options: Optional[Set[str]] = None,
    engine_flavor: Optional[str] = None,
) -> list[str]:
    normalized = normalize_ssl_options(ssl_options)
    if not normalized:
        return []
    supported_options = {opt.lower() for opt in (supported_options or set())}

    flags: list[str] = []

    def _pick(*keys: str) -> Any:
        for key in keys:
            if key in normalized:
                return normalized[key]
        return None

    mode_value = _pick("mode", "ssl_mode")
    verify_value = _pick("verify_server_cert", "ssl_verify_server_cert")
    verify_bool = parse_bool(verify_value)

    if mode_value is not None and str(mode_value).strip().upper() == "DISABLED":
        return []

    if "ssl-mode" in supported_options:
        if mode_value is None:
            if verify_bool is True:
                mode_value = "VERIFY_IDENTITY"
            elif verify_bool is False:
                mode_value = "REQUIRED"
            else:
                mode_value = "REQUIRED"

        mode_value = str(mode_value).strip().upper()
        allowed_modes = {"DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"}
        if mode_value not in allowed_modes:
            raise SSLConfigError(f"Unsupported MySQL ssl mode: {mode_value}")

        flags.append(f"--ssl-mode={mode_value}")

    elif "ssl" in supported_options or not supported_options:
        flags.append("--ssl")

        if verify_bool is True and (
            not supported_options or "ssl-verify-server-cert" in supported_options
        ):
            flags.append("--ssl-verify-server-cert")
        elif verify_bool is False and (
            not supported_options or "skip-ssl-verify-server-cert" in supported_options
        ):
            flags.append("--skip-ssl-verify-server-cert")

    for opt_keys, cli_option in [
        (("ca", "ssl_ca"), "--ssl-ca"),
        (("cert", "ssl_cert"), "--ssl-cert"),
        (("key", "ssl_key"), "--ssl-key"),
        (("cipher", "ssl_cipher"), "--ssl-cipher"),
    ]:
        value = _pick(*opt_keys)
        if value is not None and (
            not supported_options or cli_option.lstrip("-") in supported_options
        ):
            flags.append(f"{cli_option}={value}")

    return flags
