from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path
from typing import Any, Dict, Optional

from yaml import safe_load


def _parse_dotenv_assignment(line: str) -> tuple[str, str] | None:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None
    if raw.startswith("export "):
        raw = raw[len("export ") :].lstrip()
    if "=" not in raw:
        return None
    key, value = raw.split("=", 1)
    key = key.strip()
    if not key:
        return None
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    return key, value


def _find_dotenv_file() -> Optional[Path]:
    cwd = Path.cwd()
    for candidate_dir in (cwd, *cwd.parents):
        candidate = candidate_dir / ".env"
        if candidate.exists():
            return candidate
    pkg_root = Path(__file__).resolve().parents[2]
    candidate = pkg_root / ".env"
    if candidate.exists():
        return candidate
    docker_candidate = Path("/app/.env")
    if docker_candidate.exists():
        return docker_candidate
    return None


@lru_cache(maxsize=1)
def _resolve_dotenv_override() -> Optional[Path]:
    override = os.getenv("GRAPHDB_CONFIG")
    if override:
        return Path(override).expanduser().resolve()

    dotenv = _find_dotenv_file()
    if not dotenv:
        return None

    try:
        with dotenv.open("r", encoding="utf-8") as fh:
            for line in fh:
                parsed = _parse_dotenv_assignment(line)
                if parsed and parsed[0] == "GRAPHDB_CONFIG":
                    value = parsed[1]
                    if value:
                        return Path(value).expanduser().resolve()
    except OSError:
        return None

    return None


class GraphDBConfigError(ValueError):
    pass


@dataclass(frozen=True)
class EnvironmentConfig:
    host_address: str
    port: int
    username: str
    password: str
    ssl: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, env_name: str, raw: Dict[str, Any]) -> "EnvironmentConfig":
        required = ("host_address", "port", "username", "password")
        missing = [k for k in required if k not in raw]
        if missing:
            raise GraphDBConfigError(
                f"Environment '{env_name}' missing required keys: {', '.join(missing)}"
            )
        ssl_cfg = raw.get("ssl")
        if ssl_cfg is not None and not isinstance(ssl_cfg, dict):
            raise GraphDBConfigError(
                f"Environment '{env_name}' ssl block must be a dictionary."
            )
        return cls(
            host_address=str(raw["host_address"]),
            port=int(raw["port"]),
            username=str(raw["username"]),
            password=str(raw["password"]),
            ssl=ssl_cfg,
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "host_address": self.host_address,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "ssl": self.ssl,
        }


@dataclass(frozen=True)
class GraphDBConfig:
    client_bin: str
    dump_bin: str
    environments: Dict[str, EnvironmentConfig]
    default_env: str
    schema_cache: Optional[str] = None
    schema_test: Optional[str] = None
    export_path: Optional[str] = None
    data_path: Optional[Dict[str, Any]] = None

    @classmethod
    def default_path(cls) -> Path:
        override = _resolve_dotenv_override()
        if override:
            return override
        return Path(__file__).resolve().parents[2] / "config.yaml"

    @classmethod
    def default_paths(cls) -> list[Path]:
        candidates: list[Path] = []

        override = _resolve_dotenv_override()
        if override:
            candidates.append(override)

        candidates.append(Path.cwd() / "config.yaml")
        candidates.append(Path("/app/config.yaml"))
        candidates.append(Path(__file__).resolve().parents[2] / "config.yaml")

        unique: list[Path] = []
        seen: set[str] = set()
        for path in candidates:
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        return unique

    @classmethod
    def from_default_file(cls) -> "GraphDBConfig":
        for path in cls.default_paths():
            if path.exists():
                return cls.from_file(path)
        searched = ", ".join(str(p) for p in cls.default_paths())
        raise GraphDBConfigError(f"Config file not found. Searched: {searched}")

    @classmethod
    def from_file(cls, path: Path | str) -> "GraphDBConfig":
        cfg_path = Path(path)
        if not cfg_path.exists():
            raise GraphDBConfigError(f"Config file not found: {cfg_path}")
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = safe_load(f) or {}
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "GraphDBConfig":
        if not isinstance(raw, dict):
            raise GraphDBConfigError("config.yaml must parse into a dictionary.")

        mysql_cfg = raw.get("mysql") if isinstance(raw.get("mysql"), dict) else raw
        if not isinstance(mysql_cfg, dict):
            raise GraphDBConfigError("Invalid config structure.")

        client_bin = str(mysql_cfg.get("client_bin", raw.get("client_bin", "mysql")))
        dump_bin = str(mysql_cfg.get("dump_bin", raw.get("dump_bin", "mysqldump")))
        schema_cache = mysql_cfg.get("schema_cache", raw.get("schema_cache"))
        schema_test = mysql_cfg.get("schema_test", raw.get("schema_test"))
        export_path = mysql_cfg.get("export_path", raw.get("export_path"))
        data_path = mysql_cfg.get("data_path", raw.get("data_path"))

        environments: Dict[str, EnvironmentConfig] = {}
        env_block = mysql_cfg.get("environments", raw.get("environments"))
        if isinstance(env_block, dict):
            for env_key, env_value in env_block.items():
                if isinstance(env_value, dict):
                    env_name = str(env_key).removesuffix("_env")
                    environments[env_name] = EnvironmentConfig.from_dict(env_name, env_value)

        reserved_keys = {
            "client_bin",
            "dump_bin",
            "default_env",
            "schema_cache",
            "schema_test",
            "export_path",
            "data_path",
            "environments",
        }
        for key, value in mysql_cfg.items():
            if key in reserved_keys or not isinstance(value, dict):
                continue
            if key.endswith("_env"):
                env_name = key.removesuffix("_env")
                environments[env_name] = EnvironmentConfig.from_dict(env_name, value)

        if len(environments) == 0:
            raise GraphDBConfigError(
                "No environments found. Define environments under 'environments:' or '*_env' keys."
            )

        default_env_raw = mysql_cfg.get("default_env", raw.get("default_env"))
        default_env = (
            str(default_env_raw).removesuffix("_env")
            if default_env_raw
            else next(iter(environments.keys()))
        )
        if default_env not in environments:
            raise GraphDBConfigError(
                f"default_env '{default_env}' is not in configured environments: {sorted(environments.keys())}"
            )

        return cls(
            client_bin=client_bin,
            dump_bin=dump_bin,
            environments=environments,
            default_env=default_env,
            schema_cache=schema_cache,
            schema_test=schema_test,
            export_path=str(export_path) if export_path else None,
            data_path=data_path if isinstance(data_path, dict) else None,
        )

    def env_names(self) -> list[str]:
        return list(self.environments.keys())

    def get_env(self, env_name: str) -> EnvironmentConfig:
        if env_name not in self.environments:
            raise GraphDBConfigError(
                f"Unknown environment '{env_name}'. Available: {sorted(self.environments.keys())}"
            )
        return self.environments[env_name]

    def export_root(self) -> str:
        if self.export_path:
            return str(self.export_path)
        if isinstance(self.data_path, dict) and self.data_path.get("export"):
            return str(self.data_path["export"])
        raise GraphDBConfigError(
            "Missing config path for exports. Set either 'export_path' or 'data_path.export' in config.yaml"
        )
