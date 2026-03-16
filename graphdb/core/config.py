from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from yaml import safe_load


class GraphDBConfigError(ValueError):
    pass


@dataclass(frozen=True)
class EnvironmentConfig:
    host_address: str
    port: int
    username: str
    password: str

    @classmethod
    def from_dict(cls, env_name: str, raw: Dict[str, Any]) -> "EnvironmentConfig":
        required = ("host_address", "port", "username", "password")
        missing = [k for k in required if k not in raw]
        if missing:
            raise GraphDBConfigError(
                f"Environment '{env_name}' missing required keys: {', '.join(missing)}"
            )
        return cls(
            host_address=str(raw["host_address"]),
            port=int(raw["port"]),
            username=str(raw["username"]),
            password=str(raw["password"]),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "host_address": self.host_address,
            "port": self.port,
            "username": self.username,
            "password": self.password,
        }


@dataclass(frozen=True)
class GraphDBConfig:
    client_bin: str
    dump_bin: str
    environments: Dict[str, EnvironmentConfig]
    default_env: str
    schema_cache: Optional[str] = None
    schema_test: Optional[str] = None
    data_path: Optional[Dict[str, Any]] = None

    @classmethod
    def default_path(cls) -> Path:
        return Path(__file__).resolve().parents[2] / "config.yaml"

    @classmethod
    def from_default_file(cls) -> "GraphDBConfig":
        return cls.from_file(cls.default_path())

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
        if not isinstance(self.data_path, dict) or "export" not in self.data_path:
            raise GraphDBConfigError("Missing config path: data_path.export")
        return str(self.data_path["export"])
