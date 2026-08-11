from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from graphdb.domain.models.mdl_config import EnvironmentConfig


@dataclass(frozen=True)
class ConnectionParams:
    """Runtime connection parameters derived from EnvironmentConfig."""

    host_address: str
    port: int
    username: str
    password: str
    ssl: Optional[Dict[str, Any]] = None
    client_bin: Optional[str] = None
    dump_bin: Optional[str] = None
    engine_flavor: Optional[str] = None
    sqlalchemy_url: Optional[str] = None
    sqlalchemy_dialect: Optional[str] = None
    sqlalchemy_driver: Optional[str] = None

    @classmethod
    def from_environment(
        cls,
        env_name: str,
        env_config: EnvironmentConfig,
        defaults: Dict[str, Any],
    ) -> ConnectionParams:
        from graphdb.domain.models.mdl_config import EnvironmentConfig

        if isinstance(env_config, EnvironmentConfig):
            return cls(
                host_address=env_config.host_address,
                port=env_config.port,
                username=env_config.username,
                password=env_config.password,
                ssl=env_config.ssl,
                client_bin=env_config.client_bin or defaults.get("client_bin"),
                dump_bin=env_config.dump_bin or defaults.get("dump_bin"),
                engine_flavor=env_config.engine_flavor,
                sqlalchemy_url=env_config.sqlalchemy_url,
                sqlalchemy_dialect=env_config.sqlalchemy_dialect,
                sqlalchemy_driver=env_config.sqlalchemy_driver,
            )
        raise TypeError("env_config must be an EnvironmentConfig instance")
