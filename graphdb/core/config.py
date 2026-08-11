# graphdb/core/config.py
# Backward-compatible shim for external apps that import from graphdb.core.config.
# New code should import from graphdb.application.core.cfg_config or graphdb.domain.mdl_config.
from graphdb.application.core.cfg_config import (
    GraphDBConfig,
    GraphDBConfigError,
    EnvironmentConfig,
)

__all__ = ["GraphDBConfig", "GraphDBConfigError", "EnvironmentConfig"]
