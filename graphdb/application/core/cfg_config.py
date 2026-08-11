# graphdb/application/core/config.py
# Backward-compatible re-export of the domain configuration module.
# New code should import from graphdb.domain.mdl_config directly.
from graphdb.domain.models.mdl_config import (
    GraphDBConfig,
    GraphDBConfigError,
    EnvironmentConfig,
)

__all__ = ["GraphDBConfig", "GraphDBConfigError", "EnvironmentConfig"]
