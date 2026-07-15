# graphdb/core/config.py
# Backward-compatible re-export of the domain configuration module.
# New code should import from graphdb.domain.config directly.
from graphdb.domain.config import (
    GraphDBConfig,
    GraphDBConfigError,
    EnvironmentConfig,
)

__all__ = ["GraphDBConfig", "GraphDBConfigError", "EnvironmentConfig"]
