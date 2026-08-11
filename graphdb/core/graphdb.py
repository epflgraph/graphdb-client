# graphdb/core/graphdb.py
# Backward-compatible shim for external apps that import from graphdb.core.graphdb.
# New code should import from graphdb.application.core.app_graphdb.
from graphdb.application.core.app_graphdb import GraphDB

__all__ = ["GraphDB"]
