from __future__ import annotations

from typing import Any, Dict, Optional


class GraphDBError(Exception):
    """Base exception for GraphDB client errors."""


class GraphDBConfigError(GraphDBError, ValueError):
    """Raised when the GraphDB configuration is invalid or missing."""


class GraphDBOperationalError(GraphDBError):
    """Raised when a database operational error occurs.

    Attributes expose the original DBAPI error details so callers can
    distinguish transient errors (e.g. lock wait timeouts) from fatal ones.
    """

    def __init__(
        self,
        message: str,
        *,
        dbapi_code: Optional[int] = None,
        dbapi_msg: Optional[str] = None,
        error_type: Optional[str] = None,
        table: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.dbapi_code = dbapi_code
        self.dbapi_msg = dbapi_msg
        self.error_type = error_type
        self.table = table
        self.params = params or {}

    @property
    def is_transient(self) -> bool:
        """Return True if the error is likely transient and worth retrying."""
        return self.dbapi_code in {1205, 1213}  # lock wait timeout / deadlock
