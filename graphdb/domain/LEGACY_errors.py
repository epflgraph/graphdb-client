class GraphDBError(Exception):
    """Base exception for Graph DB Client."""


class ConnectionError(GraphDBError):
    """Raised when a database connection cannot be established."""


class QueryExecutionError(GraphDBError):
    """Raised when query execution fails."""


class SchemaError(GraphDBError):
    """Raised for schema introspection or DDL failures."""


class ExportError(GraphDBError):
    """Raised when an export operation fails."""


class ImportError(GraphDBError):
    """Raised when an import operation fails."""
