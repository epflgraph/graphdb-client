from __future__ import annotations

from typing import Any, Protocol

class DataCellPort(Protocol):
    """Port interface implemented by matching adapter."""
    def get_cells(self, engine_name, schema_name, table_name, select = (), where = (), verbose = False): ...
    def set_cells(self, engine_name, schema_name, table_name, set = (), where = (), verbose = False): ...
