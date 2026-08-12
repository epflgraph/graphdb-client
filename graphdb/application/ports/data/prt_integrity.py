from __future__ import annotations

from typing import Any, Protocol

class DataIntegrityPort(Protocol):
    """Port interface implemented by matching adapter."""
    def delete_orphaned_rows(self, engine_name, upd_schema, upd_table, upd_key, ref_schema, ref_table, ref_key, upd_where = 'TRUE', ref_where = None, actions = ()): ...
