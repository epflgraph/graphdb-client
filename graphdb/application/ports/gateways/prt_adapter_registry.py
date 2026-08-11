from __future__ import annotations

from typing import List, Optional, Protocol
from graphdb.application.ports.gateways.prt_table_metadata import TableMetadataPort


class AdapterRegistryPort(Protocol):
    """Abstract contract for looking up database environment adapters by name."""

    def get(self, env_name: Optional[str] = None) -> TableMetadataPort:
        ...

    def names(self) -> List[str]:
        ...
