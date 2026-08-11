from __future__ import annotations

from typing import List, Optional, Protocol

from graphdb.application.ports.gateways.prt_connection_test import ConnectionTestPort
from graphdb.application.ports.gateways.prt_export_dump import ExportDumpPort
from graphdb.application.ports.gateways.prt_import_restore import ImportRestorePort
from graphdb.application.ports.gateways.prt_table_metadata import TableMetadataPort


class EnvironmentPort(TableMetadataPort, ExportDumpPort, ImportRestorePort, ConnectionTestPort):
    """Combined port for a fully-capable per-environment database adapter."""


class AdapterRegistryPort(Protocol):
    """Abstract contract for looking up database environment adapters by name."""

    def get(self, env_name: Optional[str] = None) -> EnvironmentPort:
        ...

    def names(self) -> List[str]:
        ...
