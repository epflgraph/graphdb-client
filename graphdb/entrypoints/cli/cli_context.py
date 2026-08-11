# graphdb/cli/context.py
# Shared context for CLI commands, exposing application services.
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphdb.application.factories.fct_adapter_registry import AdapterRegistry


@dataclass
class CLIContext:
    registry: "AdapterRegistry"
