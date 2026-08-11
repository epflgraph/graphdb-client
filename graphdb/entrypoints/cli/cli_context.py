# graphdb/cli/context.py
# Shared context for CLI commands, exposing application services.
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphdb.adapters.environments import Environments


@dataclass
class CLIContext:
    registry: "Environments"
