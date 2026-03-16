# graphregistry/cli/context.py
from dataclasses import dataclass
from typing import TYPE_CHECKING

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Define a dataclass to hold shared context for CLI commands
@dataclass
class CLIContext:
    db : "GraphDB"