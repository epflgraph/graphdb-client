# graphdb/cli/context.py
# Shared context for CLI commands, exposing the composition root.
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphdb.entrypoints.cli.container import Container


@dataclass
class CLIContext:
    container: "Container"
