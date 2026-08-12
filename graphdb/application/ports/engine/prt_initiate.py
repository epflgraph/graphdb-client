from __future__ import annotations

from typing import Any, Protocol

class EngineInitiatePort(Protocol):
    """Port interface implemented by matching adapter."""
    def initiate_engine(self, server_name): ...
