from __future__ import annotations

from typing import Any, Protocol

class EngineTestPort(Protocol):
    """Port interface implemented by matching adapter."""
    def test(self, engine_name = None): ...
