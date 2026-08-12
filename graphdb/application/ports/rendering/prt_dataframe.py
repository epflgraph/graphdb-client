from __future__ import annotations

from typing import Any, Protocol

class DataframeRendererPort(Protocol):
    """Port interface for rendering pandas DataFrames."""
    def print_dataframe(self, df: Any, title: str) -> None: ...
