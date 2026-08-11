from __future__ import annotations

from typing import Protocol


class ConnectionTestPort(Protocol):
    """Abstract contract for pinging/testing database environment connectivity."""

    def test(self) -> bool:
        ...
