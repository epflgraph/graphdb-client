from __future__ import annotations

from pathlib import Path
from typing import Any, List, Protocol


class FilesystemPort(Protocol):
    """Abstract contract for local filesystem reads/writes used by import/export workflows."""

    def write_text(self, path: Any, content: str) -> None:
        ...

    def read_text(self, path: Any) -> str:
        ...

    def ensure_dir(self, path: Any) -> None:
        ...

    def exists(self, path: Any) -> bool:
        ...

    def list_sql_files(self, folder: Any, compress: bool = False) -> List[Path]:
        ...
