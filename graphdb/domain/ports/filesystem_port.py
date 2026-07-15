from __future__ import annotations

from pathlib import Path
from typing import List, Protocol


class FilesystemPort(Protocol):
    """Port for local file operations used by import/export workflows."""

    def write_text(self, path: Path, content: str) -> None:
        ...

    def read_text(self, path: Path) -> str:
        ...

    def ensure_dir(self, path: Path) -> None:
        ...

    def list_sql_files(self, folder: Path, compress: bool = False) -> List[Path]:
        ...

    def exists(self, path: Path) -> bool:
        ...
