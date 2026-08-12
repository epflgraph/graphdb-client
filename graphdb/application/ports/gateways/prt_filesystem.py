from __future__ import annotations

from pathlib import Path
from typing import List, Protocol


class FilesystemPort(Protocol):
    """Port interface implemented by matching adapter."""

    def ensure_dir(self, path: Path) -> None: ...

    def exists(self, path: Path) -> bool: ...

    def list_sql_files(self, folder: Path, compress: bool = False) -> List[Path]: ...

    def read_text(self, path: Path) -> str: ...

    def write_text(self, path: Path, content: str) -> None: ...
