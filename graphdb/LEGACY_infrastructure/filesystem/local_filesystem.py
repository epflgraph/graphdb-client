from __future__ import annotations

import glob
from pathlib import Path
from typing import List

from graphdb.domain.ports.filesystem_port import FilesystemPort


class LocalFilesystem:
    """Adapter for local filesystem operations."""

    def write_text(self, path: Path, content: str) -> None:
        path.write_text(content, encoding="utf-8")

    def read_text(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def exists(self, path: Path) -> bool:
        return path.exists()

    def list_sql_files(self, folder: Path, compress: bool = False) -> List[Path]:
        if compress:
            return sorted(Path(p) for p in glob.glob(str(folder / "*.sql.gz")))

        plain = sorted(Path(p) for p in glob.glob(str(folder / "*.sql")) if not p.endswith(".gz"))
        gzipped = sorted(Path(p) for p in glob.glob(str(folder / "*.sql.gz")))
        return plain + gzipped
