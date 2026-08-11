from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple


class DDLImportPolicy:
    """In-memory rules for parsing and manipulating MySQL import DDL files."""

    EXCLUDED_DDL_FILES = {
        "CREATE_KEYS.sql",
        "CREATE_TABLE_NO_KEYS.sql",
        "CREATE_TABLE.sql",
        "CREATE_KEYS.sql.gz",
        "CREATE_TABLE_NO_KEYS.sql.gz",
        "CREATE_TABLE.sql.gz",
    }

    @staticmethod
    def ensure_if_not_exists(content: str) -> str:
        """Injects 'IF NOT EXISTS' into a CREATE TABLE statement if missing."""
        if "IF NOT EXISTS" not in content:
            return content.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
        return content

    @classmethod
    def filter_data_sql_files(cls, sql_files: List[Path]) -> List[Path]:
        """Removes DDL schema files from a list of SQL files, leaving only data dump files."""
        return [p for p in sql_files if p.name not in cls.EXCLUDED_DDL_FILES]

    @staticmethod
    def parse_key_statements(content: str) -> List[Tuple[str | None, str]]:
        """
        Parses CREATE_KEYS.sql content and yields tuples of (key_name, key_chunk_sql).
        """
        results: List[Tuple[str | None, str]] = []
        statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]

        for statement in statements:
            match = re.search(r"ADD\s+(.*)", statement, re.IGNORECASE)
            if not match:
                continue

            key_chunk = match.group(1).strip()
            key_name_match = re.search(r"`(.*?)`", key_chunk)
            key_name = key_name_match.group(1) if key_name_match else None
            results.append((key_name, key_chunk))

        return results