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

class DDLExportPolicy:
    """In-memory rules for normalizing MySQL DDL and extracting key constraints into separate statements."""

    @staticmethod
    def normalize_create_table(sql: str) -> str:
        """Normalizes auto_increment signatures and resets auto_increment counters."""
        sql = sql.replace(
            "`row_id` int NOT NULL AUTO_INCREMENT,",
            "`row_id` int NOT NULL AUTO_INCREMENT UNIQUE KEY,",
        )
        sql = sql.replace(
            "`row_id` int unsigned NOT NULL AUTO_INCREMENT,",
            "`row_id` int NOT NULL AUTO_INCREMENT UNIQUE KEY,",
        )
        return re.sub(r"AUTO_INCREMENT=\d+", "AUTO_INCREMENT=1", sql)

    @classmethod
    def split_table_keys(cls, raw_create_sql: str, table_name: str) -> Tuple[str, str, str]:
        """
        Parses raw DDL and returns a tuple of:
        (normalized_create_sql, no_keys_sql, create_keys_alter_sql)
        """
        normalized_sql = cls.normalize_create_table(raw_create_sql)

        keys_chunk = "\n".join(
            re.findall(
                r"(?m)^\s*(?!PRIMARY KEY)(?:UNIQUE KEY|KEY|INDEX|CONSTRAINT)\b.*$",
                normalized_sql,
            )
        )
        no_keys_sql = normalized_sql.replace(keys_chunk, "").replace(",\n\n) ENGINE", "\n) ENGINE")

        create_keys_sql = ""
        for line in keys_chunk.split("\n"):
            line = line.strip()
            if any(k in line for k in ("UNIQUE KEY", "KEY", "INDEX", "CONSTRAINT")):
                if line.endswith(","):
                    line = line[:-1]
                create_keys_sql += f"ALTER TABLE `{table_name}` ADD {line};\n"

        return (
            f"{normalized_sql};\n",
            f"{no_keys_sql};\n",
            f"{create_keys_sql}\n",
        )
