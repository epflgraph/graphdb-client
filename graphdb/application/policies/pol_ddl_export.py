from __future__ import annotations

import re
from typing import Tuple


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
        