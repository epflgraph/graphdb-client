# graphdb/domain/models/mdl_sqlquery.py
# Pure SQL query model with alignment, metadata, and timing helpers.
from __future__ import annotations

import hashlib
import re
import textwrap
from collections.abc import Iterable
from time import perf_counter
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

# Box styles supported for rendering.
BoxStyle = Literal["rounded", "heavy", "double", "minimal", "simple", "none"]

# Common SQL keywords and clauses for alignment.
DEFAULT_SQL_COMMANDS: tuple[str, ...] = (
    "CREATE TABLE IF NOT EXISTS",
    "CREATE TABLE",
    "INSERT IGNORE INTO",
    "INSERT INTO",
    "REPLACE INTO",
    "UNION ALL",
    "LEFT OUTER JOIN",
    "RIGHT OUTER JOIN",
    "FULL OUTER JOIN",
    "INNER JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "FULL JOIN",
    "GROUP BY",
    "ORDER BY",
    "PARTITION BY",
    "ALTER TABLE",
    "SELECT DISTINCT",
    "DELETE FROM",
    "SELECT",
    "FROM",
    "WHERE",
    "USING",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "VALUES",
    "UPDATE",
    "DELETE",
    "INSERT",
    "JOIN",
    "ON",
    "SET",
    "AND",
    "OR",
    "CAST",
    "AVG",
    "COALESCE",
    "AS",
    "IS NULL",
    "IS NOT NULL",
    "row_number()",
    "UNIQUE KEY",
    "PRIMARY KEY",
    "KEY",
    "ENGINE",
    "(",
    ")",
    "=",
)


class SQLQuery(BaseModel):
    """SQL query value object with alignment, metadata, and timing helpers."""

    model_config = ConfigDict(str_strip_whitespace=True)

    # Core query attributes
    query: str = Field(..., min_length=1, description="Raw SQL query text.")
    description: str = Field(default="", description="Human-readable query context.")
    query_id: str = Field(default_factory=lambda: str(uuid4()), description="Unique identifier.")
    params: Any = Field(default=None, description="Bound parameters.")
    elapsed_ms: float | None = Field(default=None, ge=0, description="Execution time in milliseconds.")
    db: str | None = Field(default=None, description="Database or engine name.")
    title: str = Field(default="SQL", description="Render title.")
    theme: str = Field(default="monokai", description="Theme for SQL syntax highlighting.")
    word_wrap: bool = Field(default=True, description="Wrap long SQL lines when rendering.")
    show_header: bool = Field(default=True, description="Render title rule.")
    box_style: BoxStyle = Field(default="minimal", description="Panel box style.")
    copyable: bool = Field(default=False, description="Print plain aligned SQL if True.")
    redact_params: bool = Field(default=True, description="Redact sensitive values in displayed params.")
    commands: tuple[str, ...] = Field(default=DEFAULT_SQL_COMMANDS, description="Keywords used for alignment.")
    row_count: int | None = Field(default=None, ge=0, description="Number of rows returned/affected.")
    error: str | None = Field(default=None, description="Last execution error, if any.")

    _timer_started_at: float | None = PrivateAttr(default=None)

    @field_validator("query")
    @classmethod
    def _query_not_blank(cls, value: str) -> str:
        normalized = textwrap.dedent(value).strip()
        if not normalized:
            raise ValueError("query cannot be blank.")
        return normalized

    @field_validator("commands", mode="before")
    @classmethod
    def _normalize_commands(cls, value: Iterable[str] | None) -> tuple[str, ...]:
        if value is None:
            return DEFAULT_SQL_COMMANDS
        unique: list[str] = []
        seen: set[str] = set()
        for item in value:
            text = str(item).strip()
            if not text:
                continue
            key = text.upper()
            if key in seen:
                continue
            seen.add(key)
            unique.append(text)
        return tuple(unique) if unique else DEFAULT_SQL_COMMANDS

    def normalize_sql_lines(self) -> list[str]:
        sql = textwrap.dedent(self.query).strip("\n")
        return [line.rstrip() for line in sql.splitlines() if line.strip()]

    def canonical_sql(self) -> str:
        """Compact, whitespace-normalized SQL useful for logging and hashing."""
        return re.sub(r"\s+", " ", self.aligned_sql().strip())

    def _split_command(self, line: str) -> tuple[str | None, str]:
        stripped = line.lstrip()
        for cmd in sorted(self.commands, key=len, reverse=True):
            match = re.match(rf"^{re.escape(cmd)}\b(.*)$", stripped, flags=re.IGNORECASE)
            if match:
                return stripped[: len(cmd)], match.group(1).lstrip()
        return None, stripped

    def aligned_sql(self) -> str:
        parsed = [self._split_command(line) for line in self.normalize_sql_lines()]
        width = max((len(cmd) for cmd, _ in parsed if cmd), default=0)
        out: list[str] = []
        for cmd, rest in parsed:
            if cmd:
                out.append(f"{cmd.rjust(width)} {rest}".rstrip())
            else:
                out.append(rest)
        return "\n".join(out)

    def one_line_sql(self, max_len: int = 240) -> str:
        compact = self.canonical_sql()
        if len(compact) <= max_len:
            return compact
        return f"{compact[: max_len - 3]}..."

    def fingerprint(self, *, include_params: bool = False, length: int = 12) -> str:
        """Stable query fingerprint for debugging and correlation in logs."""
        payload = self.canonical_sql()
        if include_params and self.params is not None:
            payload += f"|{repr(self.redacted_params())}"
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
        return digest[:length]

    def redacted_params(
        self,
        *,
        sensitive_keys: tuple[str, ...] = ("password", "passwd", "pwd", "token", "secret", "api_key"),
    ) -> Any:
        """Return params with potentially sensitive values redacted."""
        if self.params is None:
            return None

        sensitive = tuple(k.lower() for k in sensitive_keys)

        def _walk(value: Any, key: str | None = None) -> Any:
            if isinstance(value, dict):
                out: dict[str, Any] = {}
                for k, v in value.items():
                    key_str = str(k)
                    out[key_str] = _walk(v, key=key_str)
                return out
            if isinstance(value, (list, tuple)):
                return [_walk(v, key=key) for v in value]
            if key and any(s in key.lower() for s in sensitive):
                return "***REDACTED***"
            return value

        return _walk(self.params)

    def start_timer(self) -> SQLQuery:
        self._timer_started_at = perf_counter()
        return self

    def stop_timer(self, *, row_count: int | None = None, error: Exception | str | None = None) -> SQLQuery:
        if self._timer_started_at is not None:
            self.elapsed_ms = (perf_counter() - self._timer_started_at) * 1000
            self._timer_started_at = None
        if row_count is not None:
            self.row_count = row_count
        if error is not None:
            self.error = str(error)
        return self

    def debug_snapshot(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "fingerprint": self.fingerprint(include_params=False),
            "db": self.db,
            "elapsed_ms": self.elapsed_ms,
            "row_count": self.row_count,
            "has_error": self.error is not None,
            "error": self.error,
            "description": self.description or None,
            "sql": self.one_line_sql(),
            "params": self.redacted_params(),
        }

    def execute_with_timing(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a callable, automatically measuring elapsed time and capturing errors."""
        self.start_timer()
        try:
            result = fn(*args, **kwargs)
            if isinstance(result, (list, tuple)):
                self.stop_timer(row_count=len(result))
            else:
                self.stop_timer()
            return result
        except Exception as exc:
            self.stop_timer(error=exc)
            raise

    @classmethod
    def from_parts(
        cls,
        select: str,
        from_: str,
        where: str | None = None,
        *,
        title: str = "SQL",
        **kwargs: Any,
    ) -> SQLQuery:
        lines = [f"SELECT {select}", f"FROM {from_}"]
        if where:
            lines.append(f"WHERE {where}")
        return cls(query="\n".join(lines), title=title, **kwargs)
