# graphdb/models/sqlquery.py
# Rich-aware SQL query model with alignment, metadata, and rendering helpers.
from __future__ import annotations

import re, hashlib, textwrap
from time import perf_counter
from typing import Any, Iterable, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator
from rich import box
from rich.console import Console, Group
from rich.markup import escape
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.text import Text

# Box styles supported by Rich for panel rendering.
BoxStyle = Literal["rounded", "heavy", "double", "minimal", "simple", "none"]

# Common SQL keywords and clauses for alignment.
# This is not an exhaustive list, but covers many typical patterns.
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

# Mapping of box style names to Rich box objects for panel rendering.
_BOX_MAP = {
    "rounded" : box.ROUNDED,
    "heavy"   : box.HEAVY,
    "double"  : box.DOUBLE,
    "minimal" : box.MINIMAL,
    "simple"  : box.SIMPLE,
    "none"    : None,
}

#============================#
# Class definition: SQLQuery #
#============================#
class SQLQuery(BaseModel):
    """
    Rich-aware SQL query object with alignment, metadata, and rendering helpers.
    """

    # Pydantic model configuration: strip whitespace from string fields by default.
    model_config = ConfigDict(str_strip_whitespace=True)

    #-----------------------#
    # Core query attributes #
    #-----------------------#
    query       : str             = Field(..., min_length=1,  description="Raw SQL query text.")
    description : str             = Field(default="",         description="Human-readable query context.")
    query_id    : str             = Field(default_factory=lambda: str(uuid4()), description="Unique identifier.")
    params      : Any             = Field(default=None,       description="Bound parameters.")
    elapsed_ms  : float    | None = Field(default=None, ge=0, description="Execution time in milliseconds.")
    db          : str      | None = Field(default=None,       description="Database or engine name.")
    title       : str             = Field(default="SQL",      description="Render title.")
    theme       : str             = Field(default="monokai",  description="Rich theme for SQL syntax highlighting.")
    word_wrap   : bool            = Field(default=True,       description="Wrap long SQL lines when rendering.")
    show_header : bool            = Field(default=True,       description="Render title rule.")
    box_style   : BoxStyle        = Field(default="minimal",  description="Panel box style.")
    copyable    : bool            = Field(default=False,      description="Print plain aligned SQL if True.")
    redact_params: bool           = Field(default=True,       description="Redact sensitive values in displayed params.")
    commands    : tuple[str, ...] = Field(default=DEFAULT_SQL_COMMANDS, description="Keywords used for alignment.")
    row_count   : int      | None = Field(default=None, ge=0, description="Number of rows returned/affected.")
    error       : str      | None = Field(default=None,       description="Last execution error, if any.")

    # Internal attribute to track timer state, not included in model fields.
    _timer_started_at: float | None = PrivateAttr(default=None)

    # Validators to ensure query is not blank and commands are normalized.
    @field_validator("query")
    @classmethod
    def _query_not_blank(cls, value: str) -> str:
        normalized = textwrap.dedent(value).strip()
        if not normalized:
            raise ValueError("query cannot be blank.")
        return value

    # Validator to normalize and deduplicate commands for alignment.
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

    # Methods for SQL normalization, alignment, fingerprinting, redaction, timing, and rendering.
    def normalize_sql_lines(self) -> list[str]:
        sql = textwrap.dedent(self.query).strip("\n")
        return [line.rstrip() for line in sql.splitlines() if line.strip()]

    # Canonical SQL is a compact, whitespace-normalized version of the query, useful for logging and hashing.
    def canonical_sql(self) -> str:
        """Compact, whitespace-normalized SQL useful for logging and hashing."""
        return re.sub(r"\s+", " ", self.aligned_sql().strip())

    # Internal method to split a line into command and rest for alignment purposes.
    def _split_command(self, line: str) -> tuple[str | None, str]:
        stripped = line.lstrip()
        for cmd in sorted(self.commands, key=len, reverse=True):
            match = re.match(rf"^{re.escape(cmd)}\b(.*)$", stripped, flags=re.IGNORECASE)
            if match:
                return stripped[: len(cmd)], match.group(1).lstrip()
        return None, stripped

    # Align SQL lines based on the longest matching command for better readability.
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

    # Generate a one-line version of the SQL, truncated if it exceeds max_len.
    def one_line_sql(self, max_len: int = 240) -> str:
        compact = self.canonical_sql()
        if len(compact) <= max_len:
            return compact
        return f"{compact[: max_len - 3]}..."

    # Generate a stable fingerprint of the query for debugging and log correlation, optionally including parameters.
    def fingerprint(self, *, include_params: bool = False, length: int = 12) -> str:
        """
        Stable query fingerprint for debugging and correlation in logs.
        """
        payload = self.canonical_sql()
        if include_params and self.params is not None:
            payload += f"|{repr(self.redacted_params())}"
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
        return digest[:length]

    # Redact potentially sensitive parameters based on key names, returning a new params structure with values replaced by "***REDACTED***".
    def redacted_params(self, *, sensitive_keys: tuple[str, ...] = ("password", "passwd", "pwd", "token", "secret", "api_key")) -> Any:
        """
        Return params with potentially sensitive values redacted.
        """
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

    # Methods for timing execution of a callable, capturing elapsed time, row count, and errors.
    def start_timer(self) -> "SQLQuery":
        self._timer_started_at = perf_counter()
        return self

    # Stop the timer, calculating elapsed time and optionally recording row count and errors.
    def stop_timer(self, *, row_count: int | None = None, error: Exception | str | None = None) -> "SQLQuery":
        if self._timer_started_at is not None:
            self.elapsed_ms = (perf_counter() - self._timer_started_at) * 1000
            self._timer_started_at = None
        if row_count is not None:
            self.row_count = row_count
        if error is not None:
            self.error = str(error)
        return self

    # Generate a debug snapshot of the query's current state as a dictionary, useful for logging and inspection.
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

    # Generate a Rich Text object containing metadata about the query, such as database, execution time,
    # row count, parameters, and optionally debug info like query ID and fingerprint.
    def meta_text(self, *, include_debug: bool = False) -> Text:
        meta = Text()
        if self.db:
            meta.append(f"db={self.db}  ", style="dim")
        if self.elapsed_ms is not None:
            meta.append("time=", style="dim")
            meta.append(f"{self.elapsed_ms:.2f} ms", style="bold magenta")
        if self.row_count is not None:
            meta.append("  rows=", style="dim")
            meta.append(str(self.row_count), style="bold cyan")
        if self.params is not None:
            params_for_display = self.redacted_params() if self.redact_params else self.params
            meta.append("  params=", style="dim")
            meta.append(repr(params_for_display), style="yellow")
        if include_debug:
            meta.append("  qid=", style="dim")
            meta.append(self.query_id[:8], style="bold blue")
            meta.append("  fp=", style="dim")
            meta.append(self.fingerprint(), style="bold green")
        if self.error:
            meta.append("  error=", style="dim")
            meta.append(self.error, style="bold red")
        return meta

    # Generate a Rich Syntax object for the aligned SQL query, using the specified theme and formatting options.
    def syntax(self) -> Syntax:
        return Syntax(
            self.aligned_sql(),
            "sql",
            theme=self.theme,
            line_numbers=False,
            word_wrap=self.word_wrap,
            background_color="black",
        )

    # Generate a Rich Panel containing the SQL syntax and metadata, with styling based on the box_style and show_header options.
    def panel(self, *, include_debug: bool = False) -> Panel:
        meta = self.meta_text(include_debug=include_debug)
        return Panel(
            Group(self.syntax(), meta if meta.plain else Text()),
            border_style="bright_cyan",
            box=_BOX_MAP[self.box_style],
            padding=(1, 2),
            expand=False,
        )

    # Print the SQL query panel to the console, optionally showing a header and using a copyable format if specified.
    def print(self, console: Console | None = None) -> None:
        target = console or Console()
        if self.show_header:
            target.print(Rule(f"[bold bright_blue]{escape(self.title)}"))
        if self.copyable:
            target.print(self.aligned_sql())
            meta = self.meta_text()
            if meta.plain:
                target.print(meta)
            return
        target.print(self.panel())

    # Print a debug version of the SQL query panel, including additional debug information in the metadata.
    def print_debug(self, console: Console | None = None) -> None:
        target = console or Console()
        target.print(Rule(f"[bold bright_blue]{escape(self.title)} [dim](debug)[/dim]"))
        target.print(self.panel(include_debug=True))

    # Execute a callable with automatic timing and error capture, updating the SQLQuery's elapsed time, row count, and error fields accordingly.
    def execute_with_timing(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Run a callable, automatically measuring elapsed time and capturing errors.
        """
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

    # Generate a plain text version of the aligned SQL query, optionally including metadata, suitable for copying to clipboard or logs.
    def as_copyable(self) -> str:
        text = self.aligned_sql()
        meta = self.meta_text()
        return text if not meta.plain else f"{text}\n{meta.plain}"

    # Alternative constructor to create an SQLQuery from separate SELECT, FROM, and optional WHERE clauses, with additional metadata.
    @classmethod
    def from_parts(cls, select: str, from_: str, where: str | None = None, *, title: str = "SQL", **kwargs: Any) -> "SQLQuery":
        lines = [f"SELECT {select}", f"FROM {from_}"]
        if where:
            lines.append(f"WHERE {where}")
        return cls(query="\n".join(lines), title=title, **kwargs)

# Compatibility wrapper for quick usage without needing to construct SQLQuery objects directly.
def print_sql(sql: str, *, params: Any = None, elapsed_ms: float | None = None, db: str | None = None, title: str = "SQL", show_header: bool = True, box_style: BoxStyle = "minimal", copyable: bool = False, theme: str = "monokai", word_wrap: bool = True, console: Console | None = None) -> None:
    """
    Compatibility wrapper around SQLQuery for drop-in usage.
    """
    SQLQuery(
        query=sql,
        params=params,
        elapsed_ms=elapsed_ms,
        db=db,
        title=title,
        show_header=show_header,
        box_style=box_style,
        copyable=copyable,
        redact_params=True,
        theme=theme,
        word_wrap=word_wrap,
    ).print(console=console)
