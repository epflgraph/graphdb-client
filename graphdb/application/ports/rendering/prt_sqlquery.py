from __future__ import annotations

from typing import Any, Protocol

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

from graphdb.domain.models.mdl_sqlquery import SQLQuery


class SQLQueryRendererPort(Protocol):
    """Port interface for rendering SQLQuery objects."""
    def meta_text(self, query: SQLQuery, *, include_debug: bool = False) -> Text: ...
    def syntax(self, query: SQLQuery) -> Syntax: ...
    def panel(self, query: SQLQuery, *, include_debug: bool = False) -> Panel: ...
    def print_query(self, query: SQLQuery, console: Console | None = None) -> None: ...
    def print_query_debug(self, query: SQLQuery, console: Console | None = None) -> None: ...
    def as_copyable(self, query: SQLQuery) -> str: ...
    def print_sql(
        self,
        sql: str,
        *,
        params: Any = None,
        elapsed_ms: float | None = None,
        db: str | None = None,
        title: str = "SQL",
        show_header: bool = True,
        box_style: Any = "minimal",
        copyable: bool = False,
        theme: str = "monokai",
        word_wrap: bool = True,
        console: Console | None = None,
    ) -> None: ...
