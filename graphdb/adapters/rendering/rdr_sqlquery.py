from __future__ import annotations

from typing import Any

from rich import box
from rich.console import Console, Group
from rich.markup import escape
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.text import Text

from graphdb.domain.models.mdl_sqlquery import SQLQuery


# Mapping of box style names to Rich box objects for panel rendering.
_BOX_MAP = {
    "rounded": box.ROUNDED,
    "heavy": box.HEAVY,
    "double": box.DOUBLE,
    "minimal": box.MINIMAL,
    "simple": box.SIMPLE,
    "none": None,
}


def meta_text(query: SQLQuery, *, include_debug: bool = False) -> Text:
    """Build a Rich Text object summarizing query metadata."""
    meta = Text()
    if query.db:
        meta.append(f"db={query.db}  ", style="dim")
    if query.elapsed_ms is not None:
        meta.append("time=", style="dim")
        meta.append(f"{query.elapsed_ms:.2f} ms", style="bold magenta")
    if query.row_count is not None:
        meta.append("  rows=", style="dim")
        meta.append(str(query.row_count), style="bold cyan")
    if query.params is not None:
        params_for_display = query.redacted_params() if query.redact_params else query.params
        meta.append("  params=", style="dim")
        meta.append(repr(params_for_display), style="yellow")
    if include_debug:
        meta.append("  qid=", style="dim")
        meta.append(query.query_id[:8], style="bold blue")
        meta.append("  fp=", style="dim")
        meta.append(query.fingerprint(), style="bold green")
    if query.error:
        meta.append("  error=", style="dim")
        meta.append(query.error, style="bold red")
    return meta


def syntax(query: SQLQuery) -> Syntax:
    """Return a Rich Syntax object for the aligned query SQL."""
    return Syntax(
        query.aligned_sql(),
        "sql",
        theme=query.theme,
        line_numbers=False,
        word_wrap=query.word_wrap,
        background_color="black",
    )


def panel(query: SQLQuery, *, include_debug: bool = False) -> Panel:
    """Return a Rich Panel containing the formatted query and metadata."""
    mt = meta_text(query, include_debug=include_debug)
    parts: list[Syntax | Text] = [syntax(query)]
    if query.description:
        parts.append(Text(query.description, style="italic dim"))
    parts.append(mt if mt.plain else Text())
    return Panel(
        Group(*parts),
        border_style="bright_cyan",
        box=_BOX_MAP[query.box_style],
        padding=(1, 2),
        expand=False,
    )


def print_query(query: SQLQuery, console: Console | None = None) -> None:
    """Render a SQLQuery to the console."""
    target = console or Console()
    if query.show_header:
        target.print(Rule(f"[bold bright_blue]{escape(query.title)}"))
    if query.copyable:
        target.print(query.aligned_sql())
        if query.description:
            target.print(Text(query.description, style="italic dim"))
        mt = meta_text(query)
        if mt.plain:
            target.print(mt)
        return
    target.print(panel(query))


def print_query_debug(query: SQLQuery, console: Console | None = None) -> None:
    """Render a SQLQuery with debug metadata to the console."""
    target = console or Console()
    target.print(Rule(f"[bold bright_blue]{escape(query.title)} [dim](debug)[/dim]"))
    target.print(panel(query, include_debug=True))


def as_copyable(query: SQLQuery) -> str:
    """Return aligned SQL plus metadata as a plain string."""
    text = query.aligned_sql()
    mt = meta_text(query)
    return text if not mt.plain else f"{text}\n{mt.plain}"


def print_sql(
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
) -> None:
    """Compatibility wrapper around SQLQuery for drop-in usage."""
    query = SQLQuery(
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
    )
    print_query(query, console=console)
