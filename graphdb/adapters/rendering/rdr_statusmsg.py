# graphdb/adapters/rendering/rdr_statusmsg.py
from contextlib import contextmanager
from typing import Generator, Iterable, TypeVar
from rich.console import Console
from rich.progress import track as rich_track
from rich.traceback import install
from graphdb.application.ports.rendering.prt_statusmsg import StatusMessagePort

T = TypeVar("T")

class StatusMessageAdapter(StatusMessagePort):
    """Concrete adapter wrapping Rich library formatting."""

    def __init__(self, enable_tracebacks: bool = True):
        self._console = Console()
        if enable_tracebacks:
            install(show_locals=True)

    def success(self, message: str) -> None:
        self._console.print(f"[bold green]✔ Success:[/bold green] {message}")

    def warning(self, message: str) -> None:
        self._console.print(f"[bold yellow]⚠ Warning:[/bold yellow] {message}")

    def error(self, message: str) -> None:
        self._console.print(f"[bold red]✖ Error:[/bold red] {message}")

    def info(self, message: str) -> None:
        self._console.print(f"[bold blue]ℹ Info:[/bold blue] {message}")

    def trace(self, message: str) -> None:
        self._console.print(f"[bold magenta]🔍 Trace:[/bold magenta] {message}")

    @contextmanager
    def status(self, message: str) -> Generator[None, None, None]:
        with self._console.status(f"[bold blue]{message}...", spinner="dots"):
            yield

    def track(self, sequence: Iterable[T], description: str = "") -> Iterable[T]:
        return rich_track(sequence, description=description, console=self._console)
