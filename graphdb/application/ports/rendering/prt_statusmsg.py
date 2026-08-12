# graphdb/application/ports/rendering/prt_statusmsg.py
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Generator, Iterable, TypeVar

T = TypeVar("T")

class StatusMessagePort(ABC):
    """Abstract Port defining console UI operations."""

    @abstractmethod
    def success(self, message: str) -> None:
        pass

    @abstractmethod
    def warning(self, message: str) -> None:
        pass

    @abstractmethod
    def error(self, message: str) -> None:
        pass

    @abstractmethod
    def info(self, message: str) -> None:
        pass

    @abstractmethod
    def trace(self, message: str) -> None:
        pass

    @abstractmethod
    @contextmanager
    def status(self, message: str) -> Generator[None, None, None]:
        """Context manager for animated status spinners."""
        pass

    @abstractmethod
    def track(self, sequence: Iterable[T], description: str = "") -> Iterable[T]:
        """Iterates through a sequence with a progress indicator."""
        pass
