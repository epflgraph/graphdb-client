from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class Column:
    """Value object representing a database table column definition."""

    name: str
    datatype: str
    nullable: Optional[bool] = None
    default: Optional[str] = None


@dataclass(frozen=True)
class Key:
    """Value object representing a database index or key constraint."""

    name: str
    columns: List[str]
    unique: bool = False
    primary: bool = False


@dataclass(frozen=True)
class Table:
    """Domain entity representing a database table structure."""

    name: str
    engine: Optional[str] = None
    collation: Optional[str] = None
    row_format: Optional[str] = None
    columns: List[Column] = field(default_factory=list)
    keys: List[Key] = field(default_factory=list)
    create_sql: Optional[str] = None


@dataclass(frozen=True)
class View:
    """Domain entity representing a database view definition."""

    name: str
    create_sql: Optional[str] = None
