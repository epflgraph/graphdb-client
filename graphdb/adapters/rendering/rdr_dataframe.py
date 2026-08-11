from __future__ import annotations

from typing import Any
from tabulate import tabulate


def print_dataframe(df: Any, title: str) -> None:
    """Pretty-print a pandas DataFrame using tabulate."""
    print(f"\n{title}\n")
    headers = df.columns if hasattr(df, "columns") else "keys"
    print(tabulate(df, headers=headers, tablefmt="grid", showindex=False))
