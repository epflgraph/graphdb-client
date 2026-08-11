from __future__ import annotations
from typing import Any

class TableComparisonPolicy:
    """In-memory rules for evaluating row count tolerances and column metric statuses."""

    @staticmethod
    def evaluate_row_count_status(a: Any, b: Any, tolerance: float) -> str:
        try:
            a_val = None if a is None else int(a)
            b_val = None if b is None else int(b)
        except Exception:
            return "ERR" if a != b else "OK"

        if a_val == b_val:
            return "OK"
        if a_val is None or b_val is None:
            return "ERR"
        if a_val == 0 and b_val == 0:
            return "OK"
        if a_val == 0 or b_val == 0:
            return "ERR"

        rel_diff = abs(a_val - b_val) / max(a_val, b_val)
        return "WARN" if rel_diff <= tolerance else "ERR"

    @staticmethod
    def evaluate_metric_status(metric: str, source_val: Any, target_val: Any, tolerance: float) -> str:
        if metric == "table_rows":
            return TableComparisonPolicy.evaluate_row_count_status(source_val, target_val, tolerance)
        return "OK" if source_val == target_val else "ERR"
