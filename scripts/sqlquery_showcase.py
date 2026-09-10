#!/usr/bin/env python3
"""
Showcase script for graphdb.domain.models.mdl_sqlquery.SQLQuery.

Run:
  PYTHONPATH=. ./.venv.graphdb/bin/python scripts/sqlquery_showcase.py
"""

from __future__ import annotations

from rich.console import Console
from rich.rule import Rule

from graphdb.domain.models.mdl_sqlquery import SQLQuery
from graphdb.adapters.rendering.rdr_sqlquery import (
    as_copyable,
    print_query,
    print_query_debug,
    print_sql,
)


console = Console()


def section(title: str) -> None:
    console.print(Rule(f"[bold cyan]{title}"))


def fake_query_executor() -> list[dict]:
    return [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Alan"}]


def failing_executor() -> None:
    raise RuntimeError("Simulated DB timeout")


def main() -> None:
    print('\n')
    section("1) Build SQLQuery")
    q = SQLQuery(
        query="""
            SELECT id, email, created_at
            FROM users
            WHERE active = 1
            ORDER BY created_at DESC
            LIMIT :limit
        """,
        description="Fetch active users",
        db="xaas_coresrv",
        params={"limit": 100, "password": "should-not-leak"},
        title="Active Users Query",
    )
    console.print(f"query_id={q.query_id}")
    console.print(f"description={q.description!r}")

    print('\n')
    section("2) SQL normalization and alignment")
    console.print("Normalized lines:")
    for line in q.normalize_sql_lines():
        console.print(f"  {line}")
    console.print("\nAligned SQL:")
    console.print(q.aligned_sql())
    console.print(f"\nCanonical SQL: {q.canonical_sql()}")
    console.print(f"One-line SQL: {q.one_line_sql(max_len=90)}")

    print('\n')
    section("3) Fingerprints")
    console.print(f"Fingerprint (SQL only): {q.fingerprint()}")
    console.print(f"Fingerprint (SQL + params): {q.fingerprint(include_params=True)}")

    print('\n')
    section("4) Safe parameter redaction")
    console.print(f"Original params: {q.params}")
    console.print(f"Redacted params: {q.redacted_params()}")

    print('\n')
    section("5) Pretty print / copyable")
    print_query(q, console=console)
    q_copy = q.model_copy(update={"copyable": True, "show_header": False})
    console.print("\nCopyable:")
    console.print(as_copyable(q_copy))

    print('\n')
    section("6) Debug render + snapshot")
    print_query_debug(q, console=console)
    console.print("Debug snapshot dict:")
    console.print(q.debug_snapshot())

    print('\n')
    section("7) Timing helpers")
    q_timed = q.model_copy()
    q_timed.start_timer().stop_timer(row_count=42)
    console.print(f"Timed elapsed_ms={q_timed.elapsed_ms:.4f}, row_count={q_timed.row_count}")

    print('\n')
    section("8) execute_with_timing success")
    q_exec_ok = q.model_copy(update={"title": "Execute Success Demo"})
    rows = q_exec_ok.execute_with_timing(fake_query_executor)
    console.print(f"Returned rows={len(rows)}, elapsed_ms={q_exec_ok.elapsed_ms:.4f}, error={q_exec_ok.error}")
    print_query_debug(q_exec_ok, console=console)

    print('\n')
    section("9) execute_with_timing failure")
    q_exec_fail = q.model_copy(update={"title": "Execute Failure Demo"})
    try:
        q_exec_fail.execute_with_timing(failing_executor)
    except RuntimeError as exc:
        console.print(f"Caught expected error: {exc}")
    console.print(f"error field captured: {q_exec_fail.error!r}")
    print_query_debug(q_exec_fail, console=console)

    print('\n')
    section("10) from_parts constructor")
    q2 = SQLQuery.from_parts(
        select="id, title",
        from_="articles",
        where="published = 1",
        title="Articles Query",
        db="prod",
    )
    print_query(q2, console=console)

    print('\n')
    section("11) Compatibility print_sql wrapper")
    print_sql(
        "SELECT COUNT(*) FROM users WHERE active = 1",
        db="prod",
        elapsed_ms=3.14,
        params={"active": 1},
        title="Wrapper Demo",
    )

    print('\n')
    section("Done")


if __name__ == "__main__":
    main()
