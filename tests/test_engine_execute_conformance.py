# graphdb-client/tests/test_engine_execute_conformance.py
"""Conformance tests for the real engine executor against the published ports.

The fakes-based port conformance suite proves the test doubles satisfy the
ports; these tests prove the live adapter does. They exist because the Oct
2026 graphregistry e2e crash showed the gap this class of test closes: the
gateway exposed an executor object that never implemented the surface the
downstream contract documented.

No database connection is made: the adapter only stores its dependencies at
construction time, so stub engine and CLI gateway objects are sufficient.
"""
from __future__ import annotations

# Stubs stand in for the SQLAlchemy engine and CLI gateway: the constructor
# only stores references, so no database connection is needed.
from types import SimpleNamespace
from typing import Any, cast

# Live adapter under test plus the two ports it must satisfy.
from graphdb.adapters.engine.eng_execute import EngineExecuteAdapter
from graphdb.application.ports.engine.prt_execute import EngineExecutePort
from graphdb.application.ports.gateways.prt_sqlalchemy import SQLAlchemyQueryExecutorPort

# Internal Function: Build the live adapter against stub dependencies.
def _adapter() -> EngineExecuteAdapter:
    # The constructor stores references only; no connection is opened.
    stub_engine = cast(Any, SimpleNamespace())
    stub_mysql_client = cast(Any, SimpleNamespace(base_command=["mysql"], env={}))
    return EngineExecuteAdapter(engine=stub_engine, mysql_client=stub_mysql_client, env_name="test")

# Public Function: Verify the adapter satisfies the full engine-addressed port.
def test_engine_execute_adapter_conforms_to_engine_execute_port():
    # The full downstream surface (execute_query, shell, file, chunks,
    # safe inserts, upsert) must exist on the live adapter.
    assert isinstance(_adapter(), EngineExecutePort)

# Public Function: Verify the adapter keeps the thin SQLAlchemy surface.
def test_engine_execute_adapter_conforms_to_thin_executor_port():
    # The adapter must also keep the thin surface that the application
    # operations (connectivity, compare) call.
    assert isinstance(_adapter(), SQLAlchemyQueryExecutorPort)

# Public Function: Verify wrong-environment names fail loudly.
def test_engine_execute_adapter_rejects_mismatched_engine_name():
    # Wrong-environment names must fail loudly, not silently execute
    # against a different database than the caller intended.
    adapter = _adapter()
    try:
        adapter.execute_query("other_env", "SELECT 1")
    except ValueError as exc:
        assert "other_env" in str(exc)
    else:
        raise AssertionError("mismatched engine_name was accepted silently")
