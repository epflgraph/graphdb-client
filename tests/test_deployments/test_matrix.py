"""Pytest wrapper for the full deployment matrix."""
from __future__ import annotations

from typing import Tuple

import pytest

from run_matrix import SCENARIOS, scenario_skip_reason, run_scenario


@pytest.mark.parametrize("engine,client,ssl_label", SCENARIOS)
def test_scenario(engine: str, client: str, ssl_label: str, deployment) -> None:
    reason = scenario_skip_reason(engine, client, ssl_label)
    if reason:
        pytest.skip(reason)
    result = run_scenario(engine, client, ssl_label)
    assert result["status"] == "passed", f"{result['scenario']} failed: {result.get('reason')}"
