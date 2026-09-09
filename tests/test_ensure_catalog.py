"""Unit tests for scripts/ensure_catalog.py.

Same pattern as tests/test_sourcing_manifest.py: real databricks-sdk response
dataclasses, no real network/Databricks calls, no Spark.
"""

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.sql import ResultData, ServiceError, StatementResponse, StatementStatus
from databricks.sdk.service.sql import StatementState as State

from scripts.ensure_catalog import ensure_catalog


def _response(state, error_message=None, statement_id="stmt-1"):
    return StatementResponse(
        statement_id=statement_id,
        status=StatementStatus(state=state, error=ServiceError(message=error_message) if error_message else None),
        result=ResultData(),
    )


def test_ensure_catalog_runs_create_catalog_if_not_exists():
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(State.SUCCEEDED)

    ensure_catalog(client, "wh-1", "rearc_stage")

    call_kwargs = client.statement_execution.execute_statement.call_args.kwargs
    assert call_kwargs["statement"] == "CREATE CATALOG IF NOT EXISTS rearc_stage"
    assert call_kwargs["warehouse_id"] == "wh-1"


def test_ensure_catalog_polls_until_succeeded(monkeypatch):
    monkeypatch.setattr("scripts.ensure_catalog.time.sleep", lambda _seconds: None)
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(State.PENDING, statement_id="stmt-2")
    client.statement_execution.get_statement.side_effect = [
        _response(State.RUNNING, statement_id="stmt-2"),
        _response(State.SUCCEEDED, statement_id="stmt-2"),
    ]

    ensure_catalog(client, "wh-1", "rearc_prod")

    assert client.statement_execution.get_statement.call_count == 2


def test_ensure_catalog_raises_on_failed_statement():
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(
        State.FAILED, error_message="permission denied"
    )
    with pytest.raises(RuntimeError, match="permission denied"):
        ensure_catalog(client, "wh-1", "rearc_stage")
