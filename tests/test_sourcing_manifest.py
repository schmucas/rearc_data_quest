"""Unit tests for sourcing/manifest.py.

Uses real databricks-sdk response dataclasses (not ad hoc mocks) so these
tests fail loudly if the SDK's shape ever changes, rather than passing against
a stale hand-rolled stand-in.
"""

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.sql import (
    ResultData,
    ServiceError,
    StatementResponse,
    StatementStatus,
)
from databricks.sdk.service.sql import StatementState as State

from sourcing.manifest import ManifestRow, fetch_last_known_state, insert_row


def _response(state, data_array=None, error_message=None, statement_id="stmt-1"):
    return StatementResponse(
        statement_id=statement_id,
        status=StatementStatus(state=state, error=ServiceError(message=error_message) if error_message else None),
        result=ResultData(data_array=data_array) if data_array is not None else None,
    )


def test_fetch_last_known_state_returns_immediately_on_success():
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(
        State.SUCCEEDED,
        data_array=[
            ["pr.series", "20260101T000000Z", "abc123", "Wed, 01 Jan 2026 00:00:00 GMT"],
        ],
    )

    result = fetch_last_known_state(client, "wh-1", "rearc_ingest.dev.source_manifest", "bls_pr")

    assert set(result) == {"pr.series"}
    state = result["pr.series"]
    assert state.ingest_ts == "20260101T000000Z"
    assert state.content_sha256 == "abc123"
    assert state.last_modified == "Wed, 01 Jan 2026 00:00:00 GMT"

    call_kwargs = client.statement_execution.execute_statement.call_args.kwargs
    assert call_kwargs["warehouse_id"] == "wh-1"
    [param] = call_kwargs["parameters"]
    assert param.name == "source"
    assert param.value == "bls_pr"


def test_fetch_last_known_state_empty_when_no_rows():
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(State.SUCCEEDED, data_array=[])
    result = fetch_last_known_state(client, "wh-1", "rearc_ingest.dev.source_manifest", "datausa_population")
    assert result == {}


def test_fetch_last_known_state_polls_until_succeeded(monkeypatch):
    monkeypatch.setattr("sourcing.manifest.time.sleep", lambda _seconds: None)
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(State.PENDING, statement_id="stmt-2")
    client.statement_execution.get_statement.side_effect = [
        _response(State.RUNNING, statement_id="stmt-2"),
        _response(State.SUCCEEDED, data_array=[], statement_id="stmt-2"),
    ]

    result = fetch_last_known_state(client, "wh-1", "rearc_ingest.dev.source_manifest", "bls_pr")

    assert result == {}
    assert client.statement_execution.get_statement.call_count == 2


def test_fetch_last_known_state_raises_on_failed_statement():
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(State.FAILED, error_message="table not found")
    with pytest.raises(RuntimeError, match="table not found"):
        fetch_last_known_state(client, "wh-1", "rearc_ingest.dev.source_manifest", "bls_pr")


def _sample_row(**overrides):
    defaults = dict(
        source="bls_pr",
        dataset="pr.series",
        ingest_ts="20260909T163000Z",
        status="FETCHED",
        http_status=200,
        content_sha256="abc123",
        last_modified="Wed, 01 Jan 2026 00:00:00 GMT",
        bytes=1024,
        source_url="https://download.bls.gov/pub/time.series/pr/pr.series",
        landing_path="/Volumes/rearc_ingest/dev/landing/bls_pr/pr.series/pr.series__20260909T163000Z",
        run_id="12345",
        fetched_at="2026-09-09 16:30:00.000000",
        error_message=None,
    )
    defaults.update(overrides)
    return ManifestRow(**defaults)


def test_insert_row_binds_named_parameters_never_interpolated_values():
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(State.SUCCEEDED)

    row = _sample_row()
    insert_row(client, "wh-1", "rearc_ingest.dev.source_manifest", row)

    call_kwargs = client.statement_execution.execute_statement.call_args.kwargs
    statement = call_kwargs["statement"]
    assert "abc123" not in statement
    assert "pr.series" not in statement
    assert ":source" in statement and ":dataset" in statement and ":error_message" in statement

    params_by_name = {p.name: p for p in call_kwargs["parameters"]}
    assert params_by_name["source"].value == "bls_pr"
    assert params_by_name["dataset"].value == "pr.series"
    assert params_by_name["http_status"].value == "200"
    assert params_by_name["http_status"].type == "INT"
    assert params_by_name["bytes"].value == "1024"
    assert params_by_name["bytes"].type == "BIGINT"
    assert params_by_name["fetched_at"].value == "2026-09-09 16:30:00.000000"
    assert params_by_name["fetched_at"].type == "TIMESTAMP"
    assert params_by_name["error_message"].value is None


def test_insert_row_nulls_out_optional_columns_for_error_status():
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _response(State.SUCCEEDED)

    row = _sample_row(
        status="ERROR",
        http_status=None,
        content_sha256=None,
        last_modified=None,
        bytes=None,
        landing_path=None,
        error_message="HTTP 500",
    )
    insert_row(client, "wh-1", "rearc_ingest.dev.source_manifest", row)

    call_kwargs = client.statement_execution.execute_statement.call_args.kwargs
    params_by_name = {p.name: p for p in call_kwargs["parameters"]}
    assert params_by_name["http_status"].value is None
    assert params_by_name["landing_path"].value is None
    assert params_by_name["error_message"].value == "HTTP 500"
