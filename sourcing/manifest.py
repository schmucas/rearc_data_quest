"""Read/write the append-only source_manifest table via the SQL Statement
Execution API.

This never runs on Databricks compute, so there is no `spark` session; every
read and write goes through `WorkspaceClient().statement_execution` against
the warehouse resolved in config.py. Each row is written immediately after its
item is processed -- never batched -- so a mid-run crash leaves every
already-processed item durably recorded. All values travel as named SQL
parameters (`:name`), never string-interpolated into the statement text.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementParameterListItem,
    StatementState,
)

_POLL_INTERVAL_SECONDS = 2
_MAX_WAIT_SECONDS = 120
# The API's own synchronous cap is 50s; ON_WAIT_TIMEOUT=CONTINUE plus our own
# poll loop covers a cold Free Edition serverless warehouse taking longer.
_SYNC_WAIT_TIMEOUT = "30s"


@dataclass(frozen=True)
class ManifestRow:
    """One row of `<catalog_prefix>_ingest.<env>.source_manifest`.

    Attributes mirror the confirmed manifest schema column-for-column.
    """

    source: str
    dataset: str
    ingest_ts: str
    status: str  # FETCHED | UNCHANGED | ERROR
    http_status: Optional[int]
    content_sha256: Optional[str]
    last_modified: Optional[str]
    bytes: Optional[int]
    source_url: str
    landing_path: Optional[str]
    run_id: str
    fetched_at: str  # "YYYY-MM-DD HH:MM:SS.ffffff", UTC, no offset suffix
    error_message: Optional[str]


@dataclass(frozen=True)
class LastKnownState:
    """The most recent FETCHED row for one (source, dataset) pair."""

    ingest_ts: str
    content_sha256: Optional[str]
    last_modified: Optional[str]


def _execute(
    client: WorkspaceClient,
    warehouse_id: str,
    statement: str,
    parameters: Optional[list[StatementParameterListItem]] = None,
):
    """Execute a statement synchronously and return its data_array, if any.

    Args:
        client: Authenticated WorkspaceClient.
        warehouse_id: Id from `config.resolve_warehouse_id`.
        statement: SQL text using named `:param` markers.
        parameters: Statement parameters, or None for a parameterless statement.

    Returns:
        `result.data_array` (list of rows, each a list of strings), or None
        for a statement with no result set (e.g. an INSERT).

    Raises:
        RuntimeError: If the statement's terminal state is not SUCCEEDED.
        TimeoutError: If it never reaches a terminal state within _MAX_WAIT_SECONDS.
    """
    response = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        parameters=parameters,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout=_SYNC_WAIT_TIMEOUT,
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    waited = 0
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if waited >= _MAX_WAIT_SECONDS:
            raise TimeoutError(f"statement {response.statement_id} did not finish within {_MAX_WAIT_SECONDS}s")
        time.sleep(_POLL_INTERVAL_SECONDS)
        waited += _POLL_INTERVAL_SECONDS
        response = client.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error
        raise RuntimeError(f"statement failed: {error.message if error else response.status.state}")

    return response.result.data_array if response.result else None


def fetch_last_known_state(
    client: WorkspaceClient, warehouse_id: str, manifest_table: str, source: str
) -> dict[str, LastKnownState]:
    """Read the most recent FETCHED row per dataset for one source.

    Args:
        client: Authenticated WorkspaceClient.
        warehouse_id: Resolved SQL warehouse id.
        manifest_table: Fully qualified `<catalog_prefix>_ingest.<env>.source_manifest`.
        source: "bls_pr" | "datausa_population".

    Returns:
        Mapping of dataset -> LastKnownState. A dataset with no FETCHED row
        yet (brand-new file, or first-ever run) is simply absent.
    """
    statement = f"""
        SELECT dataset, ingest_ts, content_sha256, last_modified
        FROM {manifest_table}
        WHERE source = :source AND status = 'FETCHED'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dataset ORDER BY ingest_ts DESC) = 1
    """
    rows = _execute(
        client, warehouse_id, statement, parameters=[StatementParameterListItem(name="source", value=source)]
    )
    return {
        dataset: LastKnownState(ingest_ts=ts, content_sha256=sha, last_modified=lm)
        for dataset, ts, sha, lm in (rows or [])
    }


def _param(name: str, value, sql_type: Optional[str] = None) -> StatementParameterListItem:
    """Build one typed statement parameter, converting non-string values to str.

    Args:
        name: The `:name` marker in the statement.
        value: The Python value (already str, int, or None).
        sql_type: Optional Databricks SQL type ("INT", "BIGINT", "TIMESTAMP");
            omitted means STRING.

    Returns:
        A StatementParameterListItem, with value=None representing SQL NULL.
    """
    return StatementParameterListItem(name=name, value=None if value is None else str(value), type=sql_type)


def insert_row(client: WorkspaceClient, warehouse_id: str, manifest_table: str, row: ManifestRow) -> None:
    """Append one manifest row, immediately after the item it describes is processed.

    Args:
        client: Authenticated WorkspaceClient.
        warehouse_id: Resolved SQL warehouse id.
        manifest_table: Fully qualified `<catalog_prefix>_ingest.<env>.source_manifest`.
        row: The row to append.
    """
    statement = f"""
        INSERT INTO {manifest_table}
        (source, dataset, ingest_ts, status, http_status, content_sha256,
         last_modified, bytes, source_url, landing_path, run_id, fetched_at, error_message)
        VALUES
        (:source, :dataset, :ingest_ts, :status, :http_status, :content_sha256,
         :last_modified, :bytes, :source_url, :landing_path, :run_id, :fetched_at, :error_message)
    """
    parameters = [
        _param("source", row.source),
        _param("dataset", row.dataset),
        _param("ingest_ts", row.ingest_ts),
        _param("status", row.status),
        _param("http_status", row.http_status, "INT"),
        _param("content_sha256", row.content_sha256),
        _param("last_modified", row.last_modified),
        _param("bytes", row.bytes, "BIGINT"),
        _param("source_url", row.source_url),
        _param("landing_path", row.landing_path),
        _param("run_id", row.run_id),
        _param("fetched_at", row.fetched_at, "TIMESTAMP"),
        _param("error_message", row.error_message),
    ]
    _execute(client, warehouse_id, statement, parameters=parameters)
