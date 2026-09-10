"""Ensure this deploy's target catalog exists before `databricks bundle deploy`.

Both declarative pipeline resources (resources/dp_bronze_ingestion.yml,
resources/dp_silver_gold.yml) point at `<catalog_prefix>_<env>`, and the
Pipelines API validates that catalog exists at *creation* time -- a first
deploy to a brand-new environment fails with CATALOG_DOES_NOT_EXIST otherwise,
since the catalog is normally created by setup_job, which itself can't run
until the bundle has been deployed once.
This script breaks that chicken-and-egg loop with a one-line, idempotent
`CREATE CATALOG IF NOT EXISTS`, safe to run on every deploy.

Uses plain SQL rather than the Catalogs REST API (`databricks catalogs
create`): that call fails on this workspace's Free Edition metastore with
"Metastore storage root URL does not exist" unless a managed location is
given explicitly, while `CREATE CATALOG IF NOT EXISTS` in SQL falls back to
default storage without issue.

Invoke as a module (a bare script path puts scripts/ on sys.path instead of
the repo root, breaking the sourcing import): uv run python -m scripts.ensure_catalog --env stage
"""

from __future__ import annotations

import argparse
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementState,
)

from sourcing.config import read_catalog_prefix, resolve_warehouse_id

_POLL_INTERVAL_SECONDS = 2
_MAX_WAIT_SECONDS = 120
_SYNC_WAIT_TIMEOUT = "30s"


def ensure_catalog(client: WorkspaceClient, warehouse_id: str, catalog: str) -> None:
    """Run `CREATE CATALOG IF NOT EXISTS <catalog>`, waiting for completion.

    Args:
        client: Authenticated WorkspaceClient.
        warehouse_id: Resolved SQL warehouse id.
        catalog: Catalog name to create, e.g. "rearc_stage".

    Raises:
        RuntimeError: If the statement's terminal state is not SUCCEEDED.
        TimeoutError: If it never reaches a terminal state within _MAX_WAIT_SECONDS.
    """
    response = client.statement_execution.execute_statement(
        statement=f"CREATE CATALOG IF NOT EXISTS {catalog}",
        warehouse_id=warehouse_id,
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


def main() -> None:
    """Create `<catalog_prefix>_<env>` for the environment given on the CLI."""
    parser = argparse.ArgumentParser(description="Ensure a bundle target's working catalog exists.")
    parser.add_argument("--env", required=True, help="Target environment, e.g. stage or prod.")
    args = parser.parse_args()

    catalog = f"{read_catalog_prefix()}_{args.env}"
    client = WorkspaceClient()
    ensure_catalog(client, resolve_warehouse_id(client), catalog)
    print(f"{catalog} ready")


if __name__ == "__main__":
    main()
