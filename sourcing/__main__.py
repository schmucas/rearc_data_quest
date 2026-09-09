"""Entry point: fetch BLS + DataUSA, land changed files, append manifest rows.

Runs on a GitHub Actions runner (.github/workflows/sourcing.yml), never on
Databricks compute -- Free Edition blocks outbound internet from serverless
compute, so this script talks to the workspace only through the Files API and
SQL Statement Execution API via databricks-sdk's WorkspaceClient (auth from
DATABRICKS_HOST / DATABRICKS_TOKEN).

One item's failure never aborts the run: every BLS file and the DataUSA
document are fetched independently, each writes its own manifest row
immediately, and the process exits non-zero at the end if -- and only if --
at least one item errored.

Invoke as: uv run python -m sourcing --env dev
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import requests
from databricks.sdk import WorkspaceClient

from sourcing import bls, datausa, landing, manifest
from sourcing.config import load_run_config, resolve_warehouse_id
from sourcing.models import FetchOutcome

FETCHED_AT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def _to_row(
    source: str, dataset: str, ingest_ts: str, run_id: str, outcome: FetchOutcome, landing_path: str | None
) -> manifest.ManifestRow:
    """Combine a FetchOutcome with run-level context into a ManifestRow.

    Args:
        source: "bls_pr" | "datausa_population".
        dataset: Original filename (BLS) or dataset key (DataUSA).
        ingest_ts: This run's single UTC timestamp.
        run_id: GitHub Actions run id, or "local".
        outcome: The result of fetching this item.
        landing_path: Where the file was written, or None if nothing landed.

    Returns:
        A ManifestRow ready to insert.
    """
    return manifest.ManifestRow(
        source=source,
        dataset=dataset,
        ingest_ts=ingest_ts,
        status=outcome.status,
        http_status=outcome.http_status,
        content_sha256=outcome.content_sha256,
        last_modified=outcome.last_modified,
        bytes=outcome.byte_count,
        source_url=outcome.source_url,
        landing_path=landing_path,
        run_id=run_id,
        fetched_at=datetime.now(timezone.utc).strftime(FETCHED_AT_FORMAT),
        error_message=outcome.error_message,
    )


def _process(
    client: WorkspaceClient,
    warehouse_id: str,
    manifest_table: str,
    landing_base: str,
    source: str,
    dataset: str,
    ingest_ts: str,
    run_id: str,
    filename: str,
    outcome: FetchOutcome,
) -> bool:
    """Land a FETCHED file (if any) and append its manifest row.

    Args:
        client: Authenticated WorkspaceClient.
        warehouse_id: Resolved SQL warehouse id.
        manifest_table: Fully qualified source_manifest table name.
        landing_base: Root of the landing volume for this environment.
        source: "bls_pr" | "datausa_population".
        dataset: Original filename (BLS) or dataset key (DataUSA).
        ingest_ts: This run's single UTC timestamp.
        run_id: GitHub Actions run id, or "local".
        filename: Name to write inside the ingest_ts partition.
        outcome: The result of fetching this item.

    Returns:
        True if `outcome.status == "ERROR"`.
    """
    landing_path = None
    if outcome.status == "FETCHED":
        landing_path = landing.build_landing_path(landing_base, source, dataset, ingest_ts, filename)
        landing.upload(client, landing_path, outcome.body)

    row = _to_row(source, dataset, ingest_ts, run_id, outcome, landing_path)
    manifest.insert_row(client, warehouse_id, manifest_table, row)
    return outcome.status == "ERROR"


def main(argv: list[str] | None = None) -> int:
    """Run one full fetch cycle.

    Args:
        argv: CLI args, or None to read sys.argv.

    Returns:
        0 if every item succeeded (FETCHED or UNCHANGED), 1 if any errored.
    """
    config = load_run_config(argv)
    client = WorkspaceClient()
    warehouse_id = resolve_warehouse_id(client)
    ingest_ts = landing.new_ingest_ts()
    had_error = False

    # ---- BLS -----------------------------------------------------------
    last_known_bls = manifest.fetch_last_known_state(client, warehouse_id, config.manifest_table, bls.SOURCE)
    session = bls.build_session(config.bls_contact_email)
    try:
        filenames = bls.list_files(session)
    except requests.RequestException as exc:
        outcome = FetchOutcome(
            status="ERROR",
            http_status=None,
            content_sha256=None,
            last_modified=None,
            body=None,
            byte_count=None,
            source_url=bls.BASE_URL,
            error_message=str(exc),
        )
        manifest.insert_row(
            client,
            warehouse_id,
            config.manifest_table,
            _to_row(bls.SOURCE, "_directory_listing", ingest_ts, config.run_id, outcome, None),
        )
        had_error = True
        filenames = []

    for filename in filenames:
        prior = last_known_bls.get(filename)
        outcome = bls.fetch_file(session, filename, prior.last_modified if prior else None)
        had_error |= _process(
            client,
            warehouse_id,
            config.manifest_table,
            config.landing_base,
            bls.SOURCE,
            filename,
            ingest_ts,
            config.run_id,
            filename,
            outcome,
        )

    # ---- DataUSA ---------------------------------------------------------
    last_known_datausa = manifest.fetch_last_known_state(client, warehouse_id, config.manifest_table, datausa.SOURCE)
    prior = last_known_datausa.get(datausa.DATASET)
    outcome = datausa.fetch(requests.Session(), prior.content_sha256 if prior else None)
    had_error |= _process(
        client,
        warehouse_id,
        config.manifest_table,
        config.landing_base,
        datausa.SOURCE,
        datausa.DATASET,
        ingest_ts,
        config.run_id,
        datausa.LANDING_FILENAME,
        outcome,
    )

    return 1 if had_error else 0


if __name__ == "__main__":
    sys.exit(main())
