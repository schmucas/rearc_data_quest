"""Full-mock orchestration tests for sourcing/__main__.py.

Everything that would touch the network or a real Databricks workspace is
patched: WorkspaceClient, warehouse resolution, manifest reads/writes,
landing uploads, and both fetchers. These tests only verify orchestration --
per-item isolation, landing-before-manifest ordering, and the overall exit
code -- not the fetch/parsing logic itself (covered by the other test files).
"""

from unittest.mock import MagicMock, patch

import requests

import sourcing.__main__ as main_module
from sourcing.models import FetchOutcome


def _outcome(status, source_url="https://example.com/x", **overrides):
    defaults = dict(
        status=status,
        http_status=200 if status == "FETCHED" else None,
        content_sha256="abc123" if status == "FETCHED" else None,
        last_modified=None,
        body=b"data" if status == "FETCHED" else None,
        byte_count=4 if status == "FETCHED" else None,
        source_url=source_url,
        error_message="boom" if status == "ERROR" else None,
    )
    defaults.update(overrides)
    return FetchOutcome(**defaults)


def _run_main(argv, bls_filenames, bls_outcomes, datausa_outcome, listing_exception=None):
    """Patch every external boundary and run main(), returning (exit_code, insert_calls)."""
    with (
        patch("sourcing.__main__.WorkspaceClient", return_value=MagicMock()),
        patch("sourcing.__main__.resolve_warehouse_id", return_value="wh-1"),
        patch("sourcing.manifest.fetch_last_known_state", return_value={}),
        patch("sourcing.manifest.insert_row") as mock_insert,
        patch("sourcing.landing.upload") as mock_upload,
        patch("sourcing.bls.list_files") as mock_list_files,
        patch("sourcing.bls.fetch_file", side_effect=bls_outcomes),
        patch("sourcing.datausa.fetch", return_value=datausa_outcome),
        patch.dict("os.environ", {"BLS_CONTACT_EMAIL": "you@example.com"}, clear=False),
    ):
        if listing_exception is not None:
            mock_list_files.side_effect = listing_exception
        else:
            mock_list_files.return_value = bls_filenames

        exit_code = main_module.main(argv)
        return exit_code, mock_insert.call_args_list, mock_upload.call_args_list


def test_all_success_exits_zero_with_one_insert_per_item():
    exit_code, insert_calls, upload_calls = _run_main(
        ["--env", "dev"],
        bls_filenames=["pr.class", "pr.series"],
        bls_outcomes=[_outcome("FETCHED"), _outcome("UNCHANGED")],
        datausa_outcome=_outcome("FETCHED"),
    )
    assert exit_code == 0
    # 2 BLS files + 1 DataUSA document
    assert len(insert_calls) == 3
    # Only FETCHED outcomes land a file: pr.class + datausa population = 2
    assert len(upload_calls) == 2


def test_one_bls_error_still_records_every_item_and_exits_nonzero():
    exit_code, insert_calls, upload_calls = _run_main(
        ["--env", "dev"],
        bls_filenames=["pr.class", "pr.series"],
        bls_outcomes=[_outcome("ERROR"), _outcome("FETCHED")],
        datausa_outcome=_outcome("UNCHANGED"),
    )
    assert exit_code == 1
    assert len(insert_calls) == 3  # error isolation: every item still gets a row
    assert len(upload_calls) == 1  # only the successful BLS file landed


def test_listing_failure_records_one_error_row_and_datausa_still_runs():
    exit_code, insert_calls, upload_calls = _run_main(
        ["--env", "dev"],
        bls_filenames=[],
        bls_outcomes=[],
        datausa_outcome=_outcome("FETCHED"),
        listing_exception=requests.ConnectionError("listing down"),
    )
    assert exit_code == 1
    # one synthetic _directory_listing ERROR row + one DataUSA row
    assert len(insert_calls) == 2
    rows = [call.args[3] for call in insert_calls]
    assert any(row.dataset == "_directory_listing" and row.status == "ERROR" for row in rows)
    assert any(row.source == "datausa_population" and row.status == "FETCHED" for row in rows)
    assert len(upload_calls) == 1  # only the DataUSA document landed
