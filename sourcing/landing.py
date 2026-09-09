"""Landing-volume path construction and file upload.

Path shape gives one clean Auto Loader prefix per dataset:

    /Volumes/<catalog_prefix>_ingest/<env>/landing/<source>/<dataset>/<filename>__<ts>

Auto Loader's checkpoint tracks individual file *paths* (a RocksDB key-value
store keyed by path), not folders, and by default won't reprocess a path it
has already seen even if the content underneath changed -- see
https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/faq
(`cloudFiles.allowOverwrites`, default false). BLS and DataUSA both rewrite
content at a stable name rather than publishing new filenames over time, so
every release needs a path Auto Loader has never seen before. `ingest_ts` is
computed once per run (`new_ingest_ts`) and appended as a filename suffix to
provide that -- not once per file, and not a date, since two changes on the
same UTC day must not collide on the same suffix.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO

from databricks.sdk import WorkspaceClient

INGEST_TS_FORMAT = "%Y%m%dT%H%M%SZ"


def new_ingest_ts(now: datetime | None = None) -> str:
    """Compute the single ingest_ts used for every item landed in this run.

    Args:
        now: Wall-clock time to stamp with; defaults to the current UTC time.
            Exposed as a parameter purely so tests can pin it.

    Returns:
        A UTC timestamp like "20260909T163000Z".
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return now.strftime(INGEST_TS_FORMAT)


def build_landing_path(landing_base: str, source: str, dataset: str, ingest_ts: str, filename: str) -> str:
    """Build the full Files API path for one landed file.

    Args:
        landing_base: e.g. "/Volumes/rearc_ingest/dev/landing".
        source: "bls_pr" | "datausa_population".
        dataset: Original filename verbatim (BLS), or the fixed dataset key (DataUSA: "population").
        ingest_ts: This run's single UTC timestamp, from `new_ingest_ts`.
        filename: Base name before the ingest_ts suffix. Equal to `dataset`
            except DataUSA (dataset="population", filename="population.json").

    Returns:
        The absolute /Volumes/... path to upload to, e.g.
        ".../bls_pr/pr.series/pr.series__20260909T163000Z".
    """
    return f"{landing_base}/{source}/{dataset}/{filename}__{ingest_ts}"


def upload(client: WorkspaceClient, path: str, content: bytes) -> None:
    """Upload one file's bytes to a Unity Catalog volume via the Files API.

    Calls `create_directory` on the parent first: it is documented as
    idempotent ("mkdir -p" semantics), and the Files API upload docs do not
    state whether PUT auto-creates a missing parent -- the dataset directory
    won't exist yet on a brand-new dataset's first run, so this can't be
    skipped on the assumption it already exists.

    Args:
        client: Authenticated WorkspaceClient.
        path: Absolute /Volumes/... destination, from `build_landing_path`.
        content: Raw response body bytes -- written as-is, no re-encoding.
    """
    parent = path.rsplit("/", 1)[0]
    client.files.create_directory(parent)
    client.files.upload(path, BytesIO(content), overwrite=True)
