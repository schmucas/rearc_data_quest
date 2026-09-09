"""Unit tests for sourcing/landing.py."""

from datetime import datetime, timedelta, timezone

from sourcing.landing import build_landing_path, new_ingest_ts, upload


def test_new_ingest_ts_formats_utc():
    fixed = datetime(2026, 9, 9, 16, 30, 0, tzinfo=timezone.utc)
    assert new_ingest_ts(fixed) == "20260909T163000Z"


def test_new_ingest_ts_converts_non_utc_to_utc():
    # 16:30 in UTC-5 is 21:30 UTC.
    fixed = datetime(2026, 9, 9, 16, 30, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert new_ingest_ts(fixed) == "20260909T213000Z"


def test_build_landing_path_bls_all_data():
    path = build_landing_path(
        "/Volumes/rearc_ingest/dev/landing", "bls_pr", "pr.data.1.AllData", "20260909T163000Z", "pr.data.1.AllData"
    )
    assert path == "/Volumes/rearc_ingest/dev/landing/bls_pr/pr.data.1.AllData/pr.data.1.AllData__20260909T163000Z"


def test_build_landing_path_bls_series():
    path = build_landing_path(
        "/Volumes/rearc_ingest/dev/landing", "bls_pr", "pr.series", "20260909T163000Z", "pr.series"
    )
    assert path == "/Volumes/rearc_ingest/dev/landing/bls_pr/pr.series/pr.series__20260909T163000Z"


def test_build_landing_path_datausa_population():
    path = build_landing_path(
        "/Volumes/rearc_ingest/dev/landing",
        "datausa_population",
        "population",
        "20260909T163000Z",
        "population.json",
    )
    assert path == ("/Volumes/rearc_ingest/dev/landing/datausa_population/population/population.json__20260909T163000Z")


class _FakeFiles:
    def __init__(self):
        self.created_directories = []
        self.uploads = []

    def create_directory(self, directory_path):
        self.created_directories.append(directory_path)

    def upload(self, file_path, contents, overwrite=None):
        self.uploads.append((file_path, contents.read(), overwrite))


class _FakeClient:
    def __init__(self):
        self.files = _FakeFiles()


def test_upload_creates_parent_directory_then_uploads():
    client = _FakeClient()
    upload(client, "/Volumes/rearc_ingest/dev/landing/bls_pr/pr.series/pr.series__20260909T163000Z", b"data")

    assert client.files.created_directories == ["/Volumes/rearc_ingest/dev/landing/bls_pr/pr.series"]
    [(path, content, overwrite)] = client.files.uploads
    assert path == "/Volumes/rearc_ingest/dev/landing/bls_pr/pr.series/pr.series__20260909T163000Z"
    assert content == b"data"
    assert overwrite is True


def test_upload_passes_bytesio_contents():
    client = _FakeClient()
    upload(client, "/Volumes/x/y/z/file.txt", b"hello")
    [(_, contents, _)] = client.files.uploads
    assert isinstance(contents, bytes)
