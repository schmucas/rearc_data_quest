"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import csv
import json
import os
import pathlib
import sys
from contextlib import contextmanager

import pytest

# Databricks Connect is optional at import time: the pure-Python guardrail tests
# in test_bundle_config.py must run in any checkout. Tests that ask for the
# `spark` fixture skip with a clear message when it is missing.
try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    _CONNECT_IMPORTED = True
except ImportError:
    DatabricksSession = WorkspaceClient = SparkSession = None
    _CONNECT_IMPORTED = False


# Set by pytest_configure: False when no workspace is reachable, so the pure
# Python guardrail tests still run locally without credentials.
_SPARK_AVAILABLE = _CONNECT_IMPORTED
_SKIP_REASON = (
    "databricks-connect is not installed; run tests with 'uv run pytest'"
    if not _CONNECT_IMPORTED
    else "no Databricks workspace configured; set DATABRICKS_HOST / DATABRICKS_TOKEN"
)


@pytest.fixture()
def spark():
    """Provide a SparkSession fixture for tests.

    Skips rather than errors when no Databricks workspace is configured, so
    `uv run pytest` works offline for the tests that do not need Spark.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    if not _SPARK_AVAILABLE:
        pytest.skip(_SKIP_REASON)
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    global _SPARK_AVAILABLE
    if not _CONNECT_IMPORTED:
        return
    with _allow_stderr_output(config):
        try:
            _enable_fallback_compute()

            # Initialize Spark session eagerly, so it is available even when
            # SparkSession.builder.getOrCreate() is used. For DB Connect 15+,
            # we validate version compatibility with the remote cluster.
            if hasattr(DatabricksSession.builder, "validateSession"):
                DatabricksSession.builder.validateSession().getOrCreate()
            else:
                DatabricksSession.builder.getOrCreate()
        except Exception as exc:  # noqa: BLE001 - any auth/network failure means no session
            _SPARK_AVAILABLE = False
            print(f"⚠️  no Databricks session ({type(exc).__name__}); Spark tests will skip", file=sys.stderr)
