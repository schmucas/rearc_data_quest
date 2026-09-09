"""Unit tests for src/utils/transform_utils.py.

These use the `spark` fixture from conftest.py, which starts a Databricks
Connect session against serverless compute. That costs DBUs on every CI run, so
keep the number of Spark-backed tests small and deliberate.
"""

import pytest

# Skips the whole module in a checkout without the dev dependencies installed,
# so the pure-Python guardrails in test_bundle_config.py still run.
pytest.importorskip("pyspark")

from src.utils.transform_utils import surrogate_key, with_audit_columns  # noqa: E402


def test_surrogate_key_is_deterministic(spark):
    df = spark.createDataFrame([("A", 1), ("A", 1)], ["k1", "k2"]).withColumn("sk", surrogate_key("k1", "k2"))
    keys = {row.sk for row in df.collect()}
    assert len(keys) == 1
    assert len(keys.pop()) == 64


def test_surrogate_key_separates_ambiguous_values(spark):
    """("AB", "C") and ("A", "BC") must not collide."""
    df = spark.createDataFrame([("AB", "C"), ("A", "BC")], ["k1", "k2"]).withColumn("sk", surrogate_key("k1", "k2"))
    assert len({row.sk for row in df.collect()}) == 2


def test_with_audit_columns_adds_both(spark):
    df = with_audit_columns(spark.createDataFrame([(1,)], ["x"]), source="unit_test")
    assert {"_insert_update_ts", "_source"} <= set(df.columns)
    assert df.collect()[0]._source == "unit_test"
