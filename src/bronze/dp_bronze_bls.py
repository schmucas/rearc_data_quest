"""Bronze: raw landing for BLS pr.* tab-delimited files.

One Auto Loader streaming table per dataset, generated from BLS_SOURCES below
rather than as ten near-identical functions. Every table is append-only and
structurally validated only: a non-null natural key and no rescued data.
Each table carries exactly three audit columns -- _ingested_at, _source_file,
and _rescued_data (the last populated by Auto Loader itself via the rescue
option, not added explicitly here). Typing, trimming, and dedup are silver's
job, not bronze's.

Runs inside the declarative pipeline. `spark` is the pipeline's global
session and this file is never imported.
"""

from dataclasses import dataclass

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"
INGEST_CATALOG = f"{CATALOG_PREFIX}_ingest"

LANDING_BASE = f"/Volumes/{INGEST_CATALOG}/{ENV}/landing/bls_pr"

# BLS pads the `series_id` header text (and every value) to a fixed width of
# 17 characters -- verified live against download.bls.gov/pub/time.series/pr/
# for pr.series, pr.data.0.Current, and pr.data.1.AllData. The CSV reader
# takes header text verbatim as the column name, so the real column is this,
# not "series_id". Declared once here rather than peeked at runtime.
SERIES_ID_COL = "series_id".ljust(17)


@dataclass(frozen=True)
class BlsSource:
    """One BLS bronze ingestion target: a landing-volume directory -> one table.

    Attributes:
        dataset: Landing volume's dataset segment, e.g. "pr.series" -- matches
            the original BLS filename verbatim (dots and all).
        table: Target bronze table name; dots in `dataset` become underscores
            so this is a legal identifier.
        key_expr: SQL boolean expression for the natural-key-not-null
            expectation. Backtick-quote any column name that needs it.
        comment: Human-readable table comment.
    """

    dataset: str
    table: str
    key_expr: str
    comment: str


BLS_SOURCES: list[BlsSource] = [
    BlsSource(
        dataset="pr.data.0.Current",
        table="pr_data_0_current",
        key_expr=f"`{SERIES_ID_COL}` IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL",
        comment="Raw BLS pr.data.0.Current: current-window series values, one row per (series_id, year, period).",
    ),
    BlsSource(
        dataset="pr.data.1.AllData",
        table="pr_data_1_alldata",
        key_expr=f"`{SERIES_ID_COL}` IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL",
        comment="Raw BLS pr.data.1.AllData: full-history series values, one row per (series_id, year, period).",
    ),
    BlsSource(
        dataset="pr.series",
        table="pr_series",
        key_expr=f"`{SERIES_ID_COL}` IS NOT NULL",
        comment="Raw BLS pr.series: series metadata dimension, one row per series_id.",
    ),
    BlsSource(
        dataset="pr.sector",
        table="pr_sector",
        key_expr="sector_code IS NOT NULL",
        comment="Raw BLS pr.sector: sector code dimension.",
    ),
    BlsSource(
        dataset="pr.measure",
        table="pr_measure",
        key_expr="measure_code IS NOT NULL",
        comment="Raw BLS pr.measure: measure code dimension.",
    ),
    BlsSource(
        dataset="pr.class",
        table="pr_class",
        key_expr="class_code IS NOT NULL",
        comment="Raw BLS pr.class: class code dimension.",
    ),
    BlsSource(
        dataset="pr.duration",
        table="pr_duration",
        key_expr="duration_code IS NOT NULL",
        comment="Raw BLS pr.duration: duration code dimension.",
    ),
    BlsSource(
        dataset="pr.seasonal",
        table="pr_seasonal",
        key_expr="`Seasonal_code` IS NOT NULL",
        comment=(
            "Raw BLS pr.seasonal: seasonal-adjustment code dimension. Header is "
            "capital-S `Seasonal_code`, unlike every other *_code column in this "
            "source -- verified live against BLS."
        ),
    ),
    BlsSource(
        dataset="pr.period",
        table="pr_period",
        key_expr="period IS NOT NULL",
        comment="Raw BLS pr.period: period code dimension.",
    ),
    BlsSource(
        dataset="pr.footnote",
        table="pr_footnote",
        key_expr="footnote_code IS NOT NULL",
        comment="Raw BLS pr.footnote: footnote code dimension.",
    ),
]


def _make_bronze_table(cfg: BlsSource):
    """Build and register one bronze Auto Loader table for cfg.

    Called once per entry in BLS_SOURCES from the loop below. Each call gets
    its own stack frame, so the @dp.table-decorated closure captures *this
    call's* cfg -- never a loop variable shared across iterations, which is
    the late-binding bug a bare `for cfg in ...: @dp.table ...` would hit.

    Args:
        cfg: One row of BLS_SOURCES describing a single dataset.

    Returns:
        The decorated table function. The pipeline discovers the table via
        the decorator's side effect, not this return value.
    """
    table_fqn = f"{CATALOG}.bronze.{cfg.table}"
    source_path = f"{LANDING_BASE}/{cfg.dataset}"

    @dp.table(name=table_fqn, comment=cfg.comment)
    @dp.expect_all(
        {
            "natural_key_not_null": cfg.key_expr,
            "no_rescued_data": "_rescued_data IS NULL",
        }
    )
    def _bronze_table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("sep", "\t")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .load(source_path)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    return _bronze_table


for _cfg in BLS_SOURCES:
    _make_bronze_table(_cfg)
