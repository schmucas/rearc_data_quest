"""Bronze: raw landing for BLS pr.* tab-delimited files.

One Auto Loader streaming table per dataset, with schema merge and rescued
data column.
Each table carries exactly three audit columns -- _ingested_at, _source_file,
and _rescued_data (the last populated by Auto Loader itself via the rescue
option).
"""

from dataclasses import dataclass

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"
INGEST_CATALOG = f"{CATALOG_PREFIX}_ingest"

LANDING_BASE = f"/Volumes/{INGEST_CATALOG}/{ENV}/landing/bls_pr"

# BLS pads some header text with spaces (e.g. "series_id" + trailing spaces),
# and Delta rejects spaces in column names outright regardless of table
# content -- DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES. Column Mapping lets
# Delta store an arbitrary display name against an internal physical one, so
# the padded header lands untouched rather than needing to be renamed.
BLS_TABLE_PROPERTIES = {
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}


@dataclass(frozen=True)
class BlsSource:
    """One BLS bronze ingestion target: a landing-volume directory -> one table.

    Attributes:
        dataset: Landing volume's dataset segment, e.g. "pr.series" -- matches
            the original BLS filename verbatim (dots and all).
        table: Target bronze table name; dots in `dataset` become underscores
            so this is a legal identifier.
        comment: Human-readable table comment.
    """

    dataset: str
    table: str
    comment: str


BLS_SOURCES: list[BlsSource] = [
    BlsSource(
        dataset="pr.data.0.Current",
        table="pr_data_0_current",
        comment="Raw BLS pr.data.0.Current: current-window series values, one row per (series_id, year, period).",
    ),
    BlsSource(
        dataset="pr.data.1.AllData",
        table="pr_data_1_alldata",
        comment="Raw BLS pr.data.1.AllData: full-history series values, one row per (series_id, year, period).",
    ),
    BlsSource(
        dataset="pr.series",
        table="pr_series",
        comment="Raw BLS pr.series: series metadata dimension, one row per series_id.",
    ),
    BlsSource(
        dataset="pr.sector",
        table="pr_sector",
        comment="Raw BLS pr.sector: sector code dimension.",
    ),
    BlsSource(
        dataset="pr.measure",
        table="pr_measure",
        comment="Raw BLS pr.measure: measure code dimension.",
    ),
    BlsSource(
        dataset="pr.class",
        table="pr_class",
        comment="Raw BLS pr.class: class code dimension.",
    ),
    BlsSource(
        dataset="pr.duration",
        table="pr_duration",
        comment="Raw BLS pr.duration: duration code dimension.",
    ),
    BlsSource(
        dataset="pr.seasonal",
        table="pr_seasonal",
        comment="Raw BLS pr.seasonal: seasonal-adjustment code dimension.",
    ),
    BlsSource(
        dataset="pr.period",
        table="pr_period",
        comment="Raw BLS pr.period: period code dimension.",
    ),
    BlsSource(
        dataset="pr.footnote",
        table="pr_footnote",
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

    @dp.table(name=table_fqn, comment=cfg.comment, table_properties=BLS_TABLE_PROPERTIES)
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
