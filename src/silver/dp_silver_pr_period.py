"""Silver: pr_period dimension, deduplicated on period.

- Trims every string column; SCD Type 1 CDC keyed on period.
- Q05 (BLS's annual average) is a legitimate row here; gold excludes it.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_or_drop("valid_period", "period IS NOT NULL")
def _pr_period_typed():
    """Trim bronze.pr_period's columns for the CDC flow below.

    Returns:
        Streaming DataFrame of trimmed period rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_period")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    return df.select(
        F.trim("period").alias("period"),
        F.trim("period_abbr").alias("period_abbr"),
        F.trim("period_name").alias("period_name"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_period")

dp.create_auto_cdc_flow(
    target="pr_period",
    source="_pr_period_typed",
    keys=["period"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
