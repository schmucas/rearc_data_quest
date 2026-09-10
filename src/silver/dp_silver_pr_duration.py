"""Silver: pr_duration dimension, deduplicated on duration_code.

- SCD Type 1 CDC keyed on duration_code.
- Bronze lands everything as STRING (Auto Loader type inference is off);
  display_level/sort_sequence are cast to INT, selectable to BOOLEAN.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_or_drop("valid_duration_code", "duration_code IS NOT NULL")
def _pr_duration_typed():
    """Trim and type bronze.pr_duration's columns for the CDC flow below.

    Returns:
        Streaming DataFrame of typed duration rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_duration")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    selectable = F.trim(F.col("selectable"))
    return df.select(
        F.trim("duration_code").alias("duration_code"),
        F.trim("duration_text").alias("duration_text"),
        F.trim("display_level").cast("int").alias("display_level"),
        F.when(selectable == "T", True).when(selectable == "F", False).alias("selectable"),
        F.trim("sort_sequence").cast("int").alias("sort_sequence"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_duration")

dp.create_auto_cdc_flow(
    target="pr_duration",
    source="_pr_duration_typed",
    keys=["duration_code"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
