"""Silver: pr_measure dimension, deduplicated on measure_code.

- SCD Type 1 CDC keyed on measure_code.
- Bronze lands everything as STRING (Auto Loader type inference is off);
  display_level/sort_sequence are cast to INT, selectable to BOOLEAN.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_or_drop("valid_measure_code", "measure_code IS NOT NULL")
def _pr_measure_typed():
    """Trim and type bronze.pr_measure's columns for the CDC flow below.

    Returns:
        Streaming DataFrame of typed measure rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_measure")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    selectable = F.trim(F.col("selectable"))
    return df.select(
        F.trim("measure_code").alias("measure_code"),
        F.trim("measure_text").alias("measure_text"),
        F.trim("display_level").cast("int").alias("display_level"),
        F.when(selectable == "T", True).when(selectable == "F", False).alias("selectable"),
        F.trim("sort_sequence").cast("int").alias("sort_sequence"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_measure")

dp.create_auto_cdc_flow(
    target="pr_measure",
    source="_pr_measure_typed",
    keys=["measure_code"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
