"""Silver: pr_class dimension, deduplicated on class_code.

- SCD Type 1 CDC keyed on class_code.
- Bronze lands everything as STRING (Auto Loader type inference is off);
  display_level/sort_sequence are cast to INT, selectable to BOOLEAN.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_or_drop("valid_class_code", "class_code IS NOT NULL")
def _pr_class_typed():
    """Trim and type bronze.pr_class's columns for the CDC flow below.

    Returns:
        Streaming DataFrame of typed class rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_class")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    selectable = F.trim(F.col("selectable"))
    return df.select(
        F.trim("class_code").alias("class_code"),
        F.trim("class_text").alias("class_text"),
        F.trim("display_level").cast("int").alias("display_level"),
        F.when(selectable == "T", True).when(selectable == "F", False).alias("selectable"),
        F.trim("sort_sequence").cast("int").alias("sort_sequence"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_class")

dp.create_auto_cdc_flow(
    target="pr_class",
    source="_pr_class_typed",
    keys=["class_code"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
