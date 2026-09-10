"""Silver: pr_seasonal dimension, deduplicated on seasonal_code.

- Trims every string column; SCD Type 1 CDC keyed on seasonal_code.
- Bronze is capitalized (Seasonal_code/_text), the only lookup table that
  breaks the lowercase convention -- renamed here for consistency.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_or_drop("valid_seasonal_code", "seasonal_code IS NOT NULL")
def _pr_seasonal_typed():
    """Trim and rename bronze.pr_seasonal's columns for the CDC flow below.

    Returns:
        Streaming DataFrame of trimmed seasonal rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_seasonal")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    return df.select(
        F.trim("Seasonal_code").alias("seasonal_code"),
        F.trim("Seasonal_text").alias("seasonal_text"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_seasonal")

dp.create_auto_cdc_flow(
    target="pr_seasonal",
    source="_pr_seasonal_typed",
    keys=["seasonal_code"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
