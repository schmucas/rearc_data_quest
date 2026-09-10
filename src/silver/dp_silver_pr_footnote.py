"""Silver: pr_footnote dimension, deduplicated on footnote_code.

- Trims every string column.
- SCD Type 1 CDC keyed on footnote_code.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_or_drop("valid_footnote_code", "footnote_code IS NOT NULL")
def _pr_footnote_typed():
    """Trim bronze.pr_footnote's columns for the CDC flow below.

    Returns:
        Streaming DataFrame of trimmed footnote rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_footnote")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    return df.select(
        F.trim("footnote_code").alias("footnote_code"),
        F.trim("footnote_text").alias("footnote_text"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_footnote")

dp.create_auto_cdc_flow(
    target="pr_footnote",
    source="_pr_footnote_typed",
    keys=["footnote_code"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
