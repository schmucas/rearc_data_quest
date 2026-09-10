"""Silver: pr_series dimension, deduplicated on series_id.

- SCD Type 1 CDC keyed on series_id.
- series_id is the only padded column here -- names are stripped
  generically rather than hardcoded.
- Dimension codes (sector_code, class_code, measure_code, duration_code,
  seasonal) stay STRING -- identifiers, not quantities, and measure_code's
  leading zero ("01") would be lost by an INT cast.
- base_year/begin_year/end_year are cast to INT ("-"/blank -> NULL for
  base_year, BLS's N/A sentinel).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL")
def _pr_series_typed():
    """Trim and type bronze.pr_series's columns for the CDC flow below.

    Returns:
        Streaming DataFrame of typed series rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_series")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    trimmed_base_year = F.trim(F.col("base_year"))
    return df.select(
        F.trim("series_id").alias("series_id"),
        F.trim("sector_code").alias("sector_code"),
        F.trim("class_code").alias("class_code"),
        F.trim("measure_code").alias("measure_code"),
        F.trim("duration_code").alias("duration_code"),
        F.trim("seasonal").alias("seasonal"),
        F.when(trimmed_base_year.isin("-", ""), None).otherwise(trimmed_base_year.cast("int")).alias("base_year"),
        F.trim("footnote_codes").alias("footnote_codes"),
        F.trim("begin_year").cast("int").alias("begin_year"),
        F.trim("begin_period").alias("begin_period"),
        F.trim("end_year").cast("int").alias("end_year"),
        F.trim("end_period").alias("end_period"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_series")

dp.create_auto_cdc_flow(
    target="pr_series",
    source="_pr_series_typed",
    keys=["series_id"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
