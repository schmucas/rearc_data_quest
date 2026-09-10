"""Silver: pr_data fact table, deduplicated on (series_id, year, period).

- Source: pr_data_1_alldata only -- pr_data_0_current is a strict subset.
- Bronze headers are padded (series_id trailing, value leading); column
  names are stripped generically rather than hardcoded.
- Q05 (BLS's annual average) stays here; gold excludes it when summing.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_all_or_drop(
    {
        "valid_series_id": "series_id IS NOT NULL",
        "valid_year": "year IS NOT NULL",
        "plausible_year": "year BETWEEN 1900 AND year(current_date())",
        "valid_period": "period RLIKE '^Q0[1-5]$'",
        "valid_value": "value IS NOT NULL",
    }
)
def _pr_data_typed():
    """Trim, type, and null-ify bronze.pr_data_1_alldata for the CDC flow below.

    "-"/blank values become NULL, never 0.

    Returns:
        Streaming DataFrame of typed pr_data rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    df = spark.readStream.table(f"{CATALOG}.bronze.pr_data_1_alldata")
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())

    trimmed_value = F.trim(F.col("value"))
    return df.select(
        F.trim("series_id").alias("series_id"),
        F.trim("year").cast("int").alias("year"),
        F.trim("period").alias("period"),
        F.when(trimmed_value.isin("-", ""), None).otherwise(trimmed_value.cast("double")).alias("value"),
        F.trim("footnote_codes").alias("footnote_codes"),
        "_ingested_at",
    )


dp.create_streaming_table(name="pr_data")

dp.create_auto_cdc_flow(
    target="pr_data",
    source="_pr_data_typed",
    keys=["series_id", "year", "period"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
