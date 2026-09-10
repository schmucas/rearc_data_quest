"""Silver: population dimension, deduplicated on (nation_id, year).

- Unpacks the VARIANT value:data array into one row per (nation, year).
- SCD Type 1 CDC keyed on (nation_id, year).
- 2020 is legitimately absent (no ACS estimate that year) -- not backfilled.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"


@dp.temporary_view()
@dp.expect_all_or_drop(
    {
        "valid_nation_id": "nation_id IS NOT NULL",
        "valid_year": "year IS NOT NULL",
    }
)
def _population_typed():
    """Explode and type bronze.population's VARIANT array for the CDC flow below.

    "Nation ID" needs variant_get's bracket-quoted path form (has a space).

    Returns:
        Streaming DataFrame of typed population rows plus _ingested_at, used
        only as the sequence_by column for the flow -- not written to silver.
    """
    items = spark.readStream.table(f"{CATALOG}.bronze.population").select(
        F.explode(F.variant_get("value", "$.data", "array<variant>")).alias("item"),
        "_ingested_at",
    )
    return items.select(
        F.variant_get("item", '$["Nation ID"]', "string").alias("nation_id"),
        F.variant_get("item", "$.Nation", "string").alias("nation"),
        F.variant_get("item", "$.Year", "int").alias("year"),
        F.variant_get("item", "$.Population", "bigint").alias("population"),
        "_ingested_at",
    )


dp.create_streaming_table(name="population")

dp.create_auto_cdc_flow(
    target="population",
    source="_population_typed",
    keys=["nation_id", "year"],
    sequence_by="_ingested_at",
    except_column_list=["_ingested_at"],
)
