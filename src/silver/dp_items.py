"""Silver: item master, SCD Type 1.

The change-feed pattern. Bronze holds every version of every row from the
upstream Delta CDF, so the current state is a last-writer-wins upsert keyed on
item_id, sequenced by the source's own change timestamp.

Switch `stored_as_scd_type` to 2 to keep history instead; the flow then adds
`__START_AT` / `__END_AT` and nothing else changes.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"

SOURCE_TABLE = f"{CATALOG}.bronze.items_raw"
TARGET_TABLE = "dp_items"
SOURCE_VIEW = "dp_items_changes"


@dp.temporary_view(name=SOURCE_VIEW)
def dp_items_changes():
    """Cast the bronze change feed to the silver column shape.

    Returns:
        Streaming DataFrame of item change rows carrying the business columns
        plus `_commit_version`, the sequence column the CDC flow orders by.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        # Delta CDF emits update_preimage rows alongside update_postimage; only
        # the post-image is the new state.
        .filter(F.col("_change_type") != "update_preimage")
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("last_updated", F.to_timestamp(F.col("last_updated")))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .select(
            "item_id",
            "item_name",
            "category",
            "unit_price",
            "last_updated",
            "_insert_update_ts",
            "_commit_version",
        )
    )


dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Item master, current state (SCD Type 1), built from the upstream change feed.",
    cluster_by=["item_id"],
    expect_all_or_drop={"valid_item_id": "item_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=TARGET_TABLE,
    source=SOURCE_VIEW,
    keys=["item_id"],
    sequence_by=F.col("_commit_version"),
    stored_as_scd_type=1,
    except_column_list=["_commit_version"],
)
