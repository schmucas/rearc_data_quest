"""Silver: typed, quality-checked event stream.

The plain-append pattern. Bronze is append-only and events are immutable, so
there is nothing to merge: read the stream, cast it, drop what fails the
expectations, done.

Runs inside the declarative pipeline. `spark` is the pipeline's global session
and this file is never imported.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"

# Fully qualified: bronze sits outside the pipeline's `schema: silver` default.
SOURCE_TABLE = f"{CATALOG}.bronze.events_raw"

# Bare: resolves against the pipeline's schema, which is silver.
TARGET_TABLE = "dp_events"


@dp.table(
    name=TARGET_TABLE,
    comment="Typed event stream, one row per event_id, invalid rows dropped.",
    cluster_by=["event_date", "item_id"],
)
# expect_all_or_drop quarantines nothing: failing rows are dropped and counted
# in the pipeline's data-quality metrics. Use expect_all to warn instead, or
# expect_all_or_fail to stop the run.
@dp.expect_all_or_drop(
    {
        "valid_event_id": "event_id IS NOT NULL",
        "valid_item_id": "item_id IS NOT NULL",
        "positive_quantity": "quantity > 0",
    }
)
def dp_events():
    """Cast the bronze event feed and add the derived event_date.

    Returns:
        Streaming DataFrame of events with typed columns, `event_date`, and
        `_insert_update_ts`.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("_insert_update_ts", F.current_timestamp())
        .select(
            "event_id",
            "item_id",
            "event_type",
            "quantity",
            "amount",
            "event_timestamp",
            "event_date",
            "_insert_update_ts",
        )
    )
