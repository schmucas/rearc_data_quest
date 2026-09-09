"""Gold: daily activity by item category.

Conforms the silver event stream to the item master and aggregates. A
materialized view rather than a streaming table: gold is recomputed from silver,
and letting the pipeline own the refresh is simpler than maintaining state.

Gold targets are fully qualified, since the pipeline's bare-name default is the
silver schema.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"

SOURCE_EVENTS = "dp_events"  # bare: silver
SOURCE_ITEMS = "dp_items"  # bare: silver
TARGET_TABLE = "dp_agg_daily_category"
TARGET_FQN = f"{CATALOG}.gold.{TARGET_TABLE}"


@dp.materialized_view(
    name=TARGET_FQN,
    comment="One row per event_date and item category: event volume, units, and revenue.",
    cluster_by_auto=True,
    schema="""
        event_date DATE NOT NULL,
        category STRING NOT NULL,
        event_count BIGINT,
        distinct_items BIGINT,
        total_quantity BIGINT,
        total_amount DECIMAL(18, 2),
        CONSTRAINT dp_agg_daily_category_pk PRIMARY KEY (event_date, category)
    """,
)
def dp_agg_daily_category():
    """Join events to the item master and aggregate to date and category.

    Returns:
        Batch DataFrame, one row per (event_date, category).
    """
    events_df = spark.read.table(SOURCE_EVENTS)
    items_df = spark.read.table(SOURCE_ITEMS).select("item_id", "category")

    return (
        events_df.join(items_df, on="item_id", how="left")
        .groupBy("event_date", "category")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("item_id").alias("distinct_items"),
            F.sum("quantity").cast("bigint").alias("total_quantity"),
            F.sum("amount").cast("decimal(18,2)").alias("total_amount"),
        )
    )
