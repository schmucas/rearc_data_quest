"""Bronze: raw landing for the DataUSA population response.

- Landed as one VARIANT column via singleVariantColumn.
- No _rescued_data column -- nothing is parsed, so nothing to rescue.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"
INGEST_CATALOG = f"{CATALOG_PREFIX}_ingest"

SOURCE_PATH = f"/Volumes/{INGEST_CATALOG}/{ENV}/landing/datausa_population/population"
TARGET_TABLE = f"{CATALOG}.bronze.population"

# pipelines.reset.allowed=false blocks full-refresh resets: a full refresh
# would reprocess every landed file from scratch and discard Auto Loader's
# incremental state, which is never what we want for append-only bronze.
POPULATION_TABLE_PROPERTIES = {
    "pipelines.reset.allowed": "false",
}


@dp.table(
    name=TARGET_TABLE,
    comment="Raw DataUSA population response, landed as one VARIANT column per file version.",
    table_properties=POPULATION_TABLE_PROPERTIES,
    cluster_by_auto=True,
)
@dp.expect("value_present", "value IS NOT NULL")
def population():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("singleVariantColumn", "value")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
