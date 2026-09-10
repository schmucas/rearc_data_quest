"""Bronze: raw landing for the DataUSA population response.

Landed as a single VARIANT column via Auto Loader's singleVariantColumn --
Unlike the BLS tables, there is no
_rescued_data column here: nothing is parsed into individual columns.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"
INGEST_CATALOG = f"{CATALOG_PREFIX}_ingest"

SOURCE_PATH = f"/Volumes/{INGEST_CATALOG}/{ENV}/landing/datausa_population/population"
TARGET_TABLE = f"{CATALOG}.bronze.population"


@dp.table(
    name=TARGET_TABLE,
    comment="Raw DataUSA population response, landed as one VARIANT column per file version.",
    cluster_by_auto=True,
)
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
