"""Gold: best year per series, by summed Q01-Q04 value.

- One row per series_id: best_year, summed_value, and a human-readable
  label joined out from pr_series's dimension codes.
- Q05 (BLS's annual average) is excluded -- it would inflate every total.
- Ties broken by earliest year (ORDER BY summed_value DESC, year ASC).
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"

LABEL_EXPR = F.concat_ws(
    ", ",
    F.col("sector_name"),
    F.concat(F.col("measure_text"), F.lit(" ("), F.col("duration_text"), F.lit(")")),
    F.col("class_text"),
    F.col("seasonal_text"),
)


def _best_year_per_series_pyspark():
    """Best year per series via the DataFrame API (primary).

    Returns:
        Batch DataFrame with one row per series_id: series_id, the label
        dimension columns, a composed label, best_year, and summed_value.
    """
    quarterly = (
        spark.read.table("pr_data")
        .filter(F.col("period") != "Q05")
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("summed_value"))
    )

    rank_window = Window.partitionBy("series_id").orderBy(F.col("summed_value").desc(), F.col("year").asc())
    best = (
        quarterly.withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") == 1)
        .select("series_id", F.col("year").alias("best_year"), "summed_value")
    )

    series = spark.read.table("pr_series").select(
        "series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal"
    )
    sector = spark.read.table("pr_sector").select("sector_code", "sector_name")
    measure = spark.read.table("pr_measure").select("measure_code", "measure_text")
    series_class = spark.read.table("pr_class").select("class_code", "class_text")
    duration = spark.read.table("pr_duration").select("duration_code", "duration_text")
    seasonal = spark.read.table("pr_seasonal").select("seasonal_code", "seasonal_text")

    labels = (
        series.join(sector, "sector_code", "left")
        .join(measure, "measure_code", "left")
        .join(series_class, "class_code", "left")
        .join(duration, "duration_code", "left")
        .join(seasonal, series["seasonal"] == seasonal["seasonal_code"], "left")
        .select("series_id", "sector_name", "measure_text", "class_text", "duration_text", "seasonal_text")
    )

    return (
        best.join(labels, "series_id", "left")
        .withColumn("label", LABEL_EXPR)
        .select(
            "series_id",
            "sector_name",
            "measure_text",
            "class_text",
            "duration_text",
            "seasonal_text",
            "label",
            "best_year",
            "summed_value",
        )
    )


def _best_year_per_series_sql():
    """Best year per series via Spark SQL (documented alternative).

    Returns:
        Batch DataFrame with the same rows, same column names and types, as
        _best_year_per_series_pyspark.
    """
    return spark.sql("""
        WITH quarterly AS (
            SELECT series_id, year, SUM(value) AS summed_value
            FROM pr_data
            WHERE period != 'Q05'
            GROUP BY series_id, year
        ),
        ranked AS (
            SELECT
                series_id,
                year AS best_year,
                summed_value,
                ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY summed_value DESC, year ASC) AS rn
            FROM quarterly
        ),
        best AS (
            SELECT series_id, best_year, summed_value FROM ranked WHERE rn = 1
        ),
        labels AS (
            SELECT
                s.series_id,
                sec.sector_name,
                mea.measure_text,
                cls.class_text,
                dur.duration_text,
                sea.seasonal_text
            FROM pr_series s
            LEFT JOIN pr_sector sec ON s.sector_code = sec.sector_code
            LEFT JOIN pr_measure mea ON s.measure_code = mea.measure_code
            LEFT JOIN pr_class cls ON s.class_code = cls.class_code
            LEFT JOIN pr_duration dur ON s.duration_code = dur.duration_code
            LEFT JOIN pr_seasonal sea ON s.seasonal = sea.seasonal_code
        )
        SELECT
            b.series_id,
            l.sector_name,
            l.measure_text,
            l.class_text,
            l.duration_text,
            l.seasonal_text,
            CONCAT_WS(
                ', ',
                l.sector_name,
                CONCAT(l.measure_text, ' (', l.duration_text, ')'),
                l.class_text,
                l.seasonal_text
            ) AS label,
            b.best_year,
            b.summed_value
        FROM best b
        LEFT JOIN labels l ON b.series_id = l.series_id
    """)


# PySpark is primary.
@dp.materialized_view(
    name=f"{CATALOG}.gold.best_year_per_series",
    comment="Best-performing year per series, summed over Q01-Q04, with a human-readable label.",
)
def best_year_per_series():
    return _best_year_per_series_pyspark()
