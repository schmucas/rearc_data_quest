"""Gold: every year's summed Q01-Q04 value per series, with population.

- One row per (series_id, year): summed_value for that year, is_best_year
  (true for the single highest-summed year per series), a human-readable
  label joined out from pr_series's dimension codes, and that year's US
  population.
- Q05 (BLS's annual average) is excluded -- it would inflate every total.
- Ties broken by earliest year (ORDER BY summed_value DESC, year ASC).
- Filter to is_best_year for the one-row-per-series best-year answer; the
  unfiltered view exists so a single series' full history can be charted.
- LEFT join population -- it only covers 2013-2024 (minus 2020), so most
  years have no match; an inner join would drop most of the table.
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


def _agg_value_per_year_pyspark():
    """Every year's summed value per series, with population, via the DataFrame API (documented alternative).

    Returns:
        Batch DataFrame with the same rows, same column names and types, as
        _agg_value_per_year_sql.
    """
    quarterly = (
        spark.read.table(f"{CATALOG}.silver.pr_data")
        .filter(F.col("period") != "Q05")
        .groupBy("series_id", "year")
        .agg(F.round(F.sum("value"), 1).alias("summed_value"))
    )

    rank_window = Window.partitionBy("series_id").orderBy(F.col("summed_value").desc(), F.col("year").asc())
    ranked = quarterly.withColumn("is_best_year", F.row_number().over(rank_window) == 1)

    series = spark.read.table(f"{CATALOG}.silver.pr_series").select(
        "series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal"
    )
    sector = spark.read.table(f"{CATALOG}.silver.pr_sector").select("sector_code", "sector_name")
    measure = spark.read.table(f"{CATALOG}.silver.pr_measure").select("measure_code", "measure_text")
    series_class = spark.read.table(f"{CATALOG}.silver.pr_class").select("class_code", "class_text")
    duration = spark.read.table(f"{CATALOG}.silver.pr_duration").select("duration_code", "duration_text")
    seasonal = spark.read.table(f"{CATALOG}.silver.pr_seasonal").select("seasonal_code", "seasonal_text")

    labels = (
        series.join(sector, "sector_code", "left")
        .join(measure, "measure_code", "left")
        .join(series_class, "class_code", "left")
        .join(duration, "duration_code", "left")
        .join(seasonal, series["seasonal"] == seasonal["seasonal_code"], "left")
        .select("series_id", "sector_name", "measure_text", "class_text", "duration_text", "seasonal_text")
    )

    population = spark.read.table(f"{CATALOG}.silver.population").select("year", "population")

    return (
        ranked.join(labels, "series_id", "left")
        .withColumn("label", LABEL_EXPR)
        .join(population, "year", "left")
        .select(
            "series_id",
            "sector_name",
            "measure_text",
            "class_text",
            "duration_text",
            "seasonal_text",
            "label",
            "year",
            "summed_value",
            "is_best_year",
            "population",
        )
    )


def _agg_value_per_year_sql():
    """Every year's summed value per series, with population, via Spark SQL (primary).

    Returns:
        Batch DataFrame with one row per (series_id, year): series_id, the
        label dimension columns, a composed label, year, summed_value,
        is_best_year, and population (NULL where population has no row for
        that year).
    """
    return spark.sql(f"""
        WITH quarterly AS (
            SELECT series_id, year, ROUND(SUM(value), 1) AS summed_value
            FROM {CATALOG}.silver.pr_data
            WHERE period != 'Q05'
            GROUP BY series_id, year
        ),
        ranked AS (
            SELECT
                series_id,
                year,
                summed_value,
                ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY summed_value DESC, year ASC) = 1 AS is_best_year
            FROM quarterly
        ),
        labels AS (
            SELECT
                s.series_id,
                sec.sector_name,
                mea.measure_text,
                cls.class_text,
                dur.duration_text,
                sea.seasonal_text
            FROM {CATALOG}.silver.pr_series s
            LEFT JOIN {CATALOG}.silver.pr_sector sec ON s.sector_code = sec.sector_code
            LEFT JOIN {CATALOG}.silver.pr_measure mea ON s.measure_code = mea.measure_code
            LEFT JOIN {CATALOG}.silver.pr_class cls ON s.class_code = cls.class_code
            LEFT JOIN {CATALOG}.silver.pr_duration dur ON s.duration_code = dur.duration_code
            LEFT JOIN {CATALOG}.silver.pr_seasonal sea ON s.seasonal = sea.seasonal_code
        )
        SELECT
            r.series_id,
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
            r.year,
            r.summed_value,
            r.is_best_year,
            p.population
        FROM ranked r
        LEFT JOIN labels l ON r.series_id = l.series_id
        LEFT JOIN {CATALOG}.silver.population p ON r.year = p.year
    """)


# SQL is primary.
@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_value_per_year",
    comment="Every year's summed Q01-Q04 value per series, best year flagged, with population.",
)
def agg_value_per_year():
    return _agg_value_per_year_sql()
