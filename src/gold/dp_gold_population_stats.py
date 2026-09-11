"""Gold: population summary statistics, 2013-2018 inclusive.

- One row: mean, sample/population stddev, and year_count.
- Window is fully populated -- the 2020 gap falls outside it.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"

START_YEAR = 2013
END_YEAR = 2018


def _population_stats_pyspark():
    """Population statistics via the DataFrame API (primary).

    Returns:
        Batch DataFrame with one row: year_count, population_mean,
        population_stddev_samp, population_stddev_pop.
    """
    return (
        spark.read.table(f"{CATALOG}.silver.population")
        .filter((F.col("year") >= START_YEAR) & (F.col("year") <= END_YEAR))
        .agg(
            F.count("year").alias("year_count"),
            F.mean("population").alias("population_mean"),
            F.stddev_samp("population").alias("population_stddev_samp"),
            F.stddev_pop("population").alias("population_stddev_pop"),
        )
    )


def _population_stats_sql():
    """Population statistics via Spark SQL (documented alternative).

    Returns:
        Batch DataFrame with the same one row, same column names and types,
        as _population_stats_pyspark.
    """
    return spark.sql(f"""
        SELECT
            COUNT(year) AS year_count,
            MEAN(population) AS population_mean,
            STDDEV_SAMP(population) AS population_stddev_samp,
            STDDEV_POP(population) AS population_stddev_pop
        FROM {CATALOG}.silver.population
        WHERE year BETWEEN {START_YEAR} AND {END_YEAR}
    """)


# PySpark is primary. Report population_stddev_pop -- 2013-2018 is the exact
# set the question asks about, not a sample used to estimate a larger
# population of years.
@dp.materialized_view(
    name=f"{CATALOG}.gold.population_stats",
    comment="Mean and standard deviation of US population, 2013-2018.",
)
def population_stats():
    return _population_stats_pyspark()
