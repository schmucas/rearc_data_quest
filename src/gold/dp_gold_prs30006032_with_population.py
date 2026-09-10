"""Gold: PRS30006032 Q01 values joined with population, by year.

- One row per year BLS reports Q01, with that year's population.
- LEFT join from BLS -- population only covers 2013-2024 (minus 2020), so
  most years have no match; an inner join would drop most of the answer.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("env")
CATALOG_PREFIX = spark.conf.get("catalog_prefix")
CATALOG = f"{CATALOG_PREFIX}_{ENV}"

SERIES_ID = "PRS30006032"
PERIOD = "Q01"


def _prs30006032_with_population_pyspark():
    """PRS30006032 Q01 values with population via the DataFrame API (primary).

    Returns:
        Batch DataFrame with one row per year: year, value, population
        (NULL where population has no row for that year).
    """
    bls = (
        spark.read.table("pr_data")
        .filter((F.col("series_id") == SERIES_ID) & (F.col("period") == PERIOD))
        .select("year", "value")
    )
    population = spark.read.table("population").select("year", "population")
    return bls.join(population, "year", "left").select("year", "value", "population").orderBy("year")


def _prs30006032_with_population_sql():
    """PRS30006032 Q01 values with population via Spark SQL (documented alternative).

    Returns:
        Batch DataFrame with the same rows, same column names and types, as
        _prs30006032_with_population_pyspark.
    """
    return spark.sql(f"""
        SELECT d.year, d.value, p.population
        FROM pr_data d
        LEFT JOIN population p ON d.year = p.year
        WHERE d.series_id = '{SERIES_ID}' AND d.period = '{PERIOD}'
        ORDER BY d.year
    """)


# PySpark is primary.
@dp.materialized_view(
    name=f"{CATALOG}.gold.prs30006032_with_population",
    comment="PRS30006032 Q01 values by year, left-joined with US population where available.",
)
def prs30006032_with_population():
    return _prs30006032_with_population_pyspark()
