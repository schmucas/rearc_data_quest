"""Small, importable helpers shared by job notebooks and tests.

Anything here must be pure PySpark Column / DataFrame logic with no global
`spark` and no side effects, so pytest can exercise it directly. Declarative
`@dp.*` files deliberately do NOT import from here (see CLAUDE.md): they restate
what they need so the pipeline has no external dependency to resolve.

If a helper outgrows this file, it belongs in a separate utils wheel repo.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def surrogate_key(*columns: str) -> Column:
    """Build a deterministic surrogate key from business key columns.

    Nulls are normalised to an empty string so a null key never collides with a
    different null key, and the parts are joined by a separator that cannot
    appear in a value.

    Args:
        *columns: Names of the business key columns, in a fixed order.

    Returns:
        A Column of 64-character SHA-256 hex digests.
    """
    if not columns:
        raise ValueError("surrogate_key requires at least one column")
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("‖", *parts), 256)


def with_audit_columns(df, source: str):
    """Stamp a DataFrame with the standard audit columns.

    Args:
        df: The DataFrame to stamp.
        source: Identifier for where the rows came from, e.g. a table name.

    Returns:
        The DataFrame with `_insert_update_ts` and `_source` appended.
    """
    return df.withColumn("_insert_update_ts", F.current_timestamp()).withColumn("_source", F.lit(source))
