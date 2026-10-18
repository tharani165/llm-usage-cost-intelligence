"""Idempotent batch loading: a batch can be replayed without duplicating Bronze rows."""
from __future__ import annotations

from pyspark.sql import DataFrame


def rows_not_yet_loaded(existing_bronze: DataFrame | None, incoming: DataFrame) -> DataFrame:
    """Insert-only merge semantics: keep incoming rows whose (batch_id, _record_hash) is not in Bronze.

    Equivalent to `MERGE ... WHEN NOT MATCHED THEN INSERT`, but written as an anti-join so it
    behaves identically on Databricks and in local tests. Exact duplicates *inside* one batch
    are kept (Bronze preserves what arrived); replaying the batch inserts nothing.
    """
    if existing_bronze is None:
        return incoming
    keys = ["batch_id", "_record_hash"]
    return incoming.join(existing_bronze.select(*keys).distinct(), on=keys, how="left_anti")
