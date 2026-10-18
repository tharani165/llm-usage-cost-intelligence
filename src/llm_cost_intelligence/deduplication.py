"""Deterministic duplicate resolution on the request key."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

REQUEST_KEY = "request_id"


def resolve_duplicates(valid: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Keep one row per request_id; return (winners, superseded).

    Winner policy (a total order, so the result never depends on partitioning or row order):
      1. latest `ingested_at`    - a later delivery is a correction/refresh of the same request
      2. latest `batch_id`       - tie-break between deliveries stamped with the same time
      3. greatest `_record_hash` - last resort; identical records hash identically
    Only rows that already passed hard validation compete, so an invalid correction can
    never replace a valid earlier record. Distinct request_ids are never merged, so a retry
    that was logged as its own request is preserved.
    """
    order = Window.partitionBy(REQUEST_KEY).orderBy(
        F.col("ingested_at").desc(), F.col("batch_id").desc(), F.col("_record_hash").desc()
    )
    group = Window.partitionBy(REQUEST_KEY)
    ranked = (
        valid.withColumn("_rank", F.row_number().over(order))
        .withColumn("duplicate_group_size", F.count(F.lit(1)).over(group))
        .withColumn("winner_record_hash", F.first("_record_hash").over(order))
    )
    winners = ranked.filter(F.col("_rank") == 1).drop("_rank")
    superseded = ranked.filter(F.col("_rank") > 1).drop("_rank")
    return winners, superseded
