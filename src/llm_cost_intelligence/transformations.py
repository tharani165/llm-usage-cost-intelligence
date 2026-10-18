"""Bronze metadata, string/timestamp normalisation, and the Silver build orchestration."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, functions as F

from .config import ProjectConfig
from .deduplication import resolve_duplicates
from .schemas import RAW_COLUMNS
from .validation import add_validation_flags, split_valid_invalid

# Columns that define record *content*; batch_id is excluded so that the same
# record re-delivered in a later batch hashes identically.
_HASH_COLUMNS = [c for c in RAW_COLUMNS if c != "batch_id"]
_LOWERCASE_COLUMNS = ["team", "application", "provider", "model", "environment", "status"]


def add_bronze_metadata(df: DataFrame, source_name: str) -> DataFrame:
    """Append lineage columns. Raw columns are left untouched."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in _HASH_COLUMNS]
    return (
        df.withColumn("_record_hash", F.sha2(F.concat_ws("\x1f", *parts), 256))
        .withColumn("_source", F.lit(source_name))
        .withColumn("_bronze_loaded_at", F.current_timestamp())
    )


def _clean_string(column: str, lower: bool) -> Column:
    trimmed = F.trim(F.col(column))
    value = F.lower(trimmed) if lower else trimmed
    return F.when(F.length(trimmed) == 0, F.lit(None)).otherwise(value)


def normalize_events(bronze: DataFrame) -> DataFrame:
    """Trim/lower-case categorical strings, blank -> NULL, parse the timestamp safely."""
    raw_record = F.to_json(F.struct(*[F.col(c) for c in RAW_COLUMNS]))
    out = bronze.withColumn("_raw_record", raw_record)
    out = out.withColumn("request_id", _clean_string("request_id", lower=False))
    for column in _LOWERCASE_COLUMNS:
        out = out.withColumn(column, _clean_string(column, lower=True))
    parsed = F.try_to_timestamp(F.trim(F.col("request_timestamp_raw")))
    return (
        out.withColumnRenamed("request_timestamp", "request_timestamp_raw")
        .withColumn("request_timestamp", parsed)
        .withColumn("request_date", F.to_date("request_timestamp"))
    )


@dataclass
class SilverResult:
    events: DataFrame
    quarantine: DataFrame


def build_silver(bronze: DataFrame, cfg: ProjectConfig) -> SilverResult:
    """Bronze -> (silver events, quarantine). Every Bronze row lands in exactly one of them.

    1. normalise; 2. hard-rule validation -> quarantine (validation_failure);
    3. deterministic duplicate resolution on valid rows -> losers to quarantine (superseded_duplicate).
    """
    flagged = add_validation_flags(normalize_events(bronze), cfg)
    valid, invalid = split_valid_invalid(flagged)
    winners, superseded = resolve_duplicates(valid)

    rejected = invalid.select(
        "request_id", "batch_id", "ingested_at", "_record_hash", "_raw_record",
        F.lit("validation_failure").alias("quarantine_category"),
        F.col("rejection_reasons"),
        F.lit(None).cast("string").alias("winner_record_hash"),
    )
    superseded_rows = superseded.select(
        "request_id", "batch_id", "ingested_at", "_record_hash", "_raw_record",
        F.lit("superseded_duplicate").alias("quarantine_category"),
        F.array(F.lit("superseded_duplicate")).alias("rejection_reasons"),
        "winner_record_hash",
    )
    quarantine = rejected.unionByName(superseded_rows).withColumn("quarantined_at", F.current_timestamp())

    events = winners.select(
        "request_id", "request_timestamp", "request_date", "team", "application", "provider",
        "model", "input_tokens", "output_tokens", "latency_ms", "status", "retry_count",
        "cache_hit", "environment", "ingested_at", "batch_id", "warnings",
        "duplicate_group_size", "_record_hash",
    )
    return SilverResult(events=events, quarantine=quarantine)
