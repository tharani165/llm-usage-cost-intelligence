"""Hard validation rules (quarantine) and soft warnings (kept, but flagged)."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F

from .config import ProjectConfig
from .schemas import VALID_STATUSES


def _true(condition: Column) -> Column:
    """NULL-safe: a rule that evaluates to NULL is treated as not triggered."""
    return F.coalesce(condition, F.lit(False))


def hard_rules() -> dict[str, Column]:
    """Rules that quarantine a record. Blank strings are already NULL after normalize_events."""
    null = lambda c: F.col(c).isNull()
    return {
        "missing_request_id": null("request_id"),
        "missing_team": null("team"),
        "missing_application": null("application"),
        "missing_provider": null("provider"),
        "missing_model": null("model"),
        "missing_environment": null("environment"),
        "invalid_timestamp": null("request_timestamp"),
        "missing_input_tokens": null("input_tokens"),
        "negative_input_tokens": _true(F.col("input_tokens") < 0),
        "missing_output_tokens": null("output_tokens"),
        "negative_output_tokens": _true(F.col("output_tokens") < 0),
        "missing_latency": null("latency_ms"),
        "negative_latency": _true(F.col("latency_ms") < 0),
        "invalid_status": null("status") | _true(~F.col("status").isin(*VALID_STATUSES)),
        "invalid_retry_count": null("retry_count") | _true(F.col("retry_count") < 0),
        "missing_cache_hit": null("cache_hit"),
        "missing_ingested_at": null("ingested_at"),
    }


def soft_rules(cfg: ProjectConfig) -> dict[str, Column]:
    lag_seconds = F.col("ingested_at").cast("long") - F.col("request_timestamp").cast("long")
    return {
        "high_latency": _true(F.col("latency_ms") >= cfg.high_latency_ms),
        "high_input_tokens": _true(F.col("input_tokens") >= cfg.high_input_tokens),
        "high_output_tokens": _true(F.col("output_tokens") >= cfg.high_output_tokens),
        "high_retry_count": _true(F.col("retry_count") >= cfg.high_retry_count),
        "late_arrival": _true(lag_seconds > cfg.late_arrival_hours * 3600),
    }


def _reason_array(rules: dict[str, Column]) -> Column:
    triggered = F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    return F.filter(triggered, lambda x: x.isNotNull())  # when() without otherwise yields NULLs; drop them


def add_validation_flags(df: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Adds `rejection_reasons` (hard failures) and `warnings` (soft; the record stays valid)."""
    return (
        df.withColumn("rejection_reasons", _reason_array(hard_rules()))
        .withColumn("warnings", _reason_array(soft_rules(cfg)))
    )


def split_valid_invalid(flagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    has_failure = F.size("rejection_reasons") > 0
    return flagged.filter(~has_failure), flagged.filter(has_failure)
