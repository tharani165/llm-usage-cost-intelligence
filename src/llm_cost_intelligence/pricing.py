"""Synthetic pricing configuration: validation, effective-dated lookup, decimal cost maths.

All prices are ILLUSTRATIVE and FICTIONAL. Billing convention (synthetic, documented in
docs/cost_calculation.md):
  * rate_limited -> 'not_billed': cost is an explicit 0 (rejected before processing).
  * every other status is billed on the recorded tokens, success or not.
  * retries: token counts already cover all attempts of the request, so no multiplier.
  * cache hits: billed like any other request (tracked for hit-rate only).
  * no matching price -> 'unpriced': costs are NULL, never 0.
"""
from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DecimalType

from .schemas import MONEY_TYPE, RAW_PRICING_SCHEMA, SILVER_PRICING_SCHEMA

PRICING_COLUMNS = [f.name for f in RAW_PRICING_SCHEMA.fields]
_ONE_MILLION = Decimal(1_000_000)


def load_pricing_csv(path: str | Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as handle:
        return [{c: (row.get(c) or "").strip() for c in PRICING_COLUMNS} for row in csv.DictReader(handle)]


def validate_pricing_rows(rows: list[dict]) -> list[str]:
    """Return human-readable problems (empty list == valid). Checks values, dates and overlaps."""
    errors: list[str] = []
    spans: dict[tuple, list[tuple[date, date]]] = {}
    for n, row in enumerate(rows, start=1):
        label = f"row {n} ({row.get('provider')}/{row.get('model')}/{row.get('pricing_version')})"
        for col in ("provider", "model", "currency", "pricing_version"):
            if not row.get(col):
                errors.append(f"{label}: {col} is required")
        for col in ("input_price_per_million_tokens", "output_price_per_million_tokens"):
            try:
                if Decimal(row.get(col, "")) < 0:
                    errors.append(f"{label}: {col} is negative")
            except InvalidOperation:
                errors.append(f"{label}: {col} is not a number")
        try:
            start = date.fromisoformat(row.get("effective_from", ""))
        except ValueError:
            errors.append(f"{label}: effective_from is not a valid date")
            continue
        try:
            end = date.fromisoformat(row["effective_to"]) if row.get("effective_to") else date.max
        except ValueError:
            errors.append(f"{label}: effective_to is not a valid date")
            continue
        if end < start:
            errors.append(f"{label}: effective_to is before effective_from")
            continue
        spans.setdefault((row["provider"], row["model"]), []).append((start, end))
    for (provider, model), intervals in spans.items():
        intervals.sort()
        for (_, prev_end), (next_start, _) in zip(intervals, intervals[1:]):
            if next_start <= prev_end:
                errors.append(f"{provider}/{model}: overlapping effective-date ranges")
    return errors


def pricing_dataframe(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Typed Silver pricing table from validated rows; raises if the config is invalid."""
    errors = validate_pricing_rows(rows)
    if errors:
        raise ValueError("Invalid pricing configuration:\n  " + "\n  ".join(errors))
    typed = [
        (r["provider"].lower(), r["model"].lower(),
         Decimal(r["input_price_per_million_tokens"]), Decimal(r["output_price_per_million_tokens"]),
         r["currency"], r["pricing_version"], date.fromisoformat(r["effective_from"]),
         date.fromisoformat(r["effective_to"]) if r["effective_to"] else None)
        for r in rows
    ]
    return spark.createDataFrame(typed, SILVER_PRICING_SCHEMA)


def attach_pricing(events: DataFrame, pricing: DataFrame) -> DataFrame:
    """Left-join the price in force on the request date. Unmatched events keep NULL prices."""
    p = pricing.alias("p")
    e = events.alias("e")
    condition = (
        (F.col("e.provider") == F.col("p.provider"))
        & (F.col("e.model") == F.col("p.model"))
        & (F.col("e.request_date") >= F.col("p.effective_from"))
        & (F.col("p.effective_to").isNull() | (F.col("e.request_date") <= F.col("p.effective_to")))
    )
    return e.join(p, condition, "left").select(
        "e.*", "p.input_price_per_million_tokens", "p.output_price_per_million_tokens",
        "p.currency", "p.pricing_version",
    )


def calculate_costs(priced_join: DataFrame) -> DataFrame:
    """Add billing_status and DECIMAL(18,8) input/output/total cost columns."""
    has_price = F.col("input_price_per_million_tokens").isNotNull()
    status = (
        F.when(F.col("status") == "rate_limited", "not_billed")
        .when(has_price, "priced")
        .otherwise("unpriced")
    )
    million = F.lit(_ONE_MILLION)
    tokens = lambda c: F.col(c).cast(DecimalType(20, 0))
    input_cost = (tokens("input_tokens") * F.col("input_price_per_million_tokens") / million).cast(MONEY_TYPE)
    output_cost = (tokens("output_tokens") * F.col("output_price_per_million_tokens") / million).cast(MONEY_TYPE)
    zero = F.lit(Decimal(0)).cast(MONEY_TYPE)

    out = priced_join.withColumn("billing_status", status)
    for name, value in (("input_cost", input_cost), ("output_cost", output_cost)):
        out = out.withColumn(
            name,
            F.when(F.col("billing_status") == "not_billed", zero)
            .when(F.col("billing_status") == "priced", value),  # unpriced -> NULL, never 0
        )
    return out.withColumn("total_estimated_cost", (F.col("input_cost") + F.col("output_cost")).cast(MONEY_TYPE))


def price_events(events: DataFrame, pricing: DataFrame) -> DataFrame:
    return calculate_costs(attach_pricing(events, pricing))
