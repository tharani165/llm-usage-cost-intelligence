"""Explicit Spark schemas and shared constants."""
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

VALID_STATUSES = ("success", "error", "timeout", "rate_limited")
FAILURE_STATUSES = ("error", "timeout", "rate_limited")

MONEY_TYPE = DecimalType(18, 8)   # per-row and aggregated monetary values
PRICE_TYPE = DecimalType(12, 6)   # price per million tokens

# request_timestamp is a STRING in the raw layer on purpose: unparseable values
# must reach Bronze intact and be quarantined in Silver, not fail the load.
RAW_EVENT_SCHEMA = StructType([
    StructField("request_id", StringType()),
    StructField("request_timestamp", StringType()),
    StructField("team", StringType()),
    StructField("application", StringType()),
    StructField("provider", StringType()),
    StructField("model", StringType()),
    StructField("input_tokens", LongType()),
    StructField("output_tokens", LongType()),
    StructField("latency_ms", LongType()),
    StructField("status", StringType()),
    StructField("retry_count", IntegerType()),
    StructField("cache_hit", BooleanType()),
    StructField("environment", StringType()),
    StructField("ingested_at", TimestampType()),
    StructField("batch_id", StringType()),
])
RAW_COLUMNS = [f.name for f in RAW_EVENT_SCHEMA.fields]

# Pricing config lands as strings (as read from CSV) and is typed in Silver.
RAW_PRICING_SCHEMA = StructType([
    StructField(c, StringType()) for c in (
        "provider", "model", "input_price_per_million_tokens",
        "output_price_per_million_tokens", "currency", "pricing_version",
        "effective_from", "effective_to")
])

SILVER_PRICING_SCHEMA = StructType([
    StructField("provider", StringType(), False),
    StructField("model", StringType(), False),
    StructField("input_price_per_million_tokens", PRICE_TYPE, False),
    StructField("output_price_per_million_tokens", PRICE_TYPE, False),
    StructField("currency", StringType(), False),
    StructField("pricing_version", StringType(), False),
    StructField("effective_from", DateType(), False),
    StructField("effective_to", DateType(), True),
])
