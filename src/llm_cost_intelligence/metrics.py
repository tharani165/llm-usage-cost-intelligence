"""Gold aggregates, quality metrics and reconciliation checks.

Population: every Gold aggregate is built from silver_priced_usage_events, i.e. the
de-duplicated, validated records. Rows that failed validation or were superseded
duplicates are never counted (they are reported only in gold_quality_metrics).
"""
from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from .config import ProjectConfig
from .schemas import MONEY_TYPE


def _count_if(condition) -> "F.Column":
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long")


def build_usage_daily(priced: DataFrame) -> DataFrame:
    """Additive facts at date x provider x model x team x application x environment.

    Every rate (failure rate, cost per success, average latency, cache-hit rate) is a ratio of
    two columns here, so it can be re-aggregated correctly at any coarser grain in SQL.
    """
    status = F.col("status")
    return priced.groupBy(
        "request_date", "provider", "model", "team", "application", "environment"
    ).agg(
        F.count(F.lit(1)).alias("requests"),
        _count_if(status == "success").alias("success_requests"),
        _count_if(status != "success").alias("failed_requests"),
        _count_if(status == "rate_limited").alias("rate_limited_requests"),
        _count_if(F.col("billing_status") == "priced").alias("priced_requests"),
        _count_if(F.col("billing_status") == "unpriced").alias("unpriced_requests"),
        _count_if(F.col("billing_status") == "not_billed").alias("not_billed_requests"),
        F.sum("input_tokens").alias("input_tokens"),
        F.sum("output_tokens").alias("output_tokens"),
        F.sum("input_cost").cast(MONEY_TYPE).alias("input_cost"),
        F.sum("output_cost").cast(MONEY_TYPE).alias("output_cost"),
        F.sum("total_estimated_cost").cast(MONEY_TYPE).alias("total_estimated_cost"),
        F.sum("latency_ms").alias("latency_ms_sum"),
        _count_if(F.col("retry_count") > 0).alias("requests_with_retries"),
        F.sum("retry_count").cast("long").alias("total_retries"),
        F.sum(F.when(F.col("retry_count") > 0, F.col("total_estimated_cost"))).cast(MONEY_TYPE)
        .alias("cost_of_requests_with_retries"),
        _count_if(F.col("cache_hit")).alias("cache_hit_requests"),
    )


def build_model_performance(priced: DataFrame) -> DataFrame:
    """Per provider/model. p95 is an exact percentile (linear interpolation) over all requests
    in the group; the success-only p95 excludes failed requests. Cost per successful request is
    total priced spend (successful AND failed requests) / successful requests, and is NULL when
    any request in the group is unpriced because the numerator would be incomplete."""
    success = F.col("status") == "success"
    out = priced.groupBy("provider", "model").agg(
        F.count(F.lit(1)).alias("requests"),
        _count_if(success).alias("success_requests"),
        _count_if(~success).alias("failed_requests"),
        _count_if(F.col("billing_status") == "unpriced").alias("unpriced_requests"),
        F.avg("latency_ms").alias("avg_latency_ms"),
        F.percentile("latency_ms", F.lit(0.95)).alias("p95_latency_ms"),
        F.percentile(F.when(success, F.col("latency_ms")), F.lit(0.95)).alias("p95_latency_success_ms"),
        F.sum("total_estimated_cost").cast(MONEY_TYPE).alias("total_estimated_cost"),
    )
    return (
        out.withColumn("failure_rate", F.col("failed_requests") / F.col("requests"))
        .withColumn(
            "cost_per_successful_request",
            F.when(
                (F.col("success_requests") > 0) & (F.col("unpriced_requests") == 0),
                (F.col("total_estimated_cost") / F.col("success_requests")).cast(DecimalType(18, 8)),
            ),
        )
    )


def budgets_dataframe(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    rows = [(app, Decimal(str(amount))) for app, amount in cfg.monthly_budgets.items()]
    return spark.createDataFrame(rows, StructType([
        StructField("application", StringType()), StructField("monthly_budget", DecimalType(18, 4)),
    ]))


def build_budget_utilization(priced: DataFrame, budgets: DataFrame, cfg: ProjectConfig) -> DataFrame:
    """Spend per application and calendar month against the synthetic monthly budget."""
    spend = (
        priced.withColumn("year_month", F.date_format("request_date", "yyyy-MM"))
        .groupBy("application", "year_month")
        .agg(F.sum("total_estimated_cost").cast(MONEY_TYPE).alias("estimated_spend"),
             _count_if(F.col("billing_status") == "unpriced").alias("unpriced_requests"))
    )
    out = spend.join(budgets, "application", "left").withColumn(
        "budget_utilization", (F.col("estimated_spend") / F.col("monthly_budget")).cast(DecimalType(18, 4))
    )
    return out.withColumn(
        "budget_status",
        F.when(F.col("monthly_budget").isNull(), "no_budget")
        .when(F.col("budget_utilization") >= 1, "exceeded")
        .when(F.col("budget_utilization") >= cfg.budget_warning_ratio, "warning")
        .otherwise("ok"),
    )


def build_quality_metrics(bronze: DataFrame, silver: DataFrame, quarantine: DataFrame,
                          priced: DataFrame) -> DataFrame:
    """Long-format counts: (metric_group, metric_name, metric_value). Counts are small, so they
    are collected to the driver and returned as a tiny DataFrame.

    Note: rejection_reason counts are per reason, so a row with two reasons counts twice there;
    quarantine_category counts are per row and always sum to the quarantine row count."""
    spark = bronze.sparkSession
    metrics: list[tuple[str, str, int]] = [
        ("volume", "bronze_rows", bronze.count()),
        ("volume", "silver_rows", silver.count()),
        ("volume", "quarantine_rows", quarantine.count()),
    ]
    for r in quarantine.groupBy("quarantine_category").count().collect():
        metrics.append(("quarantine_category", r["quarantine_category"], r["count"]))
    reasons = quarantine.filter(F.col("quarantine_category") == "validation_failure") \
        .select(F.explode("rejection_reasons").alias("reason")).groupBy("reason").count()
    for r in reasons.collect():
        metrics.append(("rejection_reason", r["reason"], r["count"]))
    for r in priced.groupBy("billing_status").count().collect():
        metrics.append(("pricing_coverage", r["billing_status"], r["count"]))
    warns = silver.select(F.explode("warnings").alias("warning")).groupBy("warning").count()
    for r in warns.collect():
        metrics.append(("soft_warning", r["warning"], r["count"]))
    return spark.createDataFrame(metrics, "metric_group string, metric_name string, metric_value long")


def run_reconciliation(bronze, quarantine, silver, priced, usage_daily, model_performance,
                       budget) -> list[dict]:
    """Each check: name, expected, actual, passed. All comparisons are exact (decimals included)."""
    one = lambda df, expr: df.agg(expr).first()[0]
    total_cost = one(priced, F.sum("total_estimated_cost"))
    silver_rows = silver.count()
    checks = [
        ("bronze_rows == silver_rows + quarantine_rows", bronze.count(), silver_rows + quarantine.count()),
        ("silver request_ids are unique", silver_rows, silver.select("request_id").distinct().count()),
        ("priced rows == silver rows (no join fan-out)", silver_rows, priced.count()),
        ("priced + unpriced + not_billed == silver rows", silver_rows,
         priced.filter(F.col("billing_status").isin("priced", "unpriced", "not_billed")).count()),
        ("gold_usage_daily requests == silver rows", silver_rows, one(usage_daily, F.sum("requests"))),
        ("gold_usage_daily cost == silver priced cost", total_cost, one(usage_daily, F.sum("total_estimated_cost"))),
        ("gold_usage_daily input tokens == silver", one(silver, F.sum("input_tokens")), one(usage_daily, F.sum("input_tokens"))),
        ("gold_usage_daily output tokens == silver", one(silver, F.sum("output_tokens")), one(usage_daily, F.sum("output_tokens"))),
        ("gold_model_performance requests == silver rows", silver_rows, one(model_performance, F.sum("requests"))),
        ("gold_model_performance cost == silver priced cost", total_cost, one(model_performance, F.sum("total_estimated_cost"))),
        ("gold_budget_utilization spend == silver priced cost", total_cost, one(budget, F.sum("estimated_spend"))),
        ("rows where total != input + output cost", 0,
         priced.filter(F.col("total_estimated_cost") != F.col("input_cost") + F.col("output_cost")).count()),
        ("priced rows with NULL cost", 0,
         priced.filter((F.col("billing_status") == "priced") & F.col("total_estimated_cost").isNull()).count()),
        ("unpriced rows with non-NULL cost (silent zero)", 0,
         priced.filter((F.col("billing_status") == "unpriced") & F.col("total_estimated_cost").isNotNull()).count()),
    ]
    return [{"check": n, "expected": e, "actual": a, "passed": e == a} for n, e, a in checks]
