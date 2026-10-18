from decimal import Decimal

import pytest

from conftest import PRICING_CSV, bronze_from, raw_row
from llm_cost_intelligence.metrics import (
    budgets_dataframe, build_budget_utilization, build_model_performance,
    build_quality_metrics, build_usage_daily, run_reconciliation)
from llm_cost_intelligence.pricing import load_pricing_csv, price_events, pricing_dataframe
from llm_cost_intelligence.transformations import build_silver


def pipeline(spark, cfg, rows):
    bronze = bronze_from(spark, rows)
    silver = build_silver(bronze, cfg)
    pricing = pricing_dataframe(spark, load_pricing_csv(PRICING_CSV))
    priced = price_events(silver.events, pricing)
    return bronze, silver, priced


def rows_with_latencies(latencies, **kw):
    return [raw_row(request_id=f"r{i}", latency_ms=ms, **kw) for i, ms in enumerate(latencies)]


def test_p95_latency_uses_linear_interpolation(spark, cfg):
    # 10 values 100..1000: position 0.95 * 9 = 8.55 -> 900 + 0.55 * 100 = 955
    _, _, priced = pipeline(spark, cfg, rows_with_latencies(range(100, 1100, 100)))
    assert build_model_performance(priced).first()["p95_latency_ms"] == pytest.approx(955.0)


def test_failure_rate_denominator_is_all_requests_in_group(spark, cfg):
    statuses = ["success", "success", "error", "timeout", "rate_limited"]
    rows = [raw_row(request_id=f"r{i}", status=s) for i, s in enumerate(statuses)]
    _, _, priced = pipeline(spark, cfg, rows)
    perf = build_model_performance(priced).first()
    assert (perf["requests"], perf["success_requests"], perf["failed_requests"]) == (5, 2, 3)
    assert perf["failure_rate"] == pytest.approx(3 / 5)


def test_cost_per_successful_request_includes_failed_request_spend(spark, cfg):
    # beta/standard v1: 1000 input tokens = 0.003 each. 1 success + 1 error => 0.006 / 1 success
    rows = [raw_row(request_id="ok", output_tokens=0), raw_row(request_id="bad", status="error", output_tokens=0)]
    _, _, priced = pipeline(spark, cfg, rows)
    assert build_model_performance(priced).first()["cost_per_successful_request"] == Decimal("0.00600000")


def test_cost_per_successful_request_is_null_when_group_has_unpriced_rows(spark, cfg):
    rows = [raw_row(request_id="a", provider="provider_gamma", model="model_experimental")]
    _, _, priced = pipeline(spark, cfg, rows)
    assert build_model_performance(priced).first()["cost_per_successful_request"] is None


def test_usage_daily_aggregates_are_additive_and_exact(spark, cfg):
    rows = [raw_row(request_id="a", input_tokens=1000, output_tokens=500, retry_count=2, cache_hit=True),
            raw_row(request_id="b", input_tokens=2000, output_tokens=0, status="error"),
            raw_row(request_id="c", request_timestamp="2025-01-11T09:00:00")]
    _, _, priced = pipeline(spark, cfg, rows)
    daily = {str(r["request_date"]): r for r in build_usage_daily(priced).collect()}
    day10 = daily["2025-01-10"]
    assert (day10["requests"], day10["success_requests"], day10["failed_requests"]) == (2, 1, 1)
    assert (day10["input_tokens"], day10["output_tokens"]) == (3000, 500)
    assert day10["total_estimated_cost"] == Decimal("0.00300000") + Decimal("0.00750000") + Decimal("0.00600000")
    assert (day10["requests_with_retries"], day10["total_retries"], day10["cache_hit_requests"]) == (1, 2, 1)
    assert daily["2025-01-11"]["requests"] == 1


def test_budget_status_thresholds(spark, cfg):
    cfg.monthly_budgets = {"code_assistant": 0.0105}   # spend of one default row is exactly 0.0105
    _, _, priced = pipeline(spark, cfg, [raw_row(request_id="a")])
    row = build_budget_utilization(priced, budgets_dataframe(spark, cfg), cfg).first()
    assert row["budget_utilization"] == Decimal("1.0000") and row["budget_status"] == "exceeded"
    cfg.monthly_budgets = {"code_assistant": 0.0125}   # 84% -> warning
    row = build_budget_utilization(priced, budgets_dataframe(spark, cfg), cfg).first()
    assert row["budget_status"] == "warning"
    cfg.monthly_budgets = {}
    assert build_budget_utilization(priced, budgets_dataframe(spark, cfg), cfg).first()["budget_status"] == "no_budget"


def test_silver_to_gold_reconciliation_passes_on_mixed_data(spark, cfg):
    rows = [raw_row(request_id="a"), raw_row(request_id="a"), raw_row(request_id="b", team=None),
            raw_row(request_id="c", provider="provider_gamma", model="model_experimental"),
            raw_row(request_id="d", status="rate_limited"), raw_row(request_id="e", status="error")]
    bronze, silver, priced = pipeline(spark, cfg, rows)
    usage, perf = build_usage_daily(priced), build_model_performance(priced)
    budget = build_budget_utilization(priced, budgets_dataframe(spark, cfg), cfg)
    checks = run_reconciliation(bronze, silver.quarantine, silver.events, priced, usage, perf, budget)
    failed = [c for c in checks if not c["passed"]]
    assert not failed, failed


def test_reconciliation_detects_a_broken_aggregate(spark, cfg):
    bronze, silver, priced = pipeline(spark, cfg, [raw_row(request_id="a"), raw_row(request_id="b")])
    usage = build_usage_daily(priced).filter("requests < 0")      # simulate a lost aggregate
    perf = build_model_performance(priced)
    budget = build_budget_utilization(priced, budgets_dataframe(spark, cfg), cfg)
    checks = run_reconciliation(bronze, silver.quarantine, silver.events, priced, usage, perf, budget)
    assert any(not c["passed"] for c in checks)


def test_quality_metrics_report_quarantine_and_pricing_coverage(spark, cfg):
    rows = [raw_row(request_id="a"), raw_row(request_id="a"), raw_row(request_id="b", status="zzz"),
            raw_row(request_id="c", provider="provider_gamma", model="model_experimental")]
    bronze, silver, priced = pipeline(spark, cfg, rows)
    m = {(r["metric_group"], r["metric_name"]): r["metric_value"]
         for r in build_quality_metrics(bronze, silver.events, silver.quarantine, priced).collect()}
    assert m[("volume", "bronze_rows")] == 4 and m[("volume", "silver_rows")] == 2
    assert m[("quarantine_category", "superseded_duplicate")] == 1
    assert m[("rejection_reason", "invalid_status")] == 1
    assert m[("pricing_coverage", "unpriced")] == 1 and m[("pricing_coverage", "priced")] == 1
