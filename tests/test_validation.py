from datetime import datetime

import pytest

from conftest import bronze_from, raw_row
from llm_cost_intelligence.transformations import build_silver


def run(spark, cfg, *rows):
    result = build_silver(bronze_from(spark, list(rows)), cfg)
    quarantine = {r["request_id"]: r for r in result.quarantine.collect()}
    return {r["request_id"]: r for r in result.events.collect()}, quarantine


@pytest.mark.parametrize("override, reason", [
    ({"team": None}, "missing_team"),
    ({"application": "   "}, "missing_application"),
    ({"model": None}, "missing_model"),
    ({"provider": None}, "missing_provider"),
    ({"input_tokens": -1}, "negative_input_tokens"),
    ({"output_tokens": -5}, "negative_output_tokens"),
    ({"status": "weird"}, "invalid_status"),
    ({"status": ""}, "invalid_status"),
    ({"request_timestamp": "not-a-date"}, "invalid_timestamp"),
    ({"request_timestamp": None}, "invalid_timestamp"),
    ({"latency_ms": -1}, "negative_latency"),
    ({"retry_count": -1}, "invalid_retry_count"),
    ({"input_tokens": None}, "missing_input_tokens"),
])
def test_hard_failures_are_quarantined_with_reason(spark, cfg, override, reason):
    silver, quarantine = run(spark, cfg, raw_row(request_id="r1", **override))
    assert silver == {}
    row = quarantine["r1"]
    assert reason in row["rejection_reasons"] and row["quarantine_category"] == "validation_failure"
    assert "r1" in row["_raw_record"]  # raw payload preserved for investigation


def test_blank_request_id_is_quarantined(spark, cfg):
    silver, quarantine = run(spark, cfg, raw_row(request_id="  "))
    assert not silver and quarantine[None]["rejection_reasons"] == ["missing_request_id"]


def test_multiple_reasons_are_all_recorded(spark, cfg):
    _, q = run(spark, cfg, raw_row(request_id="r1", team=None, input_tokens=-3))
    assert set(q["r1"]["rejection_reasons"]) == {"missing_team", "negative_input_tokens"}


def test_dirty_formatting_is_normalised_not_rejected(spark, cfg):
    silver, quarantine = run(spark, cfg, raw_row(request_id="r1", status="  SUCCESS ", team=" Engineering  "))
    assert not quarantine
    assert silver["r1"]["status"] == "success" and silver["r1"]["team"] == "engineering"


def test_suspicious_but_valid_records_are_kept_with_warnings(spark, cfg):
    silver, quarantine = run(
        spark, cfg,
        raw_row(request_id="slow", latency_ms=120_000),
        raw_row(request_id="busy", retry_count=3),
        raw_row(request_id="late", ingested_at=datetime(2025, 1, 20)),
    )
    assert not quarantine
    assert "high_latency" in silver["slow"]["warnings"]
    assert "high_retry_count" in silver["busy"]["warnings"]
    assert "late_arrival" in silver["late"]["warnings"]


def test_clean_record_has_no_warnings(spark, cfg):
    silver, _ = run(spark, cfg, raw_row(request_id="ok"))
    assert list(silver["ok"]["warnings"]) == []
