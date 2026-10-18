from decimal import Decimal

import pytest

from conftest import PRICING_CSV, bronze_from, raw_row
from llm_cost_intelligence.pricing import (
    load_pricing_csv, price_events, pricing_dataframe, validate_pricing_rows)
from llm_cost_intelligence.transformations import build_silver


# ---- pure Python: pricing configuration rules ----

def price(**kw):
    base = {"provider": "p", "model": "m", "input_price_per_million_tokens": "1.0",
            "output_price_per_million_tokens": "2.0", "currency": "USD", "pricing_version": "v1",
            "effective_from": "2025-01-01", "effective_to": ""}
    base.update(kw)
    return base


def test_shipped_pricing_csv_is_valid():
    assert validate_pricing_rows(load_pricing_csv(PRICING_CSV)) == []


@pytest.mark.parametrize("row, fragment", [
    (price(input_price_per_million_tokens="-1"), "negative"),
    (price(output_price_per_million_tokens="abc"), "not a number"),
    (price(effective_to="2024-12-31"), "before effective_from"),
    (price(effective_from="2025-13-01"), "not a valid date"),
    (price(currency=""), "currency is required"),
])
def test_invalid_pricing_rows_are_reported(row, fragment):
    assert any(fragment in e for e in validate_pricing_rows([row]))


def test_overlapping_ranges_are_rejected_but_adjacent_are_fine():
    overlap = [price(effective_to="2025-01-31"), price(pricing_version="v2", effective_from="2025-01-31")]
    assert any("overlapping" in e for e in validate_pricing_rows(overlap))
    adjacent = [price(effective_to="2025-01-31"), price(pricing_version="v2", effective_from="2025-02-01")]
    assert validate_pricing_rows(adjacent) == []
    open_ended_twice = [price(), price(pricing_version="v2", effective_from="2025-06-01")]
    assert any("overlapping" in e for e in validate_pricing_rows(open_ended_twice))


# ---- Spark: cost arithmetic ----

@pytest.fixture
def pricing(spark):
    return pricing_dataframe(spark, load_pricing_csv(PRICING_CSV))


def cost_of(spark, cfg, pricing, **row):
    events = build_silver(bronze_from(spark, [raw_row(request_id="r1", **row)]), cfg).events
    return price_events(events, pricing).first()


def test_manual_cost_calculation(spark, cfg, pricing):
    # provider_beta/model_standard v1: input 3.00, output 15.00 per million tokens.
    # 1,000 * 3.00 / 1e6 = 0.003 ; 500 * 15.00 / 1e6 = 0.0075 ; total = 0.0105
    r = cost_of(spark, cfg, pricing, input_tokens=1000, output_tokens=500)
    assert (r["input_cost"], r["output_cost"], r["total_estimated_cost"]) == (
        Decimal("0.00300000"), Decimal("0.00750000"), Decimal("0.01050000"))
    assert r["billing_status"] == "priced" and r["pricing_version"] == "v1"


def test_input_only_output_only_and_zero_tokens(spark, cfg, pricing):
    r = cost_of(spark, cfg, pricing, input_tokens=2_000_000, output_tokens=0)
    assert (r["input_cost"], r["output_cost"]) == (Decimal("6.00000000"), Decimal("0"))
    r = cost_of(spark, cfg, pricing, input_tokens=0, output_tokens=1_000_000)
    assert (r["input_cost"], r["output_cost"]) == (Decimal("0"), Decimal("15.00000000"))
    r = cost_of(spark, cfg, pricing, input_tokens=0, output_tokens=0)
    assert r["total_estimated_cost"] == 0 and r["billing_status"] == "priced"


def test_small_amounts_keep_eight_decimals(spark, cfg, pricing):
    # provider_alpha/model_economy input 0.15 per million: 1 token = 0.00000015 exactly
    r = cost_of(spark, cfg, pricing, provider="provider_alpha", model="model_economy",
                input_tokens=1, output_tokens=0)
    assert r["input_cost"] == Decimal("0.00000015")


def test_cost_columns_are_decimal_18_8(spark, cfg, pricing):
    events = build_silver(bronze_from(spark, [raw_row()]), cfg).events
    schema = {f.name: f.dataType.simpleString() for f in price_events(events, pricing).schema.fields}
    assert schema["input_cost"] == schema["output_cost"] == schema["total_estimated_cost"] == "decimal(18,8)"


def test_missing_pricing_is_flagged_not_zero(spark, cfg, pricing):
    r = cost_of(spark, cfg, pricing, provider="provider_gamma", model="model_experimental")
    assert r["billing_status"] == "unpriced"
    assert r["input_cost"] is None and r["output_cost"] is None and r["total_estimated_cost"] is None


def test_rate_limited_is_explicitly_not_billed(spark, cfg, pricing):
    r = cost_of(spark, cfg, pricing, status="rate_limited")
    assert r["billing_status"] == "not_billed" and r["total_estimated_cost"] == 0


def test_failed_requests_are_billed_on_recorded_tokens(spark, cfg, pricing):
    r = cost_of(spark, cfg, pricing, status="error", input_tokens=1000, output_tokens=0)
    assert r["billing_status"] == "priced" and r["total_estimated_cost"] == Decimal("0.00300000")


@pytest.mark.parametrize("day, version, input_cost", [
    ("2025-01-15", "v1", Decimal("2.50000000")),   # last day of v1
    ("2025-01-16", "v2", Decimal("2.00000000")),   # first day of v2
])
def test_effective_date_selects_the_price_version(spark, cfg, pricing, day, version, input_cost):
    r = cost_of(spark, cfg, pricing, provider="provider_alpha", model="model_standard",
                request_timestamp=f"{day}T12:00:00", input_tokens=1_000_000, output_tokens=0)
    assert r["pricing_version"] == version and r["input_cost"] == input_cost


def test_request_outside_any_effective_range_is_unpriced(spark, cfg, pricing):
    r = cost_of(spark, cfg, pricing, request_timestamp="2024-06-01T00:00:00")
    assert r["billing_status"] == "unpriced"


def test_invalid_pricing_config_cannot_build_a_dataframe(spark):
    with pytest.raises(ValueError, match="negative"):
        pricing_dataframe(spark, [price(input_price_per_million_tokens="-1")])
