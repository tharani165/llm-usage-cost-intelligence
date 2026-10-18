"""Batch replay, new/late/corrected records, and no double counting across batches."""
import pytest

from conftest import PRICING_CSV
from llm_cost_intelligence import data_generator as dg
from llm_cost_intelligence.incremental import rows_not_yet_loaded
from llm_cost_intelligence.metrics import build_usage_daily
from llm_cost_intelligence.pricing import load_pricing_csv, price_events, pricing_dataframe
from llm_cost_intelligence.schemas import RAW_EVENT_SCHEMA
from llm_cost_intelligence.transformations import add_bronze_metadata, build_silver


@pytest.fixture(scope="module")
def batches():
    rows1, exp1 = dg.generate_initial_batch(dg.GeneratorConfig(600))
    rows2, exp2 = dg.generate_incremental_batch(rows1, n_new=40, n_exact=10, n_corrections=5, n_late=4)
    return rows1, exp1, rows2, exp2


def load(spark, rows, bronze=None):
    df = add_bronze_metadata(spark.createDataFrame(dg.to_tuples(rows), RAW_EVENT_SCHEMA), "test")
    new = rows_not_yet_loaded(bronze, df)
    return new if bronze is None else bronze.unionByName(new)


def test_replaying_a_batch_inserts_nothing(spark, batches):
    rows1, exp1, _, _ = batches
    bronze = load(spark, rows1).cache()
    assert bronze.count() == exp1["raw_rows"]          # in-batch exact duplicates are preserved
    assert load(spark, rows1, bronze).count() == exp1["raw_rows"]


def test_new_batch_adds_only_new_requests_to_silver(spark, cfg, batches):
    rows1, exp1, rows2, exp2 = batches
    bronze1 = load(spark, rows1).cache()
    bronze2 = load(spark, rows2, bronze1).cache()
    assert bronze2.count() == exp1["raw_rows"] + exp2["raw_rows"]
    silver = build_silver(bronze2, cfg).events
    assert silver.count() == exp1["silver_rows"] + exp2["new_request_ids"]
    assert silver.select("request_id").distinct().count() == silver.count()


def test_reprocessing_everything_gives_identical_totals(spark, cfg, batches):
    rows1, _, rows2, _ = batches
    pricing = pricing_dataframe(spark, load_pricing_csv(PRICING_CSV))

    def totals(bronze):
        priced = price_events(build_silver(bronze, cfg).events, pricing)
        return build_usage_daily(priced).agg({"requests": "sum", "total_estimated_cost": "sum"}).first()

    once = load(spark, rows2, load(spark, rows1)).cache()
    replayed = load(spark, rows2, load(spark, rows1, once), ).cache()    # same data delivered again
    assert tuple(totals(once)) == tuple(totals(replayed))


def test_late_arrivals_land_on_their_event_date_and_are_flagged(spark, cfg, batches):
    rows1, _, rows2, exp2 = batches
    silver = build_silver(load(spark, rows2, load(spark, rows1)), cfg).events
    late_ids = [r["request_id"] for r in rows2[40:44]]            # the late block in batch 2
    late = {r["request_id"]: r for r in silver.filter(silver.request_id.isin(late_ids)).collect()}
    assert len(late) == exp2["late_arrivals"]
    assert all("late_arrival" in r["warnings"] and str(r["request_date"]) < "2025-02-01" for r in late.values())


def test_corrections_replace_the_original_record(spark, cfg, batches):
    rows1, _, rows2, exp2 = batches
    silver = build_silver(load(spark, rows2, load(spark, rows1)), cfg).events
    originals = {r["request_id"]: r for r in rows1}
    corrected = {r["request_id"]: r for r in silver.collect()
                 if r["request_id"] in set(exp2["corrected_request_ids"])}
    assert len(corrected) == exp2["corrections"]
    for rid, row in corrected.items():
        assert row["output_tokens"] == originals[rid]["output_tokens"] + 50
        assert row["batch_id"] == "batch_002"
