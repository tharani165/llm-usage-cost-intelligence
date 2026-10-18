import itertools
from datetime import datetime

from conftest import bronze_from, raw_row
from llm_cost_intelligence.transformations import build_silver


def silver_of(spark, cfg, rows):
    res = build_silver(bronze_from(spark, rows), cfg)
    return res, {r["request_id"]: r for r in res.events.collect()}


def test_exact_duplicate_ingestion_collapses_to_one_row(spark, cfg):
    row = raw_row(request_id="r1")
    res, silver = silver_of(spark, cfg, [row, dict(row)])
    assert len(silver) == 1 and silver["r1"]["duplicate_group_size"] == 2
    sup = res.quarantine.collect()
    assert [r["quarantine_category"] for r in sup] == ["superseded_duplicate"]
    assert sup[0]["winner_record_hash"] == silver["r1"]["_record_hash"]


def test_conflicting_duplicate_latest_ingestion_wins(spark, cfg):
    old = raw_row(request_id="r1", latency_ms=500, ingested_at=datetime(2025, 1, 10, 10, 0, 0))
    new = raw_row(request_id="r1", latency_ms=777, ingested_at=datetime(2025, 1, 10, 11, 0, 0))
    _, silver = silver_of(spark, cfg, [new, old])
    assert silver["r1"]["latency_ms"] == 777


def test_winner_is_independent_of_input_order(spark, cfg):
    same_time = datetime(2025, 1, 10, 10, 0, 0)
    versions = [raw_row(request_id="r1", latency_ms=ms, ingested_at=same_time, batch_id=b)
                for ms, b in ((100, "batch_001"), (200, "batch_002"), (300, "batch_002"))]
    winners = set()
    for perm in itertools.permutations(versions):
        _, silver = silver_of(spark, cfg, list(perm))
        winners.add(silver["r1"]["latency_ms"])
    assert len(winners) == 1  # batch_002 beats batch_001; the hash decides between the two batch_002 rows
    assert winners != {100}


def test_distinct_request_ids_for_retries_are_all_kept(spark, cfg):
    rows = [raw_row(request_id="r1", status="error"), raw_row(request_id="r1-retry", status="success")]
    _, silver = silver_of(spark, cfg, rows)
    assert set(silver) == {"r1", "r1-retry"}


def test_invalid_later_correction_does_not_replace_valid_record(spark, cfg):
    good = raw_row(request_id="r1", latency_ms=500, ingested_at=datetime(2025, 1, 10, 10, 0, 0))
    bad = raw_row(request_id="r1", input_tokens=-1, ingested_at=datetime(2025, 1, 10, 12, 0, 0))
    res, silver = silver_of(spark, cfg, [good, bad])
    assert silver["r1"]["latency_ms"] == 500
    assert [r["quarantine_category"] for r in res.quarantine.collect()] == ["validation_failure"]


def test_every_bronze_row_is_in_exactly_one_output(spark, cfg):
    rows = [raw_row(request_id="a"), raw_row(request_id="a"), raw_row(request_id="b", team=None),
            raw_row(request_id="c")]
    res = build_silver(bronze_from(spark, rows), cfg)
    assert res.events.count() + res.quarantine.count() == len(rows)
