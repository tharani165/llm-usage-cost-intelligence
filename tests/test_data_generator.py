from collections import Counter

import pytest

from llm_cost_intelligence import data_generator as dg
from llm_cost_intelligence.config import ProjectConfig


def test_fixed_seed_is_reproducible():
    a, _ = dg.generate_initial_batch(dg.GeneratorConfig(500, seed=7))
    b, _ = dg.generate_initial_batch(dg.GeneratorConfig(500, seed=7))
    c, _ = dg.generate_initial_batch(dg.GeneratorConfig(500, seed=8))
    assert a == b and a != c


def test_rows_have_every_required_field():
    rows, _ = dg.generate_initial_batch(dg.GeneratorConfig(100))
    assert all(set(r) == set(dg.BATCH_COLUMNS) for r in rows)


def test_injected_defects_match_documented_expectations():
    """Independent Python oracle: count the defects directly from the generated rows."""
    rows, exp = dg.generate_initial_batch(dg.GeneratorConfig(2_000))
    assert len(rows) == exp["raw_rows"] == 2_000 + exp["exact_duplicates"] + exp["conflicting_duplicates"]
    dup_ids = {i for i, n in Counter(r["request_id"] for r in rows).items() if n > 1 and i}
    assert len(dup_ids) == exp["superseded_duplicates"]
    assert sum((r["input_tokens"] < 0) or (r["output_tokens"] < 0) for r in rows) == exp["negative_tokens"]
    assert sum(r["status"].strip().lower() not in dg.VALID_STATUSES for r in rows) == exp["invalid_status"]
    assert sum(r["latency_ms"] >= 30_000 for r in rows) == exp["high_latency"]
    unclean = sum(not dg.is_clean_raw(r) for r in rows if r["request_id"] not in dup_ids)
    assert unclean == exp["hard_failures"]
    assert exp["silver_rows"] == exp["raw_rows"] - exp["hard_failures"] - exp["superseded_duplicates"]


def test_sample_mode_still_injects_every_defect_type():
    _, exp = dg.generate_initial_batch(dg.GeneratorConfig(100))
    assert all(exp[k] >= 1 for k in ("exact_duplicates", "missing_required", "negative_tokens", "invalid_status"))


def test_incremental_batch_composition():
    rows1, _ = dg.generate_initial_batch(dg.GeneratorConfig(1_000))
    rows2, exp2 = dg.generate_incremental_batch(rows1, n_new=40, n_exact=10, n_corrections=5, n_late=4)
    ids1 = {r["request_id"] for r in rows1}
    assert len(rows2) == exp2["raw_rows"] == 40 + 4 + 10 + 5
    assert len({r["request_id"] for r in rows2} - ids1) == exp2["new_request_ids"] == 44
    assert all(r["batch_id"] == "batch_002" for r in rows2)


def test_config_bounds():
    with pytest.raises(ValueError):
        ProjectConfig(n_requests=50_001)
    with pytest.raises(ValueError):
        ProjectConfig(n_requests=0)
