"""Deterministic synthetic LLM usage generator (pure Python; no Spark required).

All providers, models and prices are FICTIONAL. Defects are injected on disjoint,
seed-selected rows so the expected validation outcome is known exactly.
"""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta

VALID_STATUSES = ("success", "error", "timeout", "rate_limited")
APPLICATIONS = {  # application -> owning team
    "support_assistant": "customer_support",
    "code_assistant": "engineering",
    "document_summarizer": "finance",
    "internal_knowledge_assistant": "operations",
}
# A product team also uses the knowledge assistant, so team != 1:1 with application.
EXTRA_TEAM_USE = {"internal_knowledge_assistant": "product"}
# (provider, model, weight). provider_gamma/model_experimental has NO pricing row.
MODELS = [
    ("provider_alpha", "model_economy", 5),
    ("provider_alpha", "model_standard", 4),
    ("provider_beta", "model_standard", 3),
    ("provider_beta", "model_advanced", 2),
    ("provider_gamma", "model_experimental", 1),
]
UNPRICED_MODELS = {("provider_gamma", "model_experimental")}
STATUS_WEIGHTS = [0.92, 0.04, 0.02, 0.02]
ENVIRONMENTS = [("production", 0.7), ("staging", 0.2), ("development", 0.1)]
BATCH1_START = datetime(2025, 1, 1)
BATCH1_DAYS = 30
BATCH2_START = datetime(2025, 2, 1)

BATCH_COLUMNS = [
    "request_id", "request_timestamp", "team", "application", "provider", "model",
    "input_tokens", "output_tokens", "latency_ms", "status", "retry_count",
    "cache_hit", "environment", "ingested_at", "batch_id",
]
_MISSING_FIELD_CYCLE = ["request_id", "team", "application", "provider", "model", "request_timestamp"]


@dataclass(frozen=True)
class GeneratorConfig:
    n_requests: int = 20_000
    seed: int = 42
    batch_id: str = "batch_001"

    def injection_counts(self) -> dict:
        n = self.n_requests
        rate = lambda r: max(1, round(n * r))
        return {
            "exact_duplicates": rate(0.005),
            "conflicting_duplicates": rate(0.005),
            "missing_required": rate(0.005),
            "negative_tokens": rate(0.003),
            "invalid_status": rate(0.003),
            "invalid_timestamp": rate(0.002),
            "high_latency": rate(0.005),
            "dirty_formatting": rate(0.0025),   # valid after normalisation
        }


def is_clean_raw(row: dict) -> bool:
    """True if a raw row would pass every hard rule (used to pick safe rows to copy)."""
    try:
        datetime.fromisoformat(row["request_timestamp"])
    except (TypeError, ValueError):
        return False
    required = ("request_id", "team", "application", "provider", "model", "environment")
    return (
        all(row[c] and str(row[c]).strip() for c in required)
        and (row["status"] or "").strip().lower() in VALID_STATUSES
        and row["input_tokens"] is not None and row["input_tokens"] >= 0
        and row["output_tokens"] is not None and row["output_tokens"] >= 0
    )


def _base_row(rng: random.Random, request_id: str, ts: datetime, batch_id: str) -> dict:
    app = rng.choice(list(APPLICATIONS))
    team = APPLICATIONS[app]
    if app in EXTRA_TEAM_USE and rng.random() < 0.3:
        team = EXTRA_TEAM_USE[app]
    provider, model, _ = rng.choices(MODELS, weights=[m[2] for m in MODELS])[0]
    status = rng.choices(VALID_STATUSES, weights=STATUS_WEIGHTS)[0]
    input_tokens = max(10, int(rng.lognormvariate(6.2, 0.8)))
    failed_early = status != "success" and rng.random() < 0.7
    output_tokens = 0 if failed_early else max(5, int(rng.lognormvariate(5.5, 0.9)))
    latency = max(50, int(rng.gauss(900 + 0.8 * output_tokens, 250)))
    if status == "timeout":
        latency = max(latency, 10_000)
    return {
        "request_id": request_id,
        "request_timestamp": ts.isoformat(timespec="seconds"),
        "team": team, "application": app, "provider": provider, "model": model,
        "input_tokens": input_tokens, "output_tokens": output_tokens,
        "latency_ms": latency, "status": status,
        "retry_count": rng.choices([0, 1, 2, 3], weights=[0.85, 0.10, 0.04, 0.01])[0],
        "cache_hit": rng.random() < 0.15,
        "environment": rng.choices([e for e, _ in ENVIRONMENTS], weights=[w for _, w in ENVIRONMENTS])[0],
        # normal pipeline lag of 1-300 seconds
        "ingested_at": ts + timedelta(seconds=rng.randint(1, 300)),
        "batch_id": batch_id,
    }


def generate_initial_batch(cfg: GeneratorConfig = GeneratorConfig()) -> tuple[list[dict], dict]:
    """Return (rows, expected) for batch 1. `expected` documents the injected defects."""
    rng = random.Random(cfg.seed)
    span = BATCH1_DAYS * 86_400
    rows = [
        _base_row(rng, f"req-{i:07d}", BATCH1_START + timedelta(seconds=rng.randrange(span)), cfg.batch_id)
        for i in range(cfg.n_requests)
    ]
    counts = cfg.injection_counts()
    order = list(range(cfg.n_requests))
    rng.shuffle(order)
    take = lambda k: [order.pop() for _ in range(k)]

    for k, i in enumerate(take(counts["missing_required"])):
        field = _MISSING_FIELD_CYCLE[k % len(_MISSING_FIELD_CYCLE)]
        rows[i][field] = "" if field == "request_id" else None
    for k, i in enumerate(take(counts["negative_tokens"])):
        rows[i]["input_tokens" if k % 2 == 0 else "output_tokens"] = -rng.randint(1, 500)
    for i in take(counts["invalid_status"]):
        rows[i]["status"] = rng.choice(["unknown", "ok??", ""])
    for i in take(counts["invalid_timestamp"]):
        rows[i]["request_timestamp"] = rng.choice(["not-a-timestamp", "2025-13-45T99:00:00", "yesterday"])
    for i in take(counts["high_latency"]):
        rows[i]["latency_ms"] = rng.randint(60_000, 180_000)           # soft warning only
    for k, i in enumerate(take(counts["dirty_formatting"])):
        rows[i]["status"] = f"  {rows[i]['status'].upper()} "         # valid once normalised
        rows[i]["team"] = f" {rows[i]['team'].title()}  "

    extra = []
    for i in take(counts["exact_duplicates"]):
        extra.append(dict(rows[i]))                                     # byte-identical re-delivery
    for i in take(counts["conflicting_duplicates"]):
        dup = dict(rows[i])                                             # later re-delivery, corrected value
        dup["ingested_at"] = rows[i]["ingested_at"] + timedelta(hours=1)
        dup["latency_ms"] = rows[i]["latency_ms"] + 100
        extra.append(dup)
    rows.extend(extra)

    expected = {
        "raw_rows": len(rows),
        "unique_request_ids_generated": cfg.n_requests,
        "hard_failures": counts["missing_required"] + counts["negative_tokens"]
        + counts["invalid_status"] + counts["invalid_timestamp"],
        "superseded_duplicates": counts["exact_duplicates"] + counts["conflicting_duplicates"],
        "high_latency_warnings": counts["high_latency"],
        **counts,
    }
    expected["silver_rows"] = expected["raw_rows"] - expected["hard_failures"] - expected["superseded_duplicates"]
    return rows, expected


def generate_incremental_batch(
    batch1_rows: list[dict], n_new: int = 2_000, n_exact: int = 100, n_corrections: int = 50,
    n_late: int = 30, seed: int = 43, batch_id: str = "batch_002",
) -> tuple[list[dict], dict]:
    """Batch 2: new requests, re-delivered requests, corrections and late-arriving events."""
    rng = random.Random(seed)
    id_counts = Counter(r["request_id"] for r in batch1_rows)
    safe = [r for r in batch1_rows if is_clean_raw(r) and id_counts[r["request_id"]] == 1]
    if n_exact + n_corrections > len(safe):  # small batch 1 (e.g. 100 rows): shrink re-deliveries proportionally
        total = n_exact + n_corrections
        n_exact = len(safe) * n_exact // total
        n_corrections = min(n_corrections, len(safe) - n_exact)
    chosen = rng.sample(safe, n_exact + n_corrections)
    next_id = max(int(r["request_id"][4:]) for r in batch1_rows if r["request_id"]) + 1
    batch_time = BATCH2_START + timedelta(hours=6)

    rows = []
    for i in range(n_new):                                              # new, on-time
        ts = BATCH2_START + timedelta(seconds=rng.randrange(2 * 86_400))
        rows.append(_base_row(rng, f"req-{next_id + i:07d}", ts, batch_id))
    for i in range(n_late):                                             # late: event days before ingestion
        ts = datetime(2025, 1, 28) + timedelta(seconds=rng.randrange(3 * 86_400 - 1))
        row = _base_row(rng, f"req-{next_id + n_new + i:07d}", ts, batch_id)
        row["ingested_at"] = batch_time
        rows.append(row)
    for src in chosen[:n_exact]:                                        # exact re-delivery of batch-1 rows
        rows.append({**src, "batch_id": batch_id})
    for src in chosen[n_exact:]:                                        # correction: same key, new values
        rows.append({**src, "batch_id": batch_id, "ingested_at": batch_time,
                     "output_tokens": src["output_tokens"] + 50, "latency_ms": src["latency_ms"] + 200})

    return rows, {
        "raw_rows": len(rows), "new_request_ids": n_new + n_late, "late_arrivals": n_late,
        "exact_redeliveries": n_exact, "corrections": n_corrections,
        "corrected_request_ids": sorted(r["request_id"] for r in chosen[n_exact:]),
    }


def pricing_is_available(provider: str, model: str) -> bool:
    return (provider, model) not in UNPRICED_MODELS


def to_tuples(rows: list[dict]) -> list[tuple]:
    """Rows as tuples in BATCH_COLUMNS order, ready for spark.createDataFrame(..., RAW_EVENT_SCHEMA)."""
    return [tuple(r[c] for c in BATCH_COLUMNS) for r in rows]
