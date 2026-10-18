# Testing strategy

| Layer | Where it runs | What it covers |
|---|---|---|
| Pure Python (no Spark) | anywhere, e.g. `pytest tests/test_data_generator.py`, the pricing-config tests | generator determinism and defect counts (independent oracle), pricing-config rules |
| Spark unit/integration | local Spark 4 (needs **Java 17+**), `make test` | validation, dedup, pricing arithmetic, Gold metrics, reconciliation, incremental batches |
| End-to-end + SQL smoke | `python scripts/validate_project.py` | full two-batch run, 14 reconciliation checks, every statement in `sql/*.sql` executed on local temp views |
| In Databricks | notebooks `01`–`05` | each notebook asserts its own checks; notebook 05 fails if any reconciliation check fails |

The local runs use plain Spark (no Delta, no Unity Catalog), so table writes, Delta behaviour and serverless
specifics are **only exercised in Databricks**. The shared logic is identical, because the notebooks import the
same `src/llm_cost_intelligence` functions.

Spark tests are slow on a laptop (the full suite took ~15 minutes on the author's Windows machine) because each
test starts several small Spark jobs. If Spark cannot start, the `spark` fixture skips those tests rather than
failing them, so check the summary line for skips.

Key deterministic checks: manual cost (`1,000 × 3.00/M + 500 × 15.00/M = 0.0105`), p95 of 100…1000 = 955,
effective-date boundary (Jan 15 → v1, Jan 16 → v2), winner independent of input order (all 6 permutations),
replaying a batch inserts 0 rows, corrections replace originals, late arrivals land on their event date.
