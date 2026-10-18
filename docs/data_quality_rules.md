# Data-quality rules, deduplication and incremental processing

## Normalisation (before validation)
Strings are trimmed; blank becomes NULL; `team`, `application`, `provider`, `model`, `environment` and `status`
are lower-cased (`request_id` keeps its case); `request_timestamp` is parsed with `try_to_timestamp`, so an
unparseable value becomes NULL instead of failing the job. The original row is preserved as JSON in
`_raw_record` for anything that is quarantined. Padding/casing issues therefore do **not** cause rejection.

## Hard rules → quarantine (`quarantine_category = validation_failure`)
A row failing any rule is quarantined with **all** triggered reasons in `rejection_reasons`.

| Reason | Condition |
|---|---|
| `missing_request_id`, `missing_team`, `missing_application`, `missing_provider`, `missing_model`, `missing_environment` | NULL/blank after normalisation |
| `invalid_timestamp` | missing or unparseable `request_timestamp` |
| `missing_input_tokens`, `missing_output_tokens`, `missing_latency`, `missing_cache_hit`, `missing_ingested_at` | NULL |
| `negative_input_tokens`, `negative_output_tokens`, `negative_latency` | value < 0 |
| `invalid_status` | NULL or not one of `success`, `error`, `timeout`, `rate_limited` |
| `invalid_retry_count` | NULL or < 0 |

## Soft warnings → kept in Silver (`warnings` array)
`high_latency` (≥ 30,000 ms), `high_input_tokens` (≥ 20,000), `high_output_tokens` (≥ 8,000),
`high_retry_count` (≥ 3), `late_arrival` (ingested > 24 h after the event). Thresholds: `ProjectConfig`.
Suspicious records are still billed and counted; the warning only supports investigation.

## Deduplication
- **Key:** `request_id`, evaluated only among rows that passed the hard rules. An invalid later delivery can
  therefore never replace a valid earlier one.
- **Winner policy** (total order, independent of row order or partitioning): latest `ingested_at`, then latest
  `batch_id`, then greatest `_record_hash`. A later delivery is treated as a correction.
- Losers go to `quarantined_usage_events` with `quarantine_category = superseded_duplicate` and
  `winner_record_hash`, so every decision is auditable.
- Distinct `request_id`s are never merged, so separately logged retries are preserved.

## What each count means
- `bronze_rows` — every delivered row, including re-deliveries.
- `quarantine_rows` — `validation_failure` rows + `superseded_duplicate` rows (one per Bronze row).
- `silver_rows` — one row per distinct valid `request_id`.
- **Reconciliation:** `bronze_rows = silver_rows + quarantine_rows` exactly. Rows with several rejection
  reasons are counted once in the identity (per-reason counts in `gold_quality_metrics` can exceed the row count).

## Incremental processing
- Bronze load is idempotent per `(batch_id, _record_hash)`: replaying a batch inserts 0 rows. Identical rows
  inside one batch are kept (Bronze preserves what arrived). The check is an anti-join with insert-only merge
  semantics rather than a Delta `MERGE`, so it runs unchanged locally and on Databricks.
- Silver, pricing and Gold are **rebuilt from Bronze each run** (full refresh, overwrite). This is simple and
  correct at this scale; late arrivals and corrections change historical aggregates automatically because the
  affected days are recomputed. It would not scale to very large Bronze tables (see future work).
- `batch_002` contains new requests, exact re-deliveries, corrections (same key, later `ingested_at`, changed
  values) and late-arriving events (event dates before the batch window).
