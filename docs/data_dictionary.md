# Data dictionary (all data is synthetic)

Defaults: catalog `workspace` (configurable), schema `llm_usage`.

## landing_usage_events / bronze_usage_events (raw columns)
| Column | Type | Notes |
|---|---|---|
| request_id | string | business key, e.g. `req-0000042` |
| request_timestamp | **string** | ISO-8601; kept as text in the raw layers so bad values reach quarantine |
| team, application, provider, model, environment | string | |
| input_tokens, output_tokens, latency_ms | bigint | |
| status | string | `success` / `error` / `timeout` / `rate_limited` expected |
| retry_count | int | |
| cache_hit | boolean | |
| ingested_at | timestamp | delivery time |
| batch_id | string | `batch_001`, `batch_002` |

Bronze adds `_record_hash` (SHA-256 of all raw columns except `batch_id`), `_source`, `_bronze_loaded_at`.

## bronze_model_pricing / silver_model_pricing
`provider, model, input_price_per_million_tokens, output_price_per_million_tokens, currency, pricing_version,
effective_from, effective_to`. Bronze = strings as read from CSV; Silver = `DECIMAL(12,6)` prices and `DATE`s.

## silver_usage_events
Raw business columns normalised, with `request_timestamp` as a `timestamp`, `request_date` (date), `warnings`
(array<string>), `duplicate_group_size` (valid deliveries seen for the key), `_record_hash`.

## quarantined_usage_events
`request_id, batch_id, ingested_at, _record_hash, _raw_record (JSON of the raw row), quarantine_category,
rejection_reasons (array), winner_record_hash, quarantined_at`.

## silver_priced_usage_events
`silver_usage_events` columns + `input_price_per_million_tokens, output_price_per_million_tokens, currency,
pricing_version, billing_status, input_cost, output_cost, total_estimated_cost` (costs `DECIMAL(18,8)`).

## Gold
- **gold_usage_daily** — grain `request_date × provider × model × team × application × environment`. Additive
  facts only: requests, success/failed/rate_limited requests, priced/unpriced/not_billed requests, input/output
  tokens, input/output/total cost, `latency_ms_sum`, retry and cache-hit counts, `cost_of_requests_with_retries`.
- **gold_model_performance** — grain `provider × model`: counts, `failure_rate`, `avg_latency_ms`,
  `p95_latency_ms`, `p95_latency_success_ms`, `total_estimated_cost`, `cost_per_successful_request`.
- **gold_budget_utilization** — grain `application × year_month`: `estimated_spend`, `monthly_budget`,
  `budget_utilization`, `budget_status` (`ok`/`warning`/`exceeded`/`no_budget`).
- **gold_quality_metrics** — long format `(metric_group, metric_name, metric_value)`.
