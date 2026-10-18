-- Data quality, quarantine and pricing coverage.
-- Run first:  USE CATALOG <your_catalog>;  USE SCHEMA llm_usage;

-- [Table] Reconciliation summary: bronze_rows = silver_rows + quarantine_rows
SELECT
  MAX(CASE WHEN metric_name = 'bronze_rows'     THEN metric_value END) AS bronze_rows,
  MAX(CASE WHEN metric_name = 'silver_rows'     THEN metric_value END) AS silver_rows,
  MAX(CASE WHEN metric_name = 'quarantine_rows' THEN metric_value END) AS quarantine_rows,
  MAX(CASE WHEN metric_name = 'bronze_rows'     THEN metric_value END)
   - MAX(CASE WHEN metric_name = 'silver_rows'     THEN metric_value END)
   - MAX(CASE WHEN metric_name = 'quarantine_rows' THEN metric_value END) AS unexplained_rows  -- must be 0
FROM gold_quality_metrics
WHERE metric_group = 'volume';

-- [Chart: bar] Quarantine by category and failure rate (denominator: all Bronze rows)
SELECT q.metric_name AS quarantine_category,
       q.metric_value AS rows,
       q.metric_value / b.metric_value AS share_of_bronze_rows
FROM gold_quality_metrics q
CROSS JOIN (SELECT metric_value FROM gold_quality_metrics WHERE metric_name = 'bronze_rows') b
WHERE q.metric_group = 'quarantine_category';

-- [Chart: bar] Hard-failure reasons (a row with two reasons is counted once per reason)
SELECT metric_name AS rejection_reason, metric_value AS rows
FROM gold_quality_metrics
WHERE metric_group = 'rejection_reason'
ORDER BY rows DESC;

-- [Table] Soft warnings: records kept in Silver but flagged
SELECT metric_name AS warning, metric_value AS rows
FROM gold_quality_metrics
WHERE metric_group = 'soft_warning'
ORDER BY rows DESC;

-- [KPI] Pricing coverage = priced / (priced + unpriced). 'not_billed' (rate_limited) is excluded from the denominator.
SELECT
  MAX(CASE WHEN metric_name = 'priced'     THEN metric_value END) AS priced_requests,
  MAX(CASE WHEN metric_name = 'unpriced'   THEN metric_value END) AS unpriced_requests,
  MAX(CASE WHEN metric_name = 'not_billed' THEN metric_value END) AS not_billed_requests,
  MAX(CASE WHEN metric_name = 'priced' THEN metric_value END)
    / (MAX(CASE WHEN metric_name = 'priced' THEN metric_value END)
       + MAX(CASE WHEN metric_name = 'unpriced' THEN metric_value END)) AS pricing_coverage
FROM gold_quality_metrics
WHERE metric_group = 'pricing_coverage';

-- [Table] Which models are unpriced (their cost is NOT in the spend totals)
SELECT provider, model, COUNT(*) AS unpriced_requests, SUM(input_tokens + output_tokens) AS tokens_without_cost
FROM silver_priced_usage_events
WHERE billing_status = 'unpriced'
GROUP BY provider, model;

-- [Table] Investigate quarantined records (raw payload preserved)
SELECT request_id, batch_id, quarantine_category, rejection_reasons, winner_record_hash, _raw_record
FROM quarantined_usage_events
WHERE quarantine_category = 'validation_failure'
LIMIT 50;

-- [Table] Duplicate resolution audit: each superseded record and the record that replaced it
SELECT q.request_id, q.batch_id AS superseded_batch, q.ingested_at AS superseded_ingested_at,
       s.batch_id AS winning_batch, s.ingested_at AS winning_ingested_at
FROM quarantined_usage_events q
JOIN silver_usage_events s ON s._record_hash = q.winner_record_hash AND s.request_id = q.request_id
WHERE q.quarantine_category = 'superseded_duplicate'
LIMIT 50;
