-- Model performance. SYNTHETIC data; latencies are generated, not measured.
-- Run first:  USE CATALOG <your_catalog>;  USE SCHEMA llm_usage;

-- [Table / chart] Model comparison (from gold_model_performance)
-- failure_rate = (error + timeout + rate_limited) / ALL requests for the model
-- p95 uses an exact percentile over all requests; p95_latency_success_ms covers successful requests only
-- cost_per_successful_request = ALL priced spend for the model / successful requests; NULL if any request is unpriced
SELECT provider, model, requests, success_requests, failed_requests,
       ROUND(failure_rate, 4)           AS failure_rate,
       ROUND(avg_latency_ms, 1)         AS avg_latency_ms,
       ROUND(p95_latency_ms, 1)         AS p95_latency_ms,
       ROUND(p95_latency_success_ms, 1) AS p95_latency_success_ms,
       total_estimated_cost,
       cost_per_successful_request,
       unpriced_requests
FROM gold_model_performance
ORDER BY provider, model;

-- [KPI] Overall average and p95 latency (exact percentile over all eligible requests)
SELECT AVG(latency_ms)             AS avg_latency_ms,
       percentile(latency_ms, 0.95) AS p95_latency_ms
FROM silver_priced_usage_events;

-- [Chart: bar] Error rate by model (re-aggregated from the additive daily table)
SELECT provider, model,
       SUM(failed_requests) / SUM(requests)       AS error_rate,
       SUM(rate_limited_requests) / SUM(requests) AS rate_limited_rate
FROM gold_usage_daily
GROUP BY provider, model
ORDER BY error_rate DESC;

-- [Chart: bar] Average latency by model (sum of latency / requests, so it re-aggregates exactly)
SELECT provider, model, SUM(latency_ms_sum) / SUM(requests) AS avg_latency_ms
FROM gold_usage_daily
GROUP BY provider, model
ORDER BY avg_latency_ms DESC;

-- [Table] Retry volume and the cost of requests that needed retries.
-- Token counts already cover every attempt of a request, so this is the full cost of those requests,
-- not an isolated "retry surcharge".
SELECT application,
       SUM(requests_with_retries)                             AS requests_with_retries,
       SUM(total_retries)                                     AS total_retries,
       SUM(requests_with_retries) / SUM(requests)             AS share_of_requests_with_retries,
       SUM(cost_of_requests_with_retries)                     AS cost_of_requests_with_retries
FROM gold_usage_daily
GROUP BY application
ORDER BY total_retries DESC;

-- [Table] Failure rate by application and environment
SELECT application, environment,
       SUM(requests) AS requests,
       SUM(failed_requests) / SUM(requests) AS failure_rate
FROM gold_usage_daily
GROUP BY application, environment
ORDER BY failure_rate DESC;
