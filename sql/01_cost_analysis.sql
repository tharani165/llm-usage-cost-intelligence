-- Cost analysis. SYNTHETIC data, FICTIONAL prices.
-- Run first:  USE CATALOG <your_catalog>;  USE SCHEMA llm_usage;
-- Population: gold_usage_daily = de-duplicated, validated requests (see docs/data_quality_rules.md).
-- Cost columns sum only 'priced' requests; 'unpriced' requests have NULL cost (surfaced as unpriced_requests)
-- and 'not_billed' (rate_limited) requests contribute an explicit 0.

-- [KPI] Total estimated spend, requests, tokens, failure rate
SELECT
  SUM(total_estimated_cost)                             AS total_estimated_cost,
  SUM(requests)                                         AS total_requests,
  SUM(input_tokens) + SUM(output_tokens)                AS total_tokens,
  SUM(failed_requests) / SUM(requests)                  AS failure_rate,         -- denominator: all requests
  SUM(unpriced_requests)                                AS unpriced_requests,
  SUM(total_estimated_cost) / NULLIF(SUM(success_requests), 0) AS cost_per_successful_request
FROM gold_usage_daily;

-- [Chart: line] Daily estimated cost
SELECT request_date,
       SUM(total_estimated_cost) AS daily_cost,
       SUM(requests)             AS requests
FROM gold_usage_daily
GROUP BY request_date
ORDER BY request_date;

-- [Chart: bar] Cost by provider and model
SELECT provider, model,
       SUM(total_estimated_cost) AS total_cost,
       SUM(requests)             AS requests,
       SUM(unpriced_requests)    AS unpriced_requests
FROM gold_usage_daily
GROUP BY provider, model
ORDER BY total_cost DESC NULLS LAST;

-- [Chart: bar] Cost by team and application
SELECT team, application,
       SUM(total_estimated_cost) AS total_cost,
       SUM(requests)             AS requests
FROM gold_usage_daily
GROUP BY team, application
ORDER BY total_cost DESC;

-- [Chart: stacked bar] Input versus output cost, per model
SELECT provider, model,
       SUM(input_cost)  AS input_cost,
       SUM(output_cost) AS output_cost,
       SUM(input_cost) / NULLIF(SUM(total_estimated_cost), 0) AS input_share_of_cost
FROM gold_usage_daily
GROUP BY provider, model
ORDER BY SUM(total_estimated_cost) DESC NULLS LAST;

-- [Chart: line] Token volume by day
SELECT request_date, SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens
FROM gold_usage_daily
GROUP BY request_date
ORDER BY request_date;

-- [Chart: combo] Request volume, successes and failures per day
SELECT request_date,
       SUM(requests) AS requests, SUM(success_requests) AS success_requests, SUM(failed_requests) AS failed_requests
FROM gold_usage_daily
GROUP BY request_date
ORDER BY request_date;

-- [Table] Cost trend and usage spikes: day cost vs trailing 7-day average (previous 7 days, excluding today)
WITH daily AS (
  SELECT request_date, SUM(total_estimated_cost) AS daily_cost
  FROM gold_usage_daily GROUP BY request_date
)
SELECT request_date, daily_cost,
       AVG(daily_cost) OVER (ORDER BY request_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS trailing_7d_avg,
       daily_cost / NULLIF(AVG(daily_cost) OVER (ORDER BY request_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING), 0) AS ratio_to_trailing_avg
FROM daily
ORDER BY request_date;

-- [Table] Cache-hit rate by application (denominator: all requests of the application)
SELECT application,
       SUM(cache_hit_requests) / SUM(requests) AS cache_hit_rate,
       SUM(requests)                           AS requests
FROM gold_usage_daily
GROUP BY application
ORDER BY cache_hit_rate DESC;
