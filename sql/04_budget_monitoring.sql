-- Budget monitoring against SYNTHETIC monthly budgets (config/project.example.yml).
-- Run first:  USE CATALOG <your_catalog>;  USE SCHEMA llm_usage;
-- budget_utilization = estimated_spend / monthly_budget. Status: ok < 0.8 <= warning < 1.0 <= exceeded.
-- estimated_spend excludes unpriced requests, so utilization can be UNDERSTATED where unpriced_requests > 0.

-- [Chart: bar] Utilization by application and month
SELECT application, year_month, estimated_spend, monthly_budget, budget_utilization, budget_status, unpriced_requests
FROM gold_budget_utilization
ORDER BY year_month, budget_utilization DESC;

-- [Table] Applications at or above the warning threshold
SELECT application, year_month, estimated_spend, monthly_budget, budget_utilization, budget_status
FROM gold_budget_utilization
WHERE budget_status IN ('warning', 'exceeded')
ORDER BY budget_utilization DESC;

-- [Table] Spend by team (a team can use several applications)
SELECT team, SUM(total_estimated_cost) AS total_cost, SUM(requests) AS requests
FROM gold_usage_daily
GROUP BY team
ORDER BY total_cost DESC;
