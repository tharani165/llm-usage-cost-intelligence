# Dashboard specification (not deployed)

**Status:** no dashboard has been built or exported. This file is the build plan; every query exists in `sql/`
and is executed against locally-built tables by `scripts/validate_project.py` (see `docs/testing.md`);
none has been run inside Databricks SQL yet. Build it manually in Databricks:
**SQL → Dashboards → Create dashboard → Data**, add each query below as a dataset (set the dataset's default
catalog/schema to your target, e.g. `workspace.llm_usage`), then add visualizations on the **Canvas**.
Screenshots go in `evidence/` only after you build it.

| # | Visualization | Type | Source query |
|---|---|---|---|
| 1 | Total estimated spend | Counter | `01_cost_analysis.sql` · KPI (`total_estimated_cost`) |
| 2 | Total requests | Counter | KPI (`total_requests`) |
| 3 | Total tokens | Counter | KPI (`total_tokens`) |
| 4 | Failure rate | Counter (percent) | KPI (`failure_rate`) |
| 5 | p95 latency | Counter | `02_model_performance.sql` · overall p95 KPI (per-model p95 is in the model comparison table) |
| 6 | Cost per successful request | Counter | KPI (`cost_per_successful_request`) |
| 7 | Daily spend trend | Line | Daily estimated cost |
| 8 | Cost by model | Bar | Cost by provider and model |
| 9 | Cost by team / application | Bar | Cost by team and application |
| 10 | Input vs output cost | Stacked bar | Input versus output cost |
| 11 | Requests and failures per day | Combo / stacked bar | Request volume, successes and failures |
| 12 | Model performance comparison | Table | `02_model_performance.sql` · model comparison |
| 13 | Latency by model | Bar | Average latency by model |
| 14 | Budget utilization | Bar | `04_budget_monitoring.sql` |
| 15 | Quarantine / rejection reasons | Bar | `03_data_quality.sql` |
| 16 | Pricing coverage | Counter | `03_data_quality.sql` · pricing coverage |

**Filters:** date range on `request_date`, plus `application`, `team`, `provider`, `model` and `environment`
(all are columns of `gold_usage_daily`; add them as filter widgets bound to the cost/volume datasets).

**Notes for readers:** label the dashboard "Synthetic data · fictional prices". Spend excludes unpriced
requests: show the pricing-coverage counter next to the spend counter.
