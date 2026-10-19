# Databricks validation checklist

**Status: not yet executed in a real workspace.** Use this to run the notebooks in Databricks Free Edition
(serverless) and record what actually happens. Tick items only after you have seen them pass.

## 0. Prerequisites
- Repo imported as a **Git folder** (`src/`, `config/` beside `notebooks/`). `00_setup` raises a clear error if not.
- A catalog/schema where you may create tables (defaults: `workspace` / `llm_usage`; change the widgets if needed).

## 1. Small run (`n_requests=100`)
| Step | Notebook / widget | Expected |
|---|---|---|
| 1 | `01_generate_data`, `n_requests=100` | landing: `batch_001` = 102 rows, `batch_002` = 2,124 rows (seed 42) |
| 2 | `02_bronze_ingestion`, `batch_id=batch_001` | "102 incoming, 102 inserted" |
| 3 | `03_silver_validation` | `bronze = silver + quarantine` (102 = 96 + 6) |
| 4 | `04_cost_enrichment` | row count unchanged; manual cost check prints `0.01050000` |
| 5 | `05_gold_analytics` | every reconciliation line `[PASS]` |

## 2. Incremental batch
| Step | Action | Expected |
|---|---|---|
| 6 | `02`, `batch_id=batch_002` | 2,124 inserted; Bronze total 2,226 |
| 7 | Rerun `03` → `04` → `05` | Silver + quarantine = 2,226; all checks `[PASS]`; Gold totals increase |
| 8 | Rerun `02` with `batch_002` and with `batch_001` | "0 inserted"; Bronze total unchanged |
| 9 | Rerun `03` → `05` again | identical counts and Gold totals (compare before/after) |

## 3. Full size
Set `n_requests=20000` and repeat; expected local numbers are in `evidence/local_validation_results.txt`
(22,380 Bronze → 21,770 Silver + 610 quarantine).

## 4. Delta-specific checks (cannot be verified locally)
- `DESCRIBE DETAIL <catalog>.<schema>.bronze_usage_events` shows `format = delta`.
- `DESCRIBE HISTORY bronze_usage_events` shows one `WRITE` (append) per ingested batch, none for replays.
- `SHOW TABLES IN <catalog>.<schema>` lists all 11 tables.

## 5. Dashboard
Follow `dashboards/dashboard_spec.md`, then save screenshots to `evidence/` and update the README status table.
